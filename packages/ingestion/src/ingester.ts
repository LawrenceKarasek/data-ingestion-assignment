import { getCheckpoint, saveCheckpoint, batchInsertEvents } from './db';

const DASHBOARD_URL = process.env.DASHBOARD_URL!;
const API_KEY = process.env.TARGET_API_KEY!;
const FEED_PATH = process.env.FEED_PATH || '/api/v1/events/d4ta/x7k9/feed';
const LIMIT = 5000;
const TOKEN_REFRESH_BUFFER_MS = 30_000;
const FETCH_DELAY_MS = 500;

interface StreamAccess {
  endpoint: string;
  token: string;
  expiresIn: number;
  tokenHeader: string;
}

let cachedAccess: StreamAccess | null = null;
let tokenExpiresAt = 0;

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getStreamToken(): Promise<StreamAccess> {
  if (cachedAccess && Date.now() < tokenExpiresAt) {
    return cachedAccess;
  }

  console.log('[token] Fetching fresh stream token...');
  const res = await fetch(`${DASHBOARD_URL}/internal/dashboard/stream-access`, {
    method: 'POST',
    headers: {
      'Accept': '*/*',
      'Accept-Encoding': 'gzip, deflate',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cache-Control': 'no-cache',
      'Content-Length': '0',
      'Content-Type': 'application/json',
      'Cookie': `dashboard_api_key=${API_KEY}`,
      'Host': 'datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com',
      'Origin': DASHBOARD_URL,
      'Pragma': 'no-cache',
      'Referer': `${DASHBOARD_URL}/`,
      'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
      'X-API-Key': API_KEY,
    },
    body: '',
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Stream token fetch failed [${res.status}]: ${body}`);
  }

  const json = await res.json() as any;
  cachedAccess = json.streamAccess as StreamAccess;
  tokenExpiresAt = Date.now() + (cachedAccess.expiresIn * 1000) - TOKEN_REFRESH_BUFFER_MS;

  console.log(`[token] Token obtained. Header: ${cachedAccess.tokenHeader}, Expires in: ${cachedAccess.expiresIn}s`);
  return cachedAccess;
}

async function fetchPage(cursor: string | null): Promise<any> {
  const access = await getStreamToken();
  const url = new URL(`${DASHBOARD_URL}${access.endpoint}`);
  url.searchParams.set('limit', String(LIMIT));
  if (cursor) url.searchParams.set('cursor', cursor);

  const res = await fetch(url.toString(), {
    headers: {
      'X-API-Key': API_KEY,
      [access.tokenHeader]: access.token,
    },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Feed fetch failed [${res.status}]: ${body}`);
  }

  return res.json();
}

export async function runIngestion(): Promise<void> {
  const { cursor: savedCursor, eventsFetched: savedCount } = await getCheckpoint();

  let cursor: string | null = savedCursor;
  let totalFetched = savedCount;
  let hasMore = true;
  let consecutiveErrors = 0;
  const startTime = Date.now();

  console.log(`[ingestion] Starting. Resume cursor: ${cursor ? 'yes' : 'none'}, Already fetched: ${totalFetched}`);

  while (hasMore) {
    try {
      const json = await fetchPage(cursor);
      const events: any[] = json.data ?? [];
      const pagination = json.pagination;

      if (events.length > 0) {
        await batchInsertEvents(events);
        totalFetched += events.length;
        await saveCheckpoint(pagination?.nextCursor ?? null, totalFetched);

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const total = json.meta?.total ?? 3_000_000;
        const pct = ((totalFetched / total) * 100).toFixed(2);
        const rate = (totalFetched / ((Date.now() - startTime) / 1000)).toFixed(0);
        console.log(`[ingestion] ${totalFetched.toLocaleString()} / ${total.toLocaleString()} (${pct}%) | ${rate} events/s | ${elapsed}s elapsed`);
      }

      hasMore = pagination?.hasMore ?? false;
      cursor = pagination?.nextCursor ?? null;
      consecutiveErrors = 0;

      if (pagination?.cursorExpiresIn && pagination.cursorExpiresIn < 20) {
        console.warn(`[warn] Cursor expires in ${pagination.cursorExpiresIn}s!`);
      }

      await sleep(FETCH_DELAY_MS);

    } catch (err: any) {
      const is429 = err.message?.includes('429');

      if (is429) {
        console.warn('[rate-limit] 429 received, waiting 60s before retry...');
        cachedAccess = null;
        await sleep(60_000);
        consecutiveErrors = 0;
        continue;
      }

      consecutiveErrors++;
      const waitMs = 2000 * consecutiveErrors;
      console.error(`[error] Attempt ${consecutiveErrors}/5 (waiting ${waitMs}ms):`, err.message);
      if (consecutiveErrors >= 5) throw new Error('5 consecutive errors — aborting.');
      cachedAccess = null;
      await sleep(waitMs);
    }
  }

  const totalSeconds = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`ingestion complete - total events: ${totalFetched} in ${totalSeconds}s`);
}
