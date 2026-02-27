import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

export async function initDb(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      ingested_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_events_started_at ON events((data->>'startedAt'));

    CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
      id INT PRIMARY KEY DEFAULT 1,
      cursor TEXT,
      events_fetched INT DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    INSERT INTO ingestion_checkpoint (id, cursor, events_fetched)
    VALUES (1, NULL, 0)
    ON CONFLICT (id) DO NOTHING;
  `);
}

export async function getCheckpoint(): Promise<{ cursor: string | null; eventsFetched: number }> {
  const result = await pool.query(
    'SELECT cursor, events_fetched FROM ingestion_checkpoint WHERE id = 1'
  );
  return {
    cursor: result.rows[0]?.cursor ?? null,
    eventsFetched: result.rows[0]?.events_fetched ?? 0,
  };
}

export async function saveCheckpoint(cursor: string | null, eventsFetched: number): Promise<void> {
  await pool.query(
    `UPDATE ingestion_checkpoint
     SET cursor = $1, events_fetched = $2, updated_at = NOW()
     WHERE id = 1`,
    [cursor, eventsFetched]
  );
}

export async function batchInsertEvents(events: any[]): Promise<void> {
  if (events.length === 0) return;

  const chunkSize = 500;
  for (let i = 0; i < events.length; i += chunkSize) {
    const chunk = events.slice(i, i + chunkSize);
    const values: any[] = [];
    const placeholders = chunk.map((event, idx) => {
      const base = idx * 2;
      values.push(event.id, JSON.stringify(event));
      return `($${base + 1}, $${base + 2})`;
    });

    await pool.query(
      `INSERT INTO events (id, data)
       VALUES ${placeholders.join(', ')}
       ON CONFLICT (id) DO NOTHING`,
      values
    );
  }
}

export async function closeDb(): Promise<void> {
  await pool.end();
}
