import { initDb, closeDb } from './db';
import { runIngestion } from './ingester';

async function main(): Promise<void> {
  console.log('[main] Initializing database...');
  await initDb();

  console.log('[main] Starting ingestion...');
  await runIngestion();

  await closeDb();
  process.exit(0);
}

main().catch(err => {
  console.error('[main] Fatal error:', err);
  process.exit(1);
});
