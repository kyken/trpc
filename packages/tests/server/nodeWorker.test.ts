import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { NodeWorkerPool } from '@trpc/server/adapters/node-worker';

const workerModule = pathToFileURL(
  resolve(
    process.cwd(),
    'packages/tests/server/fixtures/async-transformer-worker.mjs',
  ),
);
const workerEntry = pathToFileURL(
  resolve(process.cwd(), 'packages/server/src/adapters/node-worker/worker.mjs'),
);

test('worker pool rejects worker errors and keeps the pool usable', async () => {
  const pool = new NodeWorkerPool({ workerModule, workerEntry, maxWorkers: 1 });
  try {
    await expect(pool.run('serialize', 'reject')).rejects.toThrow(
      'worker serialization failed',
    );
    await expect(pool.run('serialize', 'ok')).resolves.toEqual({ json: 'ok' });
  } finally {
    await pool.close();
  }
});

test('worker pool handles worker exit, timeout, and queued abort', async () => {
  const pool = new NodeWorkerPool({
    workerModule,
    workerEntry,
    maxWorkers: 1,
    maxQueueSize: 2,
  });
  try {
    await expect(pool.run('serialize', 'exit')).rejects.toThrow(
      'Worker exited with code 1',
    );
    await expect(pool.run('serialize', 'ok')).resolves.toEqual({ json: 'ok' });
    await expect(pool.run('serialize', 'exit0')).rejects.toThrow(
      'Worker exited with code 0',
    );
    await expect(pool.run('serialize', 'ok')).resolves.toEqual({ json: 'ok' });

    await expect(
      pool.run('serialize', 'slow', { timeoutMs: 1 }),
    ).rejects.toMatchObject({ name: 'AbortError' });
    await expect(pool.run('serialize', 'ok')).resolves.toEqual({ json: 'ok' });

    const controller = new AbortController();
    const running = pool.run('serialize', 'slow');
    const queued = pool.run('serialize', 'queued', {
      signal: controller.signal,
    });
    controller.abort();
    await expect(queued).rejects.toMatchObject({ name: 'AbortError' });
    await expect(running).resolves.toEqual({ json: 'slow' });
  } finally {
    await pool.close();
  }
});
