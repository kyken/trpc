import { Worker } from 'node:worker_threads';
import type { AnyRouter, TRPCDataTransformer } from '../../@trpc/server';
import type {
  ResponseBody,
  ResponseBodyEncoder,
} from '../../@trpc/server/http';

export type WorkerOperation = 'serialize' | 'deserialize' | 'serializeResponse';

interface WorkerMessage {
  id: number;
  result?: unknown;
  error?: string;
  type?: 'ready' | 'fatal';
}

interface WorkerTask {
  id: number;
  operation: WorkerOperation;
  value: unknown;
  resolve: (value: unknown) => void;
  reject: (cause: unknown) => void;
  slot?: WorkerSlot;
  timer?: ReturnType<typeof setTimeout>;
  done: boolean;
}

interface WorkerSlot {
  index: number;
  worker: Worker;
  ready: boolean;
  dead: boolean;
  task?: WorkerTask;
}

function abortError(message = 'The operation was aborted') {
  const error = new Error(message);
  error.name = 'AbortError';
  return error;
}

export interface NodeWorkerPoolOptions {
  /** Override the built-in worker entrypoint for bundlers and tests. */
  workerEntry?: URL | string;
  /** Module URL that exports serialize/deserialize and optionally serializeResponse. */
  workerModule: URL | string;
  /** Maximum number of concurrently running worker jobs. @default 1 */
  maxWorkers?: number;
  /** Maximum number of queued jobs. @default Infinity */
  maxQueueSize?: number;
}

export interface NodeWorkerRunOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

/**
 * A reusable, bounded worker pool for Node.js transformer operations.
 * This class is intentionally exported from a Node-only adapter entrypoint.
 */
export class NodeWorkerPool {
  private readonly options: Required<
    Pick<NodeWorkerPoolOptions, 'maxWorkers' | 'maxQueueSize'>
  > & { workerModule: string; workerEntry: URL | string };
  private readonly slots: WorkerSlot[] = [];
  private readonly queue: WorkerTask[] = [];
  private nextId = 0;
  private closing = false;

  constructor(options: NodeWorkerPoolOptions) {
    const maxWorkers = options.maxWorkers ?? 1;
    const maxQueueSize = options.maxQueueSize ?? Infinity;
    if (!Number.isInteger(maxWorkers) || maxWorkers < 1) {
      throw new Error('maxWorkers must be a positive integer');
    }
    if (
      maxQueueSize !== Infinity &&
      (!Number.isInteger(maxQueueSize) || maxQueueSize < 0)
    ) {
      throw new Error(
        'maxQueueSize must be a non-negative integer or Infinity',
      );
    }
    this.options = {
      workerEntry:
        options.workerEntry ?? new URL('./worker.mjs', import.meta.url),
      workerModule:
        options.workerModule instanceof URL
          ? options.workerModule.href
          : options.workerModule,
      maxWorkers,
      maxQueueSize,
    };
    for (let index = 0; index < maxWorkers; index++) {
      this.spawn(index);
    }
  }

  run(
    operation: WorkerOperation,
    value: unknown,
    options: NodeWorkerRunOptions = {},
  ): Promise<unknown> {
    if (this.closing) {
      return Promise.reject(new Error('Worker pool is closed'));
    }
    if (options.signal?.aborted) {
      return Promise.reject(abortError());
    }
    if (this.queue.length >= this.options.maxQueueSize) {
      return Promise.reject(new Error('Worker pool queue is full'));
    }

    let resolvePromise!: (value: unknown) => void;
    let rejectPromise!: (cause: unknown) => void;
    const taskRef: { current?: WorkerTask } = {};
    const onAbort = () => {
      if (taskRef.current) {
        this.cancel(taskRef.current, abortError());
      }
    };
    const finish = () => {
      const task = taskRef.current;
      if (task?.timer) {
        clearTimeout(task.timer);
        task.timer = undefined;
      }
      options.signal?.removeEventListener('abort', onAbort);
    };
    const promise = new Promise<unknown>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });
    const task: WorkerTask = {
      id: this.nextId++,
      operation,
      value,
      resolve: (result) => {
        finish();
        task.done = true;
        resolvePromise(result);
      },
      reject: (cause) => {
        finish();
        task.done = true;
        rejectPromise(cause);
      },
      done: false,
    };
    taskRef.current = task;
    options.signal?.addEventListener('abort', onAbort, { once: true });

    if (options.timeoutMs !== undefined) {
      if (options.timeoutMs < 0) {
        task.reject(new Error('timeoutMs must be non-negative'));
        return promise;
      }
      task.timer = setTimeout(() => {
        this.cancel(task, abortError('Worker operation timed out'));
      }, options.timeoutMs);
    }

    this.queue.push(task);
    this.pump();
    return promise;
  }

  async close() {
    this.closing = true;
    const error = new Error('Worker pool is closed');
    for (const task of this.queue.splice(0)) {
      task.reject(error);
    }
    await Promise.all(
      this.slots.map(async (slot) => {
        if (slot.task) {
          slot.task.reject(error);
          slot.task = undefined;
        }
        slot.dead = true;
        await slot.worker.terminate();
      }),
    );
  }

  private spawn(index: number) {
    const slot: WorkerSlot = {
      index,
      worker: new Worker(this.options.workerEntry, {
        execArgv: process.execArgv.filter(
          (argument) =>
            argument !== '--expose-gc' && !argument.startsWith('--input-type'),
        ),
        workerData: { workerModule: this.options.workerModule },
      }),
      ready: false,
      dead: false,
    };
    slot.worker.on('message', (message: WorkerMessage) => {
      if (message.type === 'ready') {
        slot.ready = true;
        this.pump();
        return;
      }
      if (message.type === 'fatal') {
        this.fail(
          slot,
          new Error(message.error ?? 'Worker failed to load'),
          false,
        );
        return;
      }
      const task = slot.task;
      if (task?.id !== message.id) return;
      slot.task = undefined;
      if (message.error) {
        task.reject(new Error(message.error));
      } else {
        task.resolve(message.result);
      }
      this.pump();
    });
    slot.worker.on('error', (cause) => this.fail(slot, cause));
    slot.worker.on('exit', (code) => {
      if (!this.closing && !slot.dead) {
        this.fail(slot, new Error(`Worker exited with code ${code}`));
      }
    });
    this.slots[index] = slot;
  }

  private fail(slot: WorkerSlot, cause: unknown, replace = true) {
    if (slot.dead) return;
    slot.dead = true;
    slot.task?.reject(cause);
    slot.task = undefined;
    if (!this.closing && replace) {
      void slot.worker.terminate().finally(() => {
        if (!this.closing) this.spawn(slot.index);
      });
    } else {
      for (const task of this.queue.splice(0)) {
        task.reject(cause);
      }
      void slot.worker.terminate();
    }
  }

  private cancel(task: WorkerTask, cause: unknown) {
    if (task.done) return;
    const queuedIndex = this.queue.indexOf(task);
    if (queuedIndex !== -1) {
      this.queue.splice(queuedIndex, 1);
      task.reject(cause);
      return;
    }
    const slot = task.slot;
    if (!slot) {
      task.reject(cause);
      return;
    }
    slot.dead = true;
    slot.task = undefined;
    if (task.timer) clearTimeout(task.timer);
    task.reject(cause);
    void slot.worker.terminate().finally(() => {
      if (!this.closing) this.spawn(slot.index);
    });
    this.pump();
  }

  private pump() {
    if (this.closing) return;
    for (const slot of this.slots) {
      if (slot.dead || !slot.ready || slot.task) continue;
      const task = this.queue.shift();
      if (!task) return;
      task.slot = slot;
      slot.task = task;
      slot.worker.postMessage({
        id: task.id,
        operation: task.operation,
        value: task.value,
      });
    }
  }
}

export type NodeWorkerTransformerOptions = NodeWorkerPoolOptions & {
  transformer: Pick<TRPCDataTransformer, 'serialize' | 'deserialize'>;
};

export function createNodeWorkerTransformer<TRouter extends AnyRouter>(
  options: NodeWorkerTransformerOptions,
): TRPCDataTransformer & {
  responseBodyEncoder: ResponseBodyEncoder<TRouter>;
  close: () => Promise<void>;
} {
  const pool = new NodeWorkerPool(options);
  return {
    serialize: (value) => options.transformer.serialize(value),
    deserialize: (value) => options.transformer.deserialize(value),
    serializeAsync: (value) => pool.run('serialize', value),
    deserializeAsync: (value) => pool.run('deserialize', value),
    responseBodyEncoder: (response) =>
      pool.run('serializeResponse', response) as Promise<ResponseBody>,
    close: () => pool.close(),
  };
}
