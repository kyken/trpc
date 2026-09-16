import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { testServerAndClientResource } from '@trpc/client/__tests__/testClientResource';
import { waitError } from '@trpc/server/__tests__/waitError';
import { TRPCClientError } from '@trpc/client';
import {
  initTRPC,
  transformTRPCResponse,
  transformTRPCResponseAsync,
  TRPCError,
} from '@trpc/server';
import { createNodeWorkerTransformer } from '@trpc/server/adapters/node-worker';
import type {
  CombinedDataTransformer,
  DataTransformer,
} from '@trpc/server/unstable-core-do-not-import';
import { uneval } from 'devalue';
import superjson from 'superjson';
import { createTson, tsonDate } from 'tupleson';
import { z } from 'zod';

function createAsyncTransformer(base: DataTransformer = superjson) {
  const calls = {
    syncSerialize: 0,
    syncDeserialize: 0,
    serialize: 0,
    deserialize: 0,
  };

  const transformer: DataTransformer = {
    serialize: (value) => {
      calls.syncSerialize++;
      return base.serialize(value);
    },
    deserialize: (value) => {
      calls.syncDeserialize++;
      return base.deserialize(value);
    },
    serializeAsync: async (value) => {
      calls.serialize++;
      await Promise.resolve();
      return base.serialize(value);
    },
    deserializeAsync: async (value) => {
      calls.deserialize++;
      await Promise.resolve();
      return base.deserialize(value);
    },
  };

  return { calls, transformer };
}

test('async transformer is awaited for HTTP single responses and input', async () => {
  const { calls, transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    echo: t.procedure.input(z.date()).query(({ input }) => input),
  });
  const input = new Date('2025-01-01T00:00:00.000Z');

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpLink',
  });

  await expect(ctx.client.echo.query(input)).resolves.toEqual(input);
  expect(calls.serialize).toBeGreaterThan(0);
  expect(calls.deserialize).toBeGreaterThan(0);
  expect(calls.syncSerialize).toBe(0);
  expect(calls.syncDeserialize).toBe(0);
});

test('Node worker transformer encodes the complete HTTP response body', async () => {
  const transformer = createNodeWorkerTransformer({
    workerModule: pathToFileURL(
      resolve(
        process.cwd(),
        'packages/tests/server/fixtures/async-transformer-worker.mjs',
      ),
    ),
    workerEntry: pathToFileURL(
      resolve(
        process.cwd(),
        'packages/server/src/adapters/node-worker/worker.mjs',
      ),
    ),
    transformer: superjson,
    maxWorkers: 2,
  });
  const t = initTRPC.create({ transformer });
  const router = t.router({
    values: t.procedure.input(z.date()).query(({ input }) => ({
      date: input,
      map: new Map([['key', 'value']]),
      set: new Set(['value']),
      bigint: 42n,
    })),
    fail: t.procedure.query(() => {
      throw new TRPCError({ code: 'BAD_REQUEST', message: 'worker failure' });
    }),
  });

  try {
    await using ctx = testServerAndClientResource(router, {
      clientLink: 'httpLink',
      server: {
        responseBodyEncoder: transformer.responseBodyEncoder,
      },
    });

    const result = await ctx.client.values.query(
      new Date('2025-01-01T00:00:00.000Z'),
    );
    expect(result.date).toBeInstanceOf(Date);
    expect(result.map).toEqual(new Map([['key', 'value']]));
    expect(result.set).toEqual(new Set(['value']));
    expect(result.bigint).toBe(42n);

    const error = await waitError(
      ctx.client.fail.query(),
      TRPCClientError<typeof router>,
    );
    expect(error.data?.code).toBe('BAD_REQUEST');
    expect(error.data?.httpStatus).toBe(400);
  } finally {
    await transformer.close();
  }
});

test('Node worker response encoder preserves HTTP batch envelopes', async () => {
  const transformer = createNodeWorkerTransformer({
    workerModule: pathToFileURL(
      resolve(
        process.cwd(),
        'packages/tests/server/fixtures/async-transformer-worker.mjs',
      ),
    ),
    workerEntry: pathToFileURL(
      resolve(
        process.cwd(),
        'packages/server/src/adapters/node-worker/worker.mjs',
      ),
    ),
    transformer: superjson,
    maxWorkers: 2,
  });
  const t = initTRPC.create({ transformer });
  const router = t.router({
    echo: t.procedure.input(z.number()).query(({ input }) => input),
  });

  try {
    await using ctx = testServerAndClientResource(router, {
      clientLink: 'httpBatchLink',
      server: {
        responseBodyEncoder: transformer.responseBodyEncoder,
      },
    });

    await expect(
      Promise.all([ctx.client.echo.query(1), ctx.client.echo.query(2)]),
    ).resolves.toEqual([1, 2]);
  } finally {
    await transformer.close();
  }
});

test('empty superjson up and down', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    emptyUp: t.procedure.query(() => 'hello world'),
    emptyDown: t.procedure.input(z.string()).query(() => 'hello world'),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
  });

  await expect(ctx.client.emptyUp.query()).resolves.toBe('hello world');
  await expect(ctx.client.emptyDown.query('')).resolves.toBe('hello world');
});

test('devalue up and down', async () => {
  const base: DataTransformer = {
    serialize: (value) => uneval(value),
    deserialize: (value) => eval(`(${value})`),
  };
  const { transformer } = createAsyncTransformer(base);
  const date = new Date();
  const seen = vi.fn();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    hello: t.procedure.input(z.date()).query(({ input }) => {
      seen(input);
      return input;
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
  });

  const result = await ctx.client.hello.query(date);
  expect(result.getTime()).toBe(date.getTime());
  expect((seen.mock.calls[0]![0]! as Date).getTime()).toBe(date.getTime());
});

test('not batching: async superjson up and devalue down', async () => {
  const outputBase: DataTransformer = {
    serialize: (value) => uneval(value),
    deserialize: (value) => eval(`(${value})`),
  };
  const { transformer: input } = createAsyncTransformer(superjson);
  const { transformer: output } = createAsyncTransformer(outputBase);
  const transformer: CombinedDataTransformer = { input, output };
  const date = new Date();
  const seen = vi.fn();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    hello: t.procedure.input(z.date()).query(({ input }) => {
      seen(input);
      return input;
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpLink',
  });

  const result = await ctx.client.hello.query(date);
  expect(result.getTime()).toBe(date.getTime());
  expect((seen.mock.calls[0]![0]! as Date).getTime()).toBe(date.getTime());
});

describe('batching: async superjson up and devalue down', () => {
  const outputBase: DataTransformer = {
    serialize: (value) => uneval(value),
    deserialize: (value) => eval(`(${value})`),
  };

  test.each(['httpBatchLink', 'httpBatchStreamLink'] as const)(
    '$0',
    async (clientLink) => {
      const { calls: inputCalls, transformer: input } =
        createAsyncTransformer(superjson);
      const { transformer: output } = createAsyncTransformer(outputBase);
      const transformer: CombinedDataTransformer = { input, output };
      const date = new Date();
      const seen = vi.fn();
      const t = initTRPC.create({ transformer });
      const router = t.router({
        hello: t.procedure.input(z.date()).query(({ input }) => {
          seen(input);
          return input;
        }),
      });

      await using ctx = testServerAndClientResource(router, { clientLink });

      const result = await ctx.client.hello.query(date);
      expect(result.getTime()).toBe(date.getTime());
      expect((seen.mock.calls[0]![0]! as Date).getTime()).toBe(date.getTime());
      expect(inputCalls.serialize).toBeGreaterThan(0);
    },
  );
});

test('all async transformers run in the correct order', async () => {
  const value = 'foo';
  const seen = vi.fn();
  const transformer: CombinedDataTransformer = {
    input: {
      serialize: (object) => object,
      deserialize: (object) => object,
      serializeAsync: async (object) => {
        seen('client:serialized');
        await Promise.resolve();
        return object;
      },
      deserializeAsync: async (object) => {
        seen('server:deserialized');
        await Promise.resolve();
        return object;
      },
    },
    output: {
      serialize: (object) => object,
      deserialize: (object) => object,
      serializeAsync: async (object) => {
        seen('server:serialized');
        await Promise.resolve();
        return object;
      },
      deserializeAsync: async (object) => {
        seen('client:deserialized');
        await Promise.resolve();
        return object;
      },
    },
  };
  const t = initTRPC.create({ transformer });
  const router = t.router({
    hello: t.procedure.input(z.string()).query(({ input }) => {
      seen(input);
      return input;
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
  });

  await expect(ctx.client.hello.query(value)).resolves.toBe(value);
  expect(seen.mock.calls.map(([entry]) => entry)).toEqual([
    'client:serialized',
    'server:deserialized',
    value,
    'server:serialized',
    'client:deserialized',
  ]);
});

test('async response helper preserves the synchronous wire representation', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const response = {
    result: {
      data: {
        date: new Date('2025-01-01T00:00:00.000Z'),
      },
    },
  } as const;

  const syncResult = transformTRPCResponse(t._config, response);
  expect(syncResult).not.toBeInstanceOf(Promise);
  await expect(
    transformTRPCResponseAsync(t._config, response),
  ).resolves.toEqual(syncResult);
});

test('async transformer is awaited for HTTP batch responses', async () => {
  const { calls, transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    echo: t.procedure
      .input(z.object({ value: z.string() }))
      .query(({ input }) => input),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
  });

  await expect(
    Promise.all([
      ctx.client.echo.query({ value: 'first' }),
      ctx.client.echo.query({ value: 'second' }),
    ]),
  ).resolves.toEqual([{ value: 'first' }, { value: 'second' }]);
  expect(calls.serialize).toBeGreaterThanOrEqual(2);
  expect(calls.deserialize).toBeGreaterThanOrEqual(2);
});

describe('async transformer serializes HTTP mutation bodies', () => {
  test.each(['httpLink', 'httpBatchLink'] as const)(
    '$0',
    async (clientLink) => {
      const { calls, transformer } = createAsyncTransformer();
      const t = initTRPC.create({ transformer });
      const router = t.router({
        update: t.procedure.input(z.date()).mutation(({ input }) => input),
      });

      await using ctx = testServerAndClientResource(router, { clientLink });

      const date = new Date('2025-01-01T00:00:00.000Z');
      const result = await ctx.client.update.mutate(date);
      expect(result).toEqual(date);
      expect(calls.serialize).toBeGreaterThan(0);
      expect(calls.deserialize).toBeGreaterThan(0);
    },
  );
});

test('async transformer preserves SuperJSON values', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    values: t.procedure.query(() => ({
      date: new Date('2025-01-01T00:00:00.000Z'),
      map: new Map([['key', 'value']]),
      set: new Set(['value']),
      bigint: 42n,
      undefined,
    })),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpLink',
  });

  const result = await ctx.client.values.query();
  expect(result.date).toBeInstanceOf(Date);
  expect(result.map).toEqual(new Map([['key', 'value']]));
  expect(result.set).toEqual(new Set(['value']));
  expect(result.bigint).toBe(42n);
  expect(result.undefined).toBeUndefined();
});

test('async transformer preserves response metadata', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    value: t.procedure.query(() => 'value'),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
    server: {
      responseMeta: () => ({
        status: 201,
        headers: {
          'x-async-transformer': 'true',
        },
      }),
    },
  });

  await expect(ctx.client.value.query()).resolves.toBe('value');
  const result = ctx.linkSpy.next.mock.lastCall?.[0] as any;
  expect(result.context.response.status).toBe(201);
  expect(result.context.response.headers.get('x-async-transformer')).toBe(
    'true',
  );
});

test('async transformer preserves mixed batch success and error items', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    ok: t.procedure.query(() => 'ok'),
    fail: t.procedure.query(() => {
      throw new TRPCError({
        code: 'UNAUTHORIZED',
        message: 'expected batch failure',
      });
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
  });

  const [success, failure] = await Promise.allSettled([
    ctx.client.ok.query(),
    ctx.client.fail.query(),
  ]);
  expect(success).toEqual({ status: 'fulfilled', value: 'ok' });
  expect(failure.status).toBe('rejected');
  if (failure.status === 'rejected') {
    expect(failure.reason.data?.code).toBe('UNAUTHORIZED');
    expect(failure.reason.data?.httpStatus).toBe(401);
  }
});

test('async output rejection is converted to the existing error response', async () => {
  const { transformer } = createAsyncTransformer();
  transformer.serializeAsync = async (value) => {
    if (value === 'reject') {
      throw new Error('output transform failed');
    }
    return superjson.serialize(value);
  };
  const t = initTRPC.create({ transformer });
  const router = t.router({
    value: t.procedure.query(() => 'reject'),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpLink',
  });

  const error = await waitError(
    ctx.client.value.query(),
    TRPCClientError<typeof router>,
  );
  expect(error.data?.code).toBe('INTERNAL_SERVER_ERROR');
  expect(error.data?.httpStatus).toBe(500);
});

test('async transformer preserves error response shape and status', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    fail: t.procedure.query(() => {
      throw new TRPCError({
        code: 'BAD_REQUEST',
        message: 'expected failure',
      });
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpLink',
  });

  const error = await waitError(
    ctx.client.fail.query(),
    TRPCClientError<typeof router>,
  );
  expect(error.data?.code).toBe('BAD_REQUEST');
  expect(error.data?.httpStatus).toBe(400);
  expect(error.message).toBe('expected failure');
});

test('async input rejection is converted to the existing error response', async () => {
  const input = createAsyncTransformer();
  const output = createAsyncTransformer();
  input.transformer.deserializeAsync = async () => {
    throw new Error('input transform failed');
  };
  const transformer: CombinedDataTransformer = {
    input: input.transformer,
    output: output.transformer,
  };
  const t = initTRPC.create({ transformer });
  const router = t.router({
    echo: t.procedure.input(z.string()).query(({ input }) => input),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpLink',
  });

  const error = await waitError(
    ctx.client.echo.query('input'),
    TRPCClientError<typeof router>,
  );
  expect(error.data?.code).toBe('BAD_REQUEST');
  expect(error.data?.httpStatus).toBe(400);
});

test('async transformer is used by the Node HTTP exception fallback', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    value: t.procedure.query(() => 'value'),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpLink',
    server: {
      middleware: (_req, _res, next) => next(new Error('middleware failed')),
    },
  });

  const error = await waitError(
    ctx.client.value.query(),
    TRPCClientError<typeof router>,
  );
  expect(error.data?.code).toBe('INTERNAL_SERVER_ERROR');
  expect(error.data?.httpStatus).toBe(500);
});

test('async transformer preserves JSONL streaming order', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    echo: t.procedure.input(z.number()).query(({ input }) => input),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchStreamLink',
  });

  await expect(
    Promise.all([
      ctx.client.echo.query(1),
      ctx.client.echo.query(2),
      ctx.client.echo.query(3),
    ]),
  ).resolves.toEqual([1, 2, 3]);
});

test('async transformer preserves SSE streaming order', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const values = [
    new Date('2025-01-01T00:00:00.000Z'),
    new Date('2025-01-02T00:00:00.000Z'),
  ];
  const router = t.router({
    events: t.procedure.subscription(async function* () {
      yield* values;
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpSubscriptionLink',
  });

  const received: Date[] = [];
  const subscription = ctx.client.events.subscribe(undefined, {
    onData(data) {
      received.push(data);
    },
  });

  await vi.waitFor(() => {
    expect(received).toEqual(values);
  });
  subscription.unsubscribe();
});

test('async transformer preserves WebSocket response ordering', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    echo: t.procedure.input(z.number()).query(({ input }) => input),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'wsLink',
  });

  await expect(
    Promise.all([
      ctx.client.echo.query(1),
      ctx.client.echo.query(2),
      ctx.client.echo.query(3),
    ]),
  ).resolves.toEqual([1, 2, 3]);
});

test('async transformer preserves WebSocket subscription ordering', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const values = [1, 2, 3];
  const router = t.router({
    events: t.procedure.subscription(async function* () {
      yield* values;
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'wsLink',
  });

  const received: number[] = [];
  const subscription = ctx.client.events.subscribe(undefined, {
    onData(value) {
      received.push(value);
    },
  });

  await vi.waitFor(() => {
    expect(received).toEqual(values);
  });
  subscription.unsubscribe();
});

test('tupleson', async () => {
  const base = createTson({
    types: [tsonDate],
    nonce: () => Math.random() + '',
  });
  const { transformer } = createAsyncTransformer(base);
  const date = new Date();
  const seen = vi.fn();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    hello: t.procedure.input(z.date()).query(({ input }) => {
      seen(input);
      return input;
    }),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
  });

  const result = await ctx.client.hello.query(date);
  expect(result.getTime()).toBe(date.getTime());
  expect((seen.mock.calls[0]![0]! as Date).getTime()).toBe(date.getTime());
});

test('superjson - no input', async () => {
  const { transformer } = createAsyncTransformer();
  const t = initTRPC.create({ transformer });
  const router = t.router({
    hello: t.procedure.query(() => 'world'),
  });

  await using ctx = testServerAndClientResource(router, {
    clientLink: 'httpBatchLink',
  });

  const json = await (await fetch(`${ctx.httpUrl}/hello`)).json();
  expect(json).not.toHaveProperty('error');
  expect(json.result.data).toEqual({ json: 'world' });
});

describe('superjson - custom instance', () => {
  class MyCustomThing {
    constructor(public readonly value: number) {}
  }

  const base = new superjson();
  base.registerCustom(
    {
      isApplicable: (value): value is MyCustomThing =>
        value instanceof MyCustomThing,
      serialize: (value) => ({ value: value.value }),
      deserialize: (value) => new MyCustomThing(value.value),
    },
    'MyCustomThing',
  );
  const { transformer } = createAsyncTransformer(base);
  const t = initTRPC.create({ transformer });
  const router = t.router({
    custom: t.procedure.query(() => new MyCustomThing(42)),
  });

  test.each(['httpLink', 'httpBatchLink', 'httpBatchStreamLink'] as const)(
    '$0',
    async (clientLink) => {
      await using ctx = testServerAndClientResource(router, { clientLink });

      const result = await ctx.client.custom.query();
      expect(result).toBeInstanceOf(MyCustomThing);
      expect(result.value).toBe(42);
    },
  );
});
