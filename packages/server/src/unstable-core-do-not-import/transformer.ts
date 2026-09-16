import type { AnyRootTypes, RootConfig } from './rootConfig';
import type { AnyRouter, inferRouterError } from './router';
import type {
  TRPCResponse,
  TRPCResponseMessage,
  TRPCResultMessage,
} from './rpc';
import { isObject } from './utils';

/**
 * @public
 */
export interface DataTransformer {
  serialize(object: any): any;
  deserialize(object: any): any;
  /**
   * Optional asynchronous equivalent of `serialize`.
   *
   * The synchronous methods remain the default so existing transformers keep
   * using the synchronous fast path.
   */
  serializeAsync?: (object: any) => Promise<any>;
  /**
   * Optional asynchronous equivalent of `deserialize`.
   */
  deserializeAsync?: (object: any) => Promise<any>;
}

/**
 * A transformer that can only be used through the asynchronous pipeline.
 *
 * @public
 */
export interface AsyncDataTransformer {
  serializeAsync: (object: any) => Promise<any>;
  deserializeAsync: (object: any) => Promise<any>;
}

/** @public */
export type DataTransformerLike = DataTransformer | AsyncDataTransformer;

interface InputDataTransformer extends DataTransformer {
  /**
   * This function runs **on the client** before sending the data to the server.
   */
  serialize(object: any): any;
  /**
   * This function runs **on the server** to transform the data before it is passed to the resolver
   */
  deserialize(object: any): any;
}

interface OutputDataTransformer extends DataTransformer {
  /**
   * This function runs **on the server** before sending the data to the client.
   */
  serialize(object: any): any;
  /**
   * This function runs **only on the client** to transform the data sent from the server.
   */
  deserialize(object: any): any;
}

/**
 * @public
 */
export interface CombinedDataTransformer {
  /**
   * Specify how the data sent from the client to the server should be transformed.
   */
  input: InputDataTransformer;
  /**
   * Specify how the data sent from the server to the client should be transformed.
   */
  output: OutputDataTransformer;
}

/**
 * Input/output transformers may independently use the synchronous or
 * asynchronous contract.
 *
 * @public
 */
export interface CombinedDataTransformerOptions {
  input: DataTransformerLike;
  output: DataTransformerLike;
}

/**
 * @public
 */
export type CombinedDataTransformerClient = {
  input:
    | Pick<DataTransformer, 'serialize' | 'serializeAsync'>
    | Pick<AsyncDataTransformer, 'serializeAsync'>;
  output:
    | Pick<DataTransformer, 'deserialize' | 'deserializeAsync'>
    | Pick<AsyncDataTransformer, 'deserializeAsync'>;
};

/**
 * @public
 */
export type DataTransformerOptions =
  CombinedDataTransformerOptions | DataTransformerLike;

/**
 * @internal
 */
export function getDataTransformer(
  transformer: DataTransformerOptions,
): CombinedDataTransformer {
  if ('input' in transformer) {
    const input = normalizeDataTransformer(transformer.input);
    const output = normalizeDataTransformer(transformer.output);
    if (input === transformer.input && output === transformer.output) {
      return transformer as CombinedDataTransformer;
    }
    return { input, output };
  }
  const normalized = normalizeDataTransformer(transformer);
  return { input: normalized, output: normalized };
}

function normalizeDataTransformer(
  transformer: DataTransformerLike,
): DataTransformer {
  if ('serialize' in transformer && 'deserialize' in transformer) {
    return transformer;
  }

  return {
    ...transformer,
    serialize() {
      throw new Error(
        'This transformer only supports asynchronous serialization',
      );
    },
    deserialize() {
      throw new Error(
        'This transformer only supports asynchronous deserialization',
      );
    },
  };
}

/**
 * @internal
 */
export const defaultTransformer: CombinedDataTransformer = {
  input: { serialize: (obj) => obj, deserialize: (obj) => obj },
  output: { serialize: (obj) => obj, deserialize: (obj) => obj },
};

function transformTRPCResponseItem<
  TResponseItem extends TRPCResponse | TRPCResponseMessage,
>(config: RootConfig<AnyRootTypes>, item: TResponseItem): TResponseItem {
  if ('error' in item) {
    return {
      ...item,
      error: config.transformer.output.serialize(item.error),
    };
  }

  if ('data' in item.result) {
    return {
      ...item,
      result: {
        ...item.result,
        data: config.transformer.output.serialize(item.result.data),
      },
    };
  }

  return item;
}

/**
 * Takes a unserialized `TRPCResponse` and serializes it with the router's transformers
 **/
export function transformTRPCResponse<
  TResponse extends
    TRPCResponse | TRPCResponse[] | TRPCResponseMessage | TRPCResponseMessage[],
>(config: RootConfig<AnyRootTypes>, itemOrItems: TResponse) {
  return Array.isArray(itemOrItems)
    ? itemOrItems.map((item) => transformTRPCResponseItem(config, item))
    : transformTRPCResponseItem(config, itemOrItems);
}

async function transformTRPCResponseItemAsync<
  TResponseItem extends TRPCResponse | TRPCResponseMessage,
>(
  config: RootConfig<AnyRootTypes>,
  item: TResponseItem,
): Promise<TResponseItem> {
  if ('error' in item) {
    return {
      ...item,
      error: config.transformer.output.serializeAsync
        ? await config.transformer.output.serializeAsync(item.error)
        : config.transformer.output.serialize(item.error),
    };
  }

  if ('data' in item.result) {
    return {
      ...item,
      result: {
        ...item.result,
        data: config.transformer.output.serializeAsync
          ? await config.transformer.output.serializeAsync(item.result.data)
          : config.transformer.output.serialize(item.result.data),
      },
    };
  }

  return item;
}

/**
 * Async counterpart to {@link transformTRPCResponse}.
 *
 * This function is intentionally separate from the synchronous helper so the
 * existing path does not create promises for synchronous transformers.
 */
export async function transformTRPCResponseAsync<
  TResponse extends
    TRPCResponse | TRPCResponse[] | TRPCResponseMessage | TRPCResponseMessage[],
>(
  config: RootConfig<AnyRootTypes>,
  itemOrItems: TResponse,
): Promise<TResponse> {
  if (Array.isArray(itemOrItems)) {
    return (await Promise.all(
      itemOrItems.map((item) => transformTRPCResponseItemAsync(config, item)),
    )) as TResponse;
  }
  return (await transformTRPCResponseItemAsync(
    config,
    itemOrItems,
  )) as TResponse;
}

// FIXME:
// - the generics here are probably unnecessary
// - the RPC-spec could probably be simplified to combine HTTP + WS
/** @internal */
function transformResultInner<TRouter extends AnyRouter, TOutput>(
  response:
    | TRPCResponse<TOutput, inferRouterError<TRouter>>
    | TRPCResponseMessage<TOutput, inferRouterError<TRouter>>,
  transformer: DataTransformer,
) {
  if ('error' in response) {
    const error = transformer.deserialize(
      response.error,
    ) as inferRouterError<TRouter>;
    return {
      ok: false,
      error: {
        ...response,
        error,
      },
    } as const;
  }

  const result = {
    ...response.result,
    ...((!response.result.type || response.result.type === 'data') && {
      type: 'data',
      data: transformer.deserialize(response.result.data),
    }),
  } as TRPCResultMessage<TOutput>['result'];
  return { ok: true, result } as const;
}

class TransformResultError extends Error {
  constructor() {
    super('Unable to transform response from server');
  }
}

/**
 * Transforms and validates that the result is a valid TRPCResponse
 * @internal
 */
export function transformResult<TRouter extends AnyRouter, TOutput>(
  response:
    | TRPCResponse<TOutput, inferRouterError<TRouter>>
    | TRPCResponseMessage<TOutput, inferRouterError<TRouter>>,
  transformer: DataTransformer,
): ReturnType<typeof transformResultInner> {
  let result: ReturnType<typeof transformResultInner>;
  try {
    // Use the data transformers on the JSON-response
    result = transformResultInner(response, transformer);
  } catch {
    throw new TransformResultError();
  }

  // check that output of the transformers is a valid TRPCResponse
  if (
    !result.ok &&
    (!isObject(result.error.error) ||
      typeof result.error.error['code'] !== 'number')
  ) {
    throw new TransformResultError();
  }
  if (result.ok && !isObject(result.result)) {
    throw new TransformResultError();
  }
  return result;
}

/**
 * Async counterpart to {@link transformResult}.
 */
export async function transformResultAsync<TRouter extends AnyRouter, TOutput>(
  response:
    | TRPCResponse<TOutput, inferRouterError<TRouter>>
    | TRPCResponseMessage<TOutput, inferRouterError<TRouter>>,
  transformer: DataTransformer,
): Promise<ReturnType<typeof transformResultInner>> {
  try {
    const result =
      'error' in response
        ? {
            ok: false as const,
            error: {
              ...response,
              error: (transformer.deserializeAsync
                ? await transformer.deserializeAsync(response.error)
                : transformer.deserialize(
                    response.error,
                  )) as inferRouterError<TRouter>,
            },
          }
        : {
            ok: true as const,
            result: {
              ...response.result,
              ...((!response.result.type ||
                response.result.type === 'data') && {
                type: 'data' as const,
                data: transformer.deserializeAsync
                  ? await transformer.deserializeAsync(response.result.data)
                  : transformer.deserialize(response.result.data),
              }),
            } as TRPCResultMessage<TOutput>['result'],
          };

    if (
      !result.ok &&
      (!isObject(result.error.error) ||
        typeof result.error.error.code !== 'number')
    ) {
      throw new TransformResultError();
    }
    if (result.ok && !isObject(result.result)) {
      throw new TransformResultError();
    }
    return result;
  } catch {
    throw new TransformResultError();
  }
}
