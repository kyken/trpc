import { parentPort, workerData } from 'node:worker_threads';

async function transformResponse(response, serialize) {
  const transformItem = async (item) => {
    if ('error' in item) {
      return { ...item, error: await serialize(item.error) };
    }
    if ('data' in item.result) {
      return {
        ...item,
        result: {
          ...item.result,
          data: await serialize(item.result.data),
        },
      };
    }
    return item;
  };

  return Array.isArray(response)
    ? await Promise.all(response.map(transformItem))
    : await transformItem(response);
}

let transformer;
try {
  const module = await import(workerData.workerModule);
  transformer = module.default ?? module;
  parentPort.postMessage({ type: 'ready' });
} catch (cause) {
  parentPort.postMessage({
    type: 'fatal',
    error: cause instanceof Error ? cause.message : String(cause),
  });
}

parentPort.on('message', async ({ id, operation, value }) => {
  try {
    if (!transformer) {
      throw new Error('Worker transformer module is not ready');
    }
    let result;
    if (operation === 'serialize') {
      result = await transformer.serialize(value);
    } else if (operation === 'deserialize') {
      result = await transformer.deserialize(value);
    } else if (typeof transformer.serializeResponse === 'function') {
      result = await transformer.serializeResponse(value);
    } else {
      result = JSON.stringify(
        await transformResponse(value, transformer.serialize),
      );
    }
    parentPort.postMessage({ id, result });
  } catch (cause) {
    parentPort.postMessage({
      id,
      error: cause instanceof Error ? cause.message : String(cause),
    });
  }
});
