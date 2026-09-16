import superjson from 'superjson';

export function serialize(value) {
  if (value === 'reject') {
    throw new Error('worker serialization failed');
  }
  if (value === 'exit') {
    process.exit(1);
  }
  if (value === 'exit0') {
    process.exit(0);
  }
  if (value === 'slow') {
    return new Promise((resolve) => {
      setTimeout(() => resolve(superjson.serialize(value)), 100);
    });
  }
  return superjson.serialize(value);
}

export function deserialize(value) {
  return superjson.deserialize(value);
}
