import superjson from 'superjson';

const transformer = {
  serialize(value) {
    if (this !== transformer) {
      throw new Error('serialize receiver was lost');
    }
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
  },
  deserialize(value) {
    if (this !== transformer) {
      throw new Error('deserialize receiver was lost');
    }
    return superjson.deserialize(value);
  },
};

export default transformer;
