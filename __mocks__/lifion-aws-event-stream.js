import { Transform } from 'node:stream';

function Parser() {
  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      this.push(chunk);
      callback();
    }
  });
}

export { Parser };
export default { Parser };
