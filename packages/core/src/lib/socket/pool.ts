import { Pool } from 'effect';
import * as socket from './socket';

export interface Options {
  host: string;
  port: number;
}

export const make = (options: Options) =>
  Pool.make({
    acquire: socket.connect(options),
    size: 2,
  });
