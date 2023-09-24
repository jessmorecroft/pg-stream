import { connect, end, tlsConnect as clientTlsConnect } from './socket';
import {
  make as makeServer,
  tlsConnect as serverTlsConnect,
  SSLOptions,
} from './server';

export { connect, end, clientTlsConnect, makeServer, serverTlsConnect };
export type { SSLOptions };
