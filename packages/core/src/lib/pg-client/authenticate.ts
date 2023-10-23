import * as P from 'parser-ts/Parser';
import { pipe } from 'fp-ts/function';
import { Effect } from 'effect';
import { BinaryLike, createHash, createHmac, randomBytes } from 'crypto';
import {
  PgFailedAuth,
  isBackendKeyData,
  isParameterStatus,
  isReadyForQuery,
  item,
  write,
  readOrFail,
  readUntilReady,
} from './util';
import { hasTypeOf } from '../stream';
import { Duplex } from 'stream';
import * as _ from 'lodash';

const sha256 = (text: BinaryLike) => {
  return createHash('sha256').update(text).digest();
};

const hmacSha256 = (key: BinaryLike, msg: BinaryLike) => {
  return createHmac('sha256', key).update(msg).digest();
};

const xorBuffers = (a: Buffer, b: Buffer) =>
  Buffer.from(a.map((_, i) => a[i] ^ b[i]));

const Hi = (password: string, saltBytes: Buffer, iterations: number) => {
  let ui1 = hmacSha256(
    password,
    Buffer.concat([saltBytes, Buffer.from([0, 0, 0, 1])])
  );

  let ui = ui1;
  _.times(iterations - 1, () => {
    ui1 = hmacSha256(password, ui1);
    ui = xorBuffers(ui, ui1);
  });

  return ui;
};

export const authenticate = ({
  socket,
  database,
  username,
  password,
  replication,
}: {
  socket: Duplex;
  database: string;
  username: string;
  password: string;
  replication?: boolean;
}) =>
  Effect.gen(function* (_) {
    const parameters = [
      {
        name: 'database',
        value: database,
      },
      {
        name: 'user',
        value: username,
      },
    ];
    if (replication) {
      parameters.push({
        name: 'replication',
        value: 'database',
      });
    }
    yield* _(
      write(socket)({
        type: 'StartupMessage',
        protocolVersion: 196608,
        parameters,
      })
    );

    const reply = yield* _(
      readOrFail(socket)(
        'AuthenticationCleartextPassword',
        'AuthenticationMD5Password',
        'AuthenticationSASL'
      )
    );

    // CLEARTEXT PASSWORD
    if (reply.type === 'AuthenticationCleartextPassword') {
      yield* _(
        write(socket)({
          type: 'PasswordMessage',
          password,
        })
      );
      // MD5 HASHED PASSWORD
    } else if (reply.type === 'AuthenticationMD5Password') {
      yield* _(
        write(socket)({
          type: 'PasswordMessage',
          password: createHash('md5')
            .update(
              createHash('md5')
                .update(password.concat(username))
                .digest('hex')
                .concat(Buffer.from(reply.salt).toString('hex'))
            )
            .digest('hex'),
        })
      );
      // SASL
    } else {
      const mechanism = reply.mechanisms.find(
        (item) => item === 'SCRAM-SHA-256'
      );

      if (!mechanism) {
        return yield* _(
          Effect.fail(
            new PgFailedAuth({
              reply,
              msg: 'SCRAM-SHA-256 not a supported mechanism',
            })
          )
        );
      }

      const clientNonce = randomBytes(18).toString('base64');
      const clientFirstMessageHeader = 'n,,';
      const clientFirstMessageBody = `n=*,r=${clientNonce}`;
      const clientFirstMessage = `${clientFirstMessageHeader}${clientFirstMessageBody}`;

      yield* _(
        write(socket)({
          type: 'SASLInitialResponse',
          mechanism,
          clientFirstMessage,
        })
      );

      const saslContinue = yield* _(
        readOrFail(socket)('AuthenticationSASLContinue')
      );

      const { iterationCount, salt, nonce, serverFirstMessage } = saslContinue;
      if (!nonce.startsWith(clientNonce)) {
        yield* _(
          Effect.fail(
            new PgFailedAuth({ reply: saslContinue, msg: 'bad nonce' })
          )
        );
      }

      const saltedPassword = Hi(password, salt, iterationCount);
      const clientKey = hmacSha256(saltedPassword, 'Client Key');
      const storedKey = sha256(clientKey);
      const clientFinalMessageWithoutProof = `c=${Buffer.from(
        clientFirstMessageHeader
      ).toString('base64')},r=${nonce}`;
      const authMessage = `${clientFirstMessageBody},${serverFirstMessage},${clientFinalMessageWithoutProof}`;

      const clientSignature = hmacSha256(storedKey, authMessage);
      const clientProofBytes = xorBuffers(clientKey, clientSignature);
      const clientProof = clientProofBytes.toString('base64');

      const serverKey = hmacSha256(saltedPassword, 'Server Key');
      const serverSignature = hmacSha256(serverKey, authMessage);
      const clientFinalMessage =
        clientFinalMessageWithoutProof + ',p=' + clientProof;

      yield* _(write(socket)({ type: 'SASLResponse', clientFinalMessage }));

      const saslFinal = yield* _(readOrFail(socket)('AuthenticationSASLFinal'));

      if (
        serverSignature.compare(
          saslFinal.serverFinalMessage.serverSignature
        ) !== 0
      ) {
        yield* _(
          Effect.fail(
            new PgFailedAuth({
              reply: saslFinal,
              msg: 'expected signature to match',
            })
          )
        );
      }
    }

    yield* _(readOrFail(socket)('AuthenticationOk'));

    const results = yield* _(
      readUntilReady(socket)(
        pipe(
          item,
          P.filter(hasTypeOf('ParameterStatus', 'BackendKeyData')),
          P.many,
          P.chainFirst(() =>
            pipe(
              item,
              P.filter(isReadyForQuery),
              P.chain(() => P.eof())
            )
          )
        )
      )
    );

    const serverParameters = new Map(
      results.filter(isParameterStatus).map(({ name, value }) => [name, value])
    );

    const backendKeyData = results.find(isBackendKeyData);

    return { serverParameters, backendKeyData };
  });
