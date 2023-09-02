import { makeValueTypeParser, PgTypeName, PgTypes } from './value-type-parser';
import * as S from 'parser-ts/string';
import * as P from 'parser-ts/Parser';
import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/lib/function';
import { fail } from 'assert';

describe(__filename, () => {
  const verifyParseSuccess = <T>(
    parser: P.Parser<string, T>,
    input: string,
    expected: T
  ) => {
    pipe(
      S.run(input)(parser),
      E.fold(
        ({ expected }) => fail(`expected ${expected}`),
        ({ value }) => {
          expect(value).toEqual(expected);
        }
      )
    );
  };

  const verifyParseFailure = (
    parser: P.Parser<string, unknown>,
    input: string,
    expected?: string
  ) => {
    pipe(
      S.run(input)(parser),
      E.fold(
        (err) => {
          if (expected) {
            expect(err.expected).toEqual([expected]);
          }
        },
        () => fail()
      )
    );
  };

  it('should parse bool', () => {
    const baseParser = makeValueTypeParser(PgTypes.bool.baseTypeOid);
    verifyParseSuccess(baseParser, 't', true);
    verifyParseSuccess(baseParser, 'f', false);
    verifyParseFailure(baseParser, 'false', 'end of file');
    verifyParseFailure(baseParser, 'bogus', 'true (t) or false (f)');

    const arrayParser = makeValueTypeParser(PgTypes.bool.arrayTypeOid);
    verifyParseSuccess(arrayParser, '{t,f}', [true, false]);
    verifyParseSuccess(arrayParser, '{{f,t},{t}}', [[false, true], [true]]);
    verifyParseSuccess(arrayParser, '{{f,{t,{t,f,t}}},{t}}', [
      [false, [true, [true, false, true]]],
      [true],
    ]);
    verifyParseFailure(arrayParser, '{{f,{t,{t,f,t}}},{t}');
    verifyParseFailure(arrayParser, '{{f,{true,{t,f,t}}},{t}}');
  });

  it.each([['int4'], ['int2']] as PgTypeName[][])(
    'should parse int',
    (type) => {
      const baseParser = makeValueTypeParser(PgTypes[type].baseTypeOid);
      verifyParseSuccess(baseParser, '123', 123);
      verifyParseSuccess(baseParser, '-01', -1);
      verifyParseFailure(baseParser, '1.3', 'end of file');
      verifyParseFailure(baseParser, 'bogus', 'an integer');

      const arrayParser = makeValueTypeParser(PgTypes[type].arrayTypeOid);
      verifyParseSuccess(arrayParser, '{1,2}', [1, 2]);
      verifyParseSuccess(arrayParser, '{{-1,-2},{3}}', [[-1, -2], [3]]);
      verifyParseSuccess(arrayParser, '{{1,{2,{3,4,5}}},{6}}', [
        [1, [2, [3, 4, 5]]],
        [6],
      ]);
      verifyParseFailure(arrayParser, '{{1,{2,{3,4,5}}},{6}');
      verifyParseFailure(arrayParser, '{{1,{2.3,{4,5,6}}},{7}}');
    }
  );

  it('should parse bigint', () => {
    const baseParser = makeValueTypeParser(PgTypes.int8.baseTypeOid, {
      parseBigInts: true,
    });
    verifyParseSuccess(baseParser, '123', 123n);
    verifyParseSuccess(baseParser, '-01', -1n);
    verifyParseFailure(baseParser, '1.3', 'end of file');
    verifyParseFailure(baseParser, 'bogus', 'an integer');

    const arrayParser = makeValueTypeParser(PgTypes.int8.arrayTypeOid, {
      parseBigInts: true,
    });
    verifyParseSuccess(arrayParser, '{1,2}', [1n, 2n]);
    verifyParseSuccess(arrayParser, '{{-1,-2},{3}}', [[-1n, -2n], [3n]]);
    verifyParseSuccess(arrayParser, '{{1,{2,{3,4,5}}},{6}}', [
      [1n, [2n, [3n, 4n, 5n]]],
      [6n],
    ]);
    verifyParseFailure(arrayParser, '{{1,{2,{3,4,5}}},{6}');
    verifyParseFailure(arrayParser, '{{1,{2.3,{4,5,6}}},{7}}');
  });

  it.each([['float4'], ['float8', 'numeric']] as PgTypeName[][])(
    'should parse float',
    (type) => {
      const baseParser = makeValueTypeParser(PgTypes[type].baseTypeOid, {
        parseNumerics: true,
      });
      verifyParseSuccess(baseParser, '123.456', 123.456);
      verifyParseSuccess(baseParser, '01', 1);
      verifyParseFailure(baseParser, '1 .3', 'end of file');
      verifyParseFailure(baseParser, '+1.2', 'a float');

      const arrayParser = makeValueTypeParser(PgTypes[type].arrayTypeOid, {
        parseNumerics: true,
      });
      verifyParseSuccess(arrayParser, '{1,2}', [1, 2]);
      verifyParseSuccess(arrayParser, '{{1,2},{3}}', [[1, 2], [3]]);
      verifyParseSuccess(arrayParser, '{{1,{2.3,{3.1415,4,5}}},{6}}', [
        [1, [2.3, [3.1415, 4, 5]]],
        [6],
      ]);
      verifyParseFailure(arrayParser, '{{1,{2,{3,4,5}}},{6}');
      verifyParseFailure(arrayParser, '{{1,{2 .3,{4,5,6}}},{7}}');
    }
  );

  it.each([['json'], ['jsonb']] as PgTypeName[][])(
    'should parse json',
    (type) => {
      const baseParser = makeValueTypeParser(PgTypes[type].baseTypeOid);
      verifyParseSuccess(baseParser, '{"hello":1}', { hello: 1 });
      verifyParseSuccess(baseParser, '{"hello":[1,2,3]}', { hello: [1, 2, 3] });
      verifyParseFailure(
        baseParser,
        '{hello:1}',
        "JSON, but got: Expected property name or '}' in JSON at position 1"
      );
      verifyParseFailure(
        baseParser,
        '{"hello":123n}',
        "JSON, but got: Expected ',' or '}' after property value in JSON at position 12"
      );
      const arrayParser = makeValueTypeParser(PgTypes[type].arrayTypeOid);
      verifyParseSuccess(arrayParser, '{"[1,2,3]"}', [[1, 2, 3]]);
      verifyParseSuccess(arrayParser, '{"{\\"hello\\":[1,2,3]}"}', [
        { hello: [1, 2, 3] },
      ]);
      verifyParseSuccess(
        arrayParser,
        '{{"{\\"hello\\":[1,2,3]}","[1,2,3]"},{"{\\"hello\\":{\\"goodbye\\":[1,2,3]}}"}}',
        [[{ hello: [1, 2, 3] }, [1, 2, 3]], [{ hello: { goodbye: [1, 2, 3] } }]]
      );
      verifyParseFailure(arrayParser, '{"[1,2,3]"');
      verifyParseFailure(
        arrayParser,
        '{{"{\\"hello":[1,2,3]}","[1,2,3]"},{"{\\"hello\\":{\\"goodbye\\":[1,2,3]}}"}}'
      );
    }
  );

  it.each([['date'], ['timestamptz'], ['timestamp']] as PgTypeName[][])(
    'should parse dates',
    (type) => {
      const baseParser = makeValueTypeParser(PgTypes[type].baseTypeOid, {
        parseDates: true,
      });
      verifyParseSuccess(
        baseParser,
        '2012-01-01',
        new Date(Date.UTC(2012, 0, 1))
      );
      verifyParseSuccess(
        baseParser,
        '2022-05-08 03:45:04.088764+00',
        new Date(Date.UTC(2022, 4, 8, 3, 45, 4, 88))
      );
      verifyParseSuccess(
        baseParser,
        '2022-05-08 03:45:04.088764+02',
        new Date(Date.UTC(2022, 4, 8, 1, 45, 4, 88))
      );
      verifyParseSuccess(
        baseParser,
        '2022-05-08 03:45:04.088764',
        new Date(2022, 4, 8, 3, 45, 4, 88)
      );
      const arrayParser = makeValueTypeParser(PgTypes[type].arrayTypeOid, {
        parseDates: true,
      });
      verifyParseSuccess(arrayParser, '{"2012-01-01"}', [
        new Date(Date.UTC(2012, 0, 1)),
      ]);
      verifyParseSuccess(
        arrayParser,
        '{{"2022-05-08 03:45:04.088764",{"2012-01-01",{"2022-05-08 03:45:04.088764+00","2022-05-08 03:45:04.088764+02"}}}}',
        [
          [
            new Date(2022, 4, 8, 3, 45, 4, 88),
            [
              new Date(Date.UTC(2012, 0, 1)),
              [
                new Date(Date.UTC(2022, 4, 8, 3, 45, 4, 88)),
                new Date(Date.UTC(2022, 4, 8, 1, 45, 4, 88)),
              ],
            ],
          ],
        ]
      );
      verifyParseFailure(arrayParser, '{{"2012-01-01"}');
      verifyParseFailure(arrayParser, '{{"2012-01-01"},{"2012-13-01"}}');
    }
  );

  it.each([
    ['varchar'],
    ['date'],
    ['int8'],
    ['timestamptz'],
    ['numeric'],
  ] as PgTypeName[][])('should parse everything else straight thru', (type) => {
    const baseParser = makeValueTypeParser(PgTypes[type].baseTypeOid);
    verifyParseSuccess(baseParser, 'xyz', 'xyz');
    verifyParseSuccess(baseParser, '', '');
    const arrayParser = makeValueTypeParser(PgTypes[type].arrayTypeOid);
    verifyParseSuccess(arrayParser, '{"xyz"}', ['xyz']);
    verifyParseSuccess(
      arrayParser,
      '{{"xyz",{"abc",{"def","de\\"f"}}},{"xxx"}}',
      [['xyz', ['abc', ['def', 'de"f']]], ['xxx']]
    );
    verifyParseFailure(arrayParser, '{{xyz,abc},{def}}');
    verifyParseFailure(arrayParser, '{{"f",{"t",{"t","f","t"}}},{"t"}');
    verifyParseFailure(arrayParser, '{{"f",{true,{"t","f","t"}}},{"t"}}');
  });
});
