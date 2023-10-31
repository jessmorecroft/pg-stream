# @jmorecroft67/pg-stream-core

## 2.2.0

### Minor Changes

- - add new `queryRaw` method
  - add new `queryMany` method
  - add new `queryStreamRaw` method
  - improve `recvlogical` method
    - add processor.key support for "serial", "table"
    - bug fixes
  - consolidate error types in cause type of `PgClientError`
  - improve readme

## 2.1.0

### Minor Changes

- add new `queryStream` method
- improve `recvlogical` method
  - add graceful exit with optional signal
  - confirm latest WAL from keepalives as well as data logs
- expose `query` parsing options

## 2.0.1

### Patch Changes

- fix build config

## 1.0.0

### Major Changes

- initial release of refactor
