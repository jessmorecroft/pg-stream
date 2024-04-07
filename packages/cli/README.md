# pg-stream

**Version**: 2.1.0

## Usage

```bash
$ pg-stream [(-h, --host text)] [(-p, --port integer)] [(-d, --dbname text)] [(-U, --username text)] [(-W, --password text)] [--noSSL] [--output table | json] [--log verbose | quiet]
```

## Description

Simple PostgreSQL client for querying and logical replication streaming.

## Options

- `(-h, --host text)`

  A user-defined piece of text.

  postgres server host

  This option can be set from environment variables.

  This setting is optional.

- `(-p, --port integer)`

  An integer.

  postgres server port

  This option can be set from environment variables.

  This setting is optional.

- `(-d, --dbname text)`

  A user-defined piece of text.

  postgres database

  This option can be set from environment variables.

  This setting is optional.

- `(-U, --username text)`

  A user-defined piece of text.

  database user username

  This option can be set from environment variables.

  This setting is optional.

- `(-W, --password text)`

  A user-defined piece of text.

  database user password

  This option can be set from environment variables.

  This setting is optional.

- `--noSSL`

  A true or false value.

  do not connect to postgres using SSL

  This setting is optional.

- `--output table | json`

  One of the following: table, json

  output display format

  This setting is optional.

- `--log verbose | quiet`

  One of the following: verbose, quiet

  logging level

  This setting is optional.

- `--completions sh | bash | fish | zsh`

  One of the following: sh, bash, fish, zsh

  Generate a completion script for a specific shell

  This setting is optional.

- `(-h, --help)`

  A true or false value.

  Show the help documentation for a command

  This setting is optional.

- `--wizard`

  A true or false value.

  Start wizard mode for a command

  This setting is optional.

- `--version`

  A true or false value.

  Show the version of the application

  This setting is optional.

## Commands

`query (-c, --command text)`

Runs some SQL and prints the results. The SQL may be a single statement or a sequence of statements. When the SQL contains more than one SQL statement (separated by semicolons), those statements are executed as a single transaction, unless explicit transaction control commands are included to force a different behavior. Each result set will be printed separately, either in tabular form or as a single line of JSON, depending on the "output" selected.

`recvlogical (-S, --slot text) (-P, --publication text) [--createSlotIfNone] [--createPublicationIfNone] [--temporary]`

Starts a logical replication stream from the server and prints the events as they are received. Each batch of events associated with a single transaction is printed separately, either in tabular form or as a single line of JSON, depending on the "output" selected. The slot and publication may be optionally created on demand, and may optionally be designated "temporary", in which case they will be removed on exit. The SIGINT (CTRL-C) or SIGTERM signals should be used to shut down the app.
