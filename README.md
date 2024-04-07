# PgStream

This monorepo contains a core library providing a simple PostgreSQL client for querying and logical replication streaming, along with a command-line interface (CLI) library for command-line invocation of this client.

## Installation

Prepare the monorepo:

```bash
npm install
```

## Building

Build all packages:

```bash
npm run build
```

## Testing

To test locally, follow these steps:

```bash
# Start the PostgreSQL Docker container:
docker compose up db -d
# Export required env overrides for local testing:
export PG_HOST=localhost
export NODE_TLS_REJECT_UNAUTHORIZED=0
# Run the tests:
npm run test
```

To test entirely within docker, follow these steps:

```bash
# Build the docker images (always required after dependency changes to re-install node_modules on the containers):
docker compose build
# Run the tests:
npm run docker:test
```

## Develop

To add a feature(s), follow these steps:

1. Make code changes.
2. Run the following command for each feature:
```bash
# Add a changeset, specifying changelog desciptions, version bumps, etc as prompted.
npm run changeset:add
```
3. Once you have added all changes and are nearly ready to publish, you should bump the version(s) and check-in the code. The version bumps will be determined by changesets from all of the applied changesets:
```bash
# Bump the versions:
npm run changeset:version
# Check-in the code:
git commit -a
```

## Publish

To publish, we use changesets again:

```bash
# Publish using changesets:
npm run publish
```

Note that this triggers a prepublish step of building and local testing, so you'll need to ensure the db docker container is running and you environment is ready for testing - see above.
