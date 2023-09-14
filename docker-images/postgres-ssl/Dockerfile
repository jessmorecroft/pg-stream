FROM postgres:15.4-alpine

RUN apk --no-cache add openssl

COPY ./docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

RUN chmod +x /docker-entrypoint-initdb.d/*
