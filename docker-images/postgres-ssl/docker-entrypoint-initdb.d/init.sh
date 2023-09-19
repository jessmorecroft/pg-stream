#!/bin/sh

mkdir -p ${PGDATA}/ssl

openssl req -new -x509 -days 365 -nodes -text -out ${PGDATA}/ssl/server.crt \
  -keyout ${PGDATA}/ssl/server.key -subj "/CN=db"
chmod og-rwx ${PGDATA}/ssl/server.key
chown -R postgres ${PGDATA}/ssl

cat <<EOS >> ${PGDATA}/postgresql.conf
wal_level = logical			# minimal, replica, or logical
max_wal_senders = 10		# max number of walsender processes
max_replication_slots = 10	# max number of replication slots

ssl = on
ssl_cert_file = '${PGDATA}/ssl/server.crt'
ssl_key_file = '${PGDATA}/ssl/server.key'
EOS

echo "hostssl postgres postgres 0.0.0.0/0 md5" >> ${PGDATA}/pg_hba.conf
