#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username postgres --dbname "$hostname" <<-EOSQL
	CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'my_replicator_password';
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_read_1');
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_read_2');
EOSQL
pg_basebackup -D /tmp/postgres_read_1 -S replication_slot_read_1 -X stream -P -U replicator -Fp -R
pg_basebackup -D /tmp/postgres_read_2 -S replication_slot_read_2 -X stream -P -U replicator -Fp -R
