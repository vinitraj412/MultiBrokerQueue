#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'my_replicator_password'
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_read_1');
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_read_2');
EOSQL
