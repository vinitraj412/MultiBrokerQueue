#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username postgres --dbname "$hostname" <<-EOSQL
	CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'my_replicator_password';
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_read_1');
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_read_2');
EOSQL
mkdir -p "/var/lib/postgresql/data_read_1/"
mkdir -p "/var/lib/postgresql/data_read_2/"

cp /etc/postgresql/postgresql.conf /var/lib/postgresql/data
cp /etc/postgresql/pg_hba.conf /var/lib/postgresql/data


pg_basebackup -D /var/lib/postgresql/data_read_1 -S replication_slot_read_1 -X stream -P -U replicator -Fp -R 
pg_basebackup -D /var/lib/postgresql/data_read_2 -S replication_slot_read_2 -X stream -P -U replicator -Fp -R 

cp /var/lib/postgresql/data/postgresql.conf /var/lib/postgresql/data_read_1/
cp /var/lib/postgresql/data/pg_hba.conf /var/lib/postgresql/data_read_1/

cp /var/lib/postgresql/data/postgresql.conf /var/lib/postgresql/data_read_2/
cp /var/lib/postgresql/data/pg_hba.conf /var/lib/postgresql/data_read_2/



cat > /var/lib/postgresql/data_read_1/postgresql.auto.conf << EOF 
primary_conninfo = 'host=main_db_three port=5432 user=replicator password=my_replicator_password'
primary_slot_name = 'replication_slot_read_1'
EOF

cat > /var/lib/postgresql/data_read_2/postgresql.auto.conf << EOF 
primary_conninfo = 'host=main_db_three port=5432 user=replicator password=my_replicator_password'
primary_slot_name = 'replication_slot_read_2'
EOF
