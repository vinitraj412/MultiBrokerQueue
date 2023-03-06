sudo docker build . -t mbq:latest
mkdir -p ../data/reverse_proxy
mkdir -p ../data/write_manager
mkdir -p ../data/read_manager_one
mkdir -p ../data/read_manager_two
mkdir -p ../data/broker_one
mkdir -p ../data/broker_two
mkdir -p ../data/broker_three
mkdir -p ../data/main_db_one
mkdir -p ../data/main_db_two
mkdir -p ../data/main_db_three
mkdir -p ../data/db_one
mkdir -p ../data/db_two
mkdir -p ../data/db_three
sudo docker compose up