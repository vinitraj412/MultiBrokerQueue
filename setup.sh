sudo docker compose down
sudo rm -rf ../data/
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
sudo docker compose up -d  main_db_three
echo "Starting postgres_master node..."
sleep 20  # Waits for master note start complete
docker exec -it main_db_three sh /etc/postgresql/init-script/init-db.sh
echo "Restart master node"
sudo docker compose restart main_db_three 
sleep 5

echo "Starting read manager  nodes..."
sudo docker compose up -d  main_db_one
sudo docker compose up -d  main_db_two
sleep 20  # Waits for note start complete

echo "Done"


sudo docker compose up