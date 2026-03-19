#/bin/bash

cd ../1.environment

echo "Iniciando entorno"
docker compose up -d controller-1 controller-2
sleep 15

docker compose up -d broker-1 broker-2
sleep 15

docker compose up -d schema-registry
sleep 15

docker compose up -d connect
sleep 30

docker compose up -d control-center
sleep 15

docker compose up -d ksqldb-server
sleep 20

docker compose up -d ksqldb-cli
sleep 20

docker compose up -d mysql
sleep 20

docker compose up -d mongodb
sleep 20

echo "Creando la tabla transactions"
docker cp ../0.tarea/sql/transactions.sql mysql:/
docker exec mysql bash -c "mysql --user=root --password=password --database=db < /transactions.sql"

sleep 30
echo "Instalando conectores..."
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
docker compose exec connect confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest
docker compose exec connect confluent-hub install --no-prompt jcustenborder/kafka-connect-transform-common:latest

sleep 30
echo "Copiando drivers MySQL..."
docker cp ./mysql/mysql-connector-java-5.1.45.jar connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-java-5.1.45.jar

sleep 30
echo "Copiando schemas AVRO..."
docker cp ../0.tarea/datagen/sensor-telemetry.avsc connect:/home/appuser/
docker cp ../0.tarea/datagen/transactions.avsc connect:/home/appuser/

sleep 30
echo "Reiniciando contenedor connect..."
docker compose restart connect
echo "Esperando reinicio contenedor connect"
sleep 30

echo "OK"
