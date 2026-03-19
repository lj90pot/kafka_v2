# shellcheck disable=SC1113
#/bin/bash

cd ../1.environment || exit

echo "Iniciando entorno"
docker compose up -d controller-1 controller-2
sleep 30

docker compose up -d broker-1 broker-2
sleep 30

docker compose up -d schema-registry
sleep 30

docker compose up -d connect
sleep 30

docker compose up -d control-center
sleep 30

docker compose up -d ksqldb-server
sleep 30

docker compose up -d ksqldb-cli
sleep 30

docker compose up -d mysql
sleep 30

docker compose up -d mongodb
sleep 30