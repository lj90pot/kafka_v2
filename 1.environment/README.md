
# Entorno

## Descripción

El entorno local se basa en un docker 🐳 con los siguientes contenedores 

```             
controller-1
controller-2
controller-3
broker-1
broker-2
broker-3
connect
control-center
ksqldb-cli
ksqldb-server
schema-registry
mysql     
```

## Comandos

Para utilizar el entorno utilizaremos comandos. Una vez dentro del directorio _**1.environment**_ 

* arrancar el entorno:

```bash
docker compose up -d
```

* verificar estado de los contenedores:

```bash
docker compose ps
```

* parar el entorno:

```bash
docker compose down -v
```

## URLs

* Control Center : http://localhost:9021
* Schema Registry: http://localhost:8081
* Kafka Connect: http://localhost:8083
* ksqlDB:  


> ⚠️ **NOTA**<br/>El estado de los contenedores no se persiste. Esto quiere decir que el estado y los datos en nuestro cluster se perderán una vez lo paremos 