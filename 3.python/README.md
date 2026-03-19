
# PYTHON 

## Objetivo

Asimilar los conceptos mediante el uso del lenguaje python 🐍

## Dependencias

Para poder hacer uso de los dos ficheros python necesitamos instalar la dependencia **confluent_kafka** usando **pip**

```bash
python -m venv kafka
source kafka/bin/activate
pip install --upgrade pip
pip install confluent_kafka 
```


[Confluent Kafka Python](https://github.com/confluentinc/confluent-kafka-python)

## Producer API

El fichero **python_producer.py** es una aplicación python sencilla con dos funcionalidades:

- listar los topics del cluster

```bash
python python_producer.py list-topics   
```
- producir mensajes en un topic

Recibe tres parámetros: topic, key y el value. A continuación un ejemplo

```bash
python python_producer.py produce -t temperature-telemetry-simple -k '1' -v '{"id": 1, "temperature": 15}'  
```
## Consumer API

El fichero **python_consumer.py** es una aplicación python que permite consumir mensajes de un topic

Recibe dos parámetros posicionales: el topic y la duración en segundos que estará consumiendo. A continuación un ejemplo de consumir durante un minuto (60s) 

```bash
python python_consumer.py temperature-telemetry-simple 300
```
> ⚠️ **NOTA**<br/>La configuración tanto del productor como del consumidor están harcodeadas en los propios ficheros python 🙅‍

> 💊 **NOTA**<br/>Lee el código de ambos programas<br/>Analiza el API de [confluent_kafka](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)<br/> Trata de hacer pequeñas modificaciones
