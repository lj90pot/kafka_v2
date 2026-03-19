# 📄 Tarea Luis Javier Blanco Diaz

[Mi repositorio en GitHub](https://github.com/lj90pot/kafka_v2)

## 1. Introducción

Levantamos un entorno para nuestra empresa Farmia donde recibimos las medidas de sensores de nuestras plantaciones. Tambien recibimos datos de las transacciones del departamento de ventas.

Debido a la capacidad de mi ordenador yo he cofigurado el entorno para tener dos controllers con dos brokers ya que con mas el pc tiene problemas de rendimiento.
También he modificado el archivo setup.sh para que vaya levantando los containers poco a poco. Si no me da error o los contenedores empiezan a caer.

## 2. Resolucion de tareas
#### 🧩 Tarea 1

El schema avro para el topic sensor-telemetry.avsc es:
```
{
  "type": "record",
  "name": "SensorTelemetry",
  "namespace": "com.ucmmaster.kafka.data.telemetry.iot",
  "fields": [
    {
      "name": "sensor_id",
      "type": {
        "type": "string",
        "arg.properties": {
          "regex": "sensor_[0-9]{3}"
        }
      }
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "arg.properties": {
          "range": {
            "min": 1673548200000,
            "max": 1673549200000
          }
        }
      }
    },
    {
      "name": "temperature",
      "type": {
        "type": "double",
        "arg.properties": {
          "range": {
            "min": 10,
            "max": 40
          }
        }
      }
    },
    {
      "name": "humidity",
      "type": {
        "type": "double",
        "arg.properties": {
          "range": {
            "min": 20,
            "max": 90
          }
        }
      }
    },
    {
      "name": "soil_fertility",
      "type": {
        "type": "double",
        "arg.properties": {
          "range": {
            "min": 0,
            "max": 100
          }
        }
      }
    }
  ]
}
```
El connector que genera datos aleatorios es el siguiente:
```
{
  "name": "source-datagen-telemetry",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "sensor-telemetry",
    "max.interval": "1000",
    "iterations": "10000000",
    "tasks.max": "1",
    "schema.filename": "/home/appuser/sensor-telemetry.avsc",
    "schema.keyfield": "sensor_id",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```
Se producen mensajes como los que vemos en la imagen.

![mensajes telemetry](../.assets/imagen1.PNG)

#### 🧩 Tarea 2

Creo un datagen como el anterior pero para las transacciones. El schema transactions.avsc:
```
{
  "namespace": "com.farmia.sales",
  "name": "SalesTransaction",
  "type": "record",
  "fields": [
    {
      "name": "transaction_id",
      "type": {
        "type": "string",
        "arg.properties": {
          "regex": "tx[1-9]{5}"
        }
      }
    },
    {
      "name": "product_id",
      "type": {
        "type": "string",
        "arg.properties": {
          "regex": "prod_[1-9]{3}"
        }
      }
    },
    {
      "name": "category",
      "type": {
        "type": "string",
        "arg.properties": {
          "options": [
            "fertilizers",
            "seeds",
            "pesticides",
            "equipment",
            "supplies",
            "soil"
          ]
        }
      }
    },
    {
      "name": "quantity",
      "type": {
        "type": "int",
        "arg.properties": {
          "range": {
            "min": 1,
            "max": 10
          }
        }
      }
    },
    {
      "name": "price",
      "type": {
        "type": "float",
        "arg.properties": {
          "range": {
            "min": 10.00,
            "max": 200.00
          }
        }
      }
    }
  ]
}
```
El connector source-datagen para transacciones:
```
{
  "name": "source-datagen-_transactions",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "_transactions",
    "schema.filename": "/home/appuser/transactions.avsc",
    "schema.keyfield": "transaction_id",
    "max.interval": 1000,
    "iterations": 10000000,
    "tasks.max": "1",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```
Una vez tengo el topic hago un connector sink para enviar los datos a mysql. aqui he tenido que poner el modo "insert.mode": "upsert" pues en algunos mensajes sobre todo al iniciar el conector daba la casualidad que tenian la misma key y se producian en el mismo segundo. Por lo tanto daba error al intentar meter dos records en la tabla. Importante es que aqui el precio se inserta con el formato adecuado. Esto es relevante en los siguientes pasos:

```
{
  "name": "sink-mysql-_transactions",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
    "topics": "_transactions",
    "table.name.format": "sales_transactions",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```
compruebo que los datos se guardan con los siguientes comandos:
```
docker exec -it mysql mysql -u user -ppassword -D db -e "SELECT COUNT(*) FROM db.sales_transactions;"
```
```
docker exec -it mysql mysql -u user -ppassword -D db -e "SELECT * FROM db.sales_transactions LIMIT 5;"
```
En la siguiente imagen2 se ve una muestra de los registros en la tabla mysql
![tabla mysql](../.assets/imagen2.PNG)

El siguiente paso es el conector source desde mysql:
```
{
  "name": "source-mysql-sales_transactions",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "1",

    "connection.url": "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
    "dialect.name": "MySqlDatabaseDialect",

    "table.whitelist": "sales_transactions",
    "mode": "timestamp",
    "timestamp.column.name": "timestamp_ts",

    "topic.prefix": "db.",
    "poll.interval.ms": "5000",
    "batch.max.rows": "500",

    "numeric.mapping": "none",
    "validate.non.null": "false",

    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "db\\.sales_transactions",
    "transforms.route.replacement": "sales_transactions",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```
En este conector usamos el avro converter para el value del topic. Esto hace que el precio salga en binario en control center.
He intentado hacerlo en json pero entonces los siguientes pasos no funcionan. Y por lo que he visto es un problema de visualización ya que en las siguientes aplicaciones el precio se puede operar matematicamente.

Imagen 3 con ejemplos del topic sales_transactions
![sales_transactions](../.assets/imagen3.PNG)


#### 🧩 Tarea 3

Creo el topic para escribir los resultados:
```
docker exec -it broker-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic sensor-alerts \
  --partitions 1 \
  --replication-factor 1
```
Esta es la aplicación y produce un mensaje por cada alerta que se supera. Esto quiere decir que si un mensaje cumple las dos condiciones se generan dos alertas.
```
package com.farmia.streaming;

import com.ucmmaster.kafka.data.telemetry.iot.SensorTelemetry;
import com.farmia.iot.SensorAlerts;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SensorAlerterApp {

    private static final double TEMPERATURE_THRESHOLD = 35.0;
    private static final double HUMIDITY_THRESHOLD = 20.0;
    private static final String INPUT_TOPIC = "sensor-telemetry";
    private static final String OUTPUT_TOPIC = "sensor-alerts";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static Topology createTopology() {

        final Map<String, String> serdeConfig =
                Collections.singletonMap("schema.registry.url", SCHEMA_REGISTRY_URL);

        //serde para el input se coge el esquema
        Serde<SensorTelemetry> sensorTelemetrySerde = new SpecificAvroSerde<>();
        sensorTelemetrySerde.configure(serdeConfig, false);

        Serde<SensorAlerts> sensorAlertSerde = new SpecificAvroSerde<>();
        sensorAlertSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, SensorTelemetry> telemetryStream =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), sensorTelemetrySerde));

        // ALERTA: temperatura alta
        telemetryStream
                //esto imprime el message
                .peek((key, value) ->
                        System.out.println("Incoming record - key=" + key + ", value=" + value))
                //filtrar los mensajes
                .filter((key, value) ->
                        value != null &&
                                value.getTemperature() > TEMPERATURE_THRESHOLD
                )
                //escribe la nueva key
                .selectKey((key, value) -> value.getSensorId().toString())
                //escribe los valores de los campos para el output topic
                .mapValues(value -> SensorAlerts.newBuilder()
                        .setSensorId(value.getSensorId().toString())
                        .setAlertType("HIGH_TEMPERATURE")
                        .setTimestamp(System.currentTimeMillis())
                        .setDetails("Temperature exceeded " + TEMPERATURE_THRESHOLD + "°C")
                        .build()
                )
                //imprimimos el resultado
                .peek((key, value) ->
                        System.out.println("Outgoing alert - key=" + key + ", value=" + value))
                //publicamos el resultado
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), sensorAlertSerde));

        // ALERTA: humedad baja
        telemetryStream
                .filter((key, value) ->
                                value != null &&
                                        value.getHumidity() < HUMIDITY_THRESHOLD
                )
                .selectKey((key, value) -> value.getSensorId().toString())
                .mapValues(value -> SensorAlerts.newBuilder()
                                .setSensorId(value.getSensorId().toString())
                                .setAlertType("LOW_HUMIDITY")
                                .setTimestamp(System.currentTimeMillis())
                                .setDetails("Humidity threshold not reached. Below " + HUMIDITY_THRESHOLD + "%")
                                .build()
                )
                .peek((key, value) ->
                        System.out.println("Outgoing humidity alert - key=" + key + ", value=" + value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), sensorAlertSerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        System.out.println("java.version = " + System.getProperty("java.version"));
        System.out.println("java.home = " + System.getProperty("java.home"));

        //Properties props = ConfigLoader.getProperties();
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alert-app");
        // Cargamos la configuración lo pongo asi porque no se como funciona el config loader
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alert-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/kafka-streams-state");

        // Creamos la topologia
        Topology topology = createTopology();
        System.out.println(topology.describe());

        //instacio kafkastream
        KafkaStreams streams = new KafkaStreams(topology, props);

        //comprobaciones
        System.out.println(SensorTelemetry.class.getName());
        System.out.println(SensorTelemetry.class.getProtectionDomain().getCodeSource().getLocation());

        // Iniciar Kafka Streams
        streams.start();

        // Parada controlada en caso de apagado
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}


```
![mensajes producidos](../.assets/gif1_tarea3.gif)

![mensajes en control center](../.assets/gif2_tarea3.gif)


#### 🧩 Tarea 4

En este kafka streams lo primero que he hecho es que el serde para el input es GenericRecord esto lo he tenido que hacer despues de probar a usar el avro schema del topic sales_transactions, pero solo de esta manera se pueden hacer calculos con el precio.

Uso una ktable para hacer el groupby y poner un window de 1 minuto. Esto es necesario porque el group by siempre me devuelve una ktable, ademas los resultados se van guardando en un topic intermedio hasta que la ventana se cierra y se pueden emitir los resultados. Lo que hace es sobreescribir el resultado en la tabla para la misma key que es la categoria.

Hay una funcion para corregir o tratar los precios ya que han dado problemas durante el desarrollo. Y tuve que poner algo para que el precio fuera un numero.


Creo el topic para escribir los resultados:
```
docker exec -it broker-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic sales-summary \
  --partitions 1 \
  --replication-factor 1
```
El codigo de la applicacion:
```
package com.farmia.streaming;

import com.farmia.sales.SalesSummary;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SalesSummaryApp {

    public static Topology createTopology() {
        final String inputTopic = "sales_transactions";
        final String outputTopic = "sales-summary";

        final var serdeConfig =
                Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        //aunque tengo el esquema del input el precio me da error y he usado este generic avro serde que me ha funcionado
        Serde<GenericRecord> salesTransactionSerde = new GenericAvroSerde();
        salesTransactionSerde.configure(serdeConfig, false);

        //para la salida uso el serde definido con el avro de sales-summary.avsc
        Serde<SalesSummary> salesSummarySerde = new SpecificAvroSerde<>();
        salesSummarySerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        //build el kstream
        KStream<String, GenericRecord> salesStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), salesTransactionSerde)
        );

        //defino la ventana de tiempo
        TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        //hago la tabla que lee los mensajes y hace el groupby por categoria
        KTable<Windowed<String>, SalesSummary> aggregated = salesStream
                //agrupo por categoria
                .groupBy(
                        (key, value) -> value.get("category").toString(),
                        Grouped.with(Serdes.String(), salesTransactionSerde)
                )
                //aplico la ventana
                .windowedBy(windows)
                //obtengo los datos
                .aggregate(
                        () -> SalesSummary.newBuilder()
                                .setCategory("")
                                .setTotalQuantity(0)
                                .setTotalRevenue(0.0f)
                                .setWindowStart(0L)
                                .setWindowEnd(0L)
                                .build(),
                        (category, transaction, summary) -> {
                            Integer quantity = (Integer) transaction.get("quantity");
                            float price = extractPrice(transaction.get("price"));

                            return SalesSummary.newBuilder(summary)
                                    .setCategory(category)
                                    .setTotalQuantity(summary.getTotalQuantity() + quantity)
                                    .setTotalRevenue(summary.getTotalRevenue() + (quantity * price))
                                    .build();
                        },
                        //guardar una state en una ktable
                        Materialized.<String, SalesSummary, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sales-summary-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(salesSummarySerde)
                );

        aggregated
                //convertir tabla a stream
                .toStream()
                .map((windowedKey, summary) -> KeyValue.pair(
                        windowedKey.key(),
                        SalesSummary.newBuilder(summary)
                                .setCategory(windowedKey.key())
                                .setWindowStart(windowedKey.window().start())
                                .setWindowEnd(windowedKey.window().end())
                                .build()
                ))
                .to(outputTopic, Produced.with(Serdes.String(), salesSummarySerde));

        return builder.build();
    }

    //esta funcion me ayuda con el problema del precio que no me salia como numero
    private static float extractPrice(Object priceObj) {
        if (priceObj == null) {
            return 0.0f;
        }

        if (priceObj instanceof Float) {
            return (Float) priceObj;
        }

        if (priceObj instanceof Double) {
            return ((Double) priceObj).floatValue();
        }

        if (priceObj instanceof Integer) {
            return ((Integer) priceObj).floatValue();
        }

        if (priceObj instanceof Long) {
            return ((Long) priceObj).floatValue();
        }

        if (priceObj instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) priceObj).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            java.math.BigInteger unscaled = new java.math.BigInteger(bytes);
            java.math.BigDecimal decimal = new java.math.BigDecimal(unscaled, 2);
            return decimal.floatValue();
        }

        throw new IllegalArgumentException(
                "Unsupported price type: " + priceObj.getClass().getName()
        );
    }

    public static void main(String[] args) {
        // Cargamos la configuración lo pongo asi porque no se como funciona el config loader
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/kafka-streams-state");

        // Creamos la topologia
        Topology topology = createTopology();

        //instacio kafkastream
        KafkaStreams streams = new KafkaStreams(topology, props);
        // Iniciar Kafka Streams
        streams.start();

        // Parada controlada en caso de apagado
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

![mensajes en control center_](../.assets/gif1_tarea4.gif)


Imagen 4 sales-summary
#### 🧩 Tarea 5

No es necesario crear la base de datos o la collecion en mongodb. el conector lo crea.
Uso el diccionario que cree al crear las alertas. Esto sucede a traves de esquema registry

Este es el conector sink en mongo db:
```
{
  "name": "mongo-sensor-alerts-sink",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "sensor-alerts",
    "connection.uri": "mongodb://admin:secret123@mongodb:27017/?authSource=admin",
    "database": "iot",
    "collection": "sensor_alerts",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy"
  }
}
```

en mongo compass he podido comprobar que los mensajes entras y que el numero de mensajes aumenta.
![mensajes mongo](../.assets/imagenmongocompass.PNG)
