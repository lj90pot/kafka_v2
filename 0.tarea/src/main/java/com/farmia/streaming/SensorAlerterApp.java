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

