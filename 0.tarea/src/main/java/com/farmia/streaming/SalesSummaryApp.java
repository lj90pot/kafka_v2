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