package com.aggregateproject.aggregateconsumer.config;

import com.aggregateproject.aggregateconsumer.service.TransactionFinalizerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsTopologyConfig {

    private static final String WINDOW_STORE = "window-store";
    private static final String CLOSED_STORE = "closed-store";

    @Value("${app.topic.producer:producer_kafka_topic}")
    private String producerTopic;

    @Value("${app.topic.external:external_service_topic}")
    private String externalTopic;

    private final TransactionFinalizerService finalizer;
    private final ObjectMapper objectMapper;

    public KafkaStreamsTopologyConfig(TransactionFinalizerService finalizer, ObjectMapper objectMapper) {
        this.finalizer = finalizer;
        this.objectMapper = objectMapper;
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        // Define state stores
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(WINDOW_STORE),
                        Serdes.String(),
                        Serdes.String()
                )
        );
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(CLOSED_STORE),
                        Serdes.String(),
                        Serdes.String()
                )
        );

        // Streams
        KStream<String, String> producerStream = builder.stream(producerTopic, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> externalStream = builder.stream(externalTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // Tag and merge
        KStream<String, String> merged = producerStream
                .mapValues(v -> "{\"source\":\"producer\",\"payload\":" + v + "}")
                .merge(externalStream.mapValues(v -> "{\"source\":\"external\",\"payload\":" + v + "}"));

        // Use our unified transformer
        merged.transformValues(
                new ResponseWindowTransformerSupplier(finalizer, objectMapper),
                Named.as("response-window-transformer"),
                WINDOW_STORE, CLOSED_STORE
        );

        return merged;
    }
}
