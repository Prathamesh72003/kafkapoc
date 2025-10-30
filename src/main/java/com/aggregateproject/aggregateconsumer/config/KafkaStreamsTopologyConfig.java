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

    @Value("${app.topic.in:external_service_topic}")
    private String inputTopic;

    private final TransactionFinalizerService finalizer;
    private final ObjectMapper objectMapper;

    public KafkaStreamsTopologyConfig(TransactionFinalizerService finalizer, ObjectMapper objectMapper) {
        this.finalizer = finalizer;
        this.objectMapper = objectMapper;
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        // 1️⃣ Register the state stores FIRST
        KeyValueBytesStoreSupplier windowStoreSupplier = Stores.persistentKeyValueStore(WINDOW_STORE);
        KeyValueBytesStoreSupplier closedStoreSupplier = Stores.persistentKeyValueStore(CLOSED_STORE);

        builder.addStateStore(
                Stores.keyValueStoreBuilder(windowStoreSupplier, Serdes.String(), Serdes.String())
        );
        builder.addStateStore(
                Stores.keyValueStoreBuilder(closedStoreSupplier, Serdes.String(), Serdes.String())
        );

        // 2️⃣ Then create stream and transform
        KStream<String, String> stream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        stream.transformValues(
                new ResponseWindowTransformerSupplier(finalizer, objectMapper),
                Named.as("response-window-transformer"),
                WINDOW_STORE, CLOSED_STORE
        );

        return stream;
    }
}
