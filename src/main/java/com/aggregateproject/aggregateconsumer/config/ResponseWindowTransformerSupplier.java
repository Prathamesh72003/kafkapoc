package com.aggregateproject.aggregateconsumer.config;

import com.aggregateproject.aggregateconsumer.dto.ResponseMessage;
import com.aggregateproject.aggregateconsumer.service.TransactionFinalizerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.*;

public class ResponseWindowTransformerSupplier implements ValueTransformerWithKeySupplier<String, String, String> {

    private final TransactionFinalizerService finalizer;
    private final ObjectMapper mapper;

    // 5 minutes window
    private static final long WINDOW_SIZE_MS = Duration.ofMinutes(2).toMillis();

    public ResponseWindowTransformerSupplier(TransactionFinalizerService finalizer, ObjectMapper mapper) {
        this.finalizer = finalizer;
        this.mapper = mapper;
    }

    @Override
    public ValueTransformerWithKey<String, String, String> get() {
        return new ValueTransformerWithKey<>() {

            private ProcessorContext context;
            private KeyValueStore<String, String> windowStore;
            private KeyValueStore<String, String> closedStore;
            private final String WINDOW_STORE_NAME = "window-store";
            private final String CLOSED_STORE_NAME = "closed-store";

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.windowStore = (KeyValueStore<String, String>) context.getStateStore(WINDOW_STORE_NAME);
                this.closedStore = (KeyValueStore<String, String>) context.getStateStore(CLOSED_STORE_NAME);

                // schedule punctuator every 30 seconds to check for expired windows
                this.context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                    try {
                        long now = System.currentTimeMillis();
                        try (KeyValueIterator<String, String> iter = windowStore.all()) {
                            List<String> toRemove = new ArrayList<>();
                            while (iter.hasNext()) {
                                KeyValue<String, String> kv = iter.next();
                                String storeKey = kv.key; // transactionId|windowStart
                                String[] parts = storeKey.split("\\|");
                                if (parts.length != 2) continue;
                                String txnId = parts[0];
                                long windowStart = Long.parseLong(parts[1]);
                                long windowEnd = windowStart + WINDOW_SIZE_MS;
                                if (windowEnd <= now) {
                                    // finalize if not yet closed
                                    if (closedStore.get(txnId) == null) {
                                        Map<String, ResponseMessage> map = deserializeMap(kv.value);
                                        // finalize DB
                                        finalizer.finalizeTransaction(txnId, map);
                                        // mark closed
                                        closedStore.put(txnId, "CLOSED");
                                    }
                                    // remove the window store entry to save space
                                    toRemove.add(storeKey);
                                }
                            }
                            // cleanup
                            for (String k : toRemove) windowStore.delete(k);
                        }
                    } catch (Exception ex) {
                        // log
                        System.err.println("Error during punctuate: " + ex.getMessage());
                        ex.printStackTrace();
                    }
                });
            }

            @Override
            public String transform(String readOnlyKey, String value) {
                // key is transaction_id
                String txnId = readOnlyKey;
                if (!StringUtils.hasText(txnId)) {
                    // ignore unkeyed messages
                    return value;
                }

                // if transaction already closed => reject/log and do nothing
                if (closedStore.get(txnId) != null) {
                    // log or send to DLQ - here we just print
                    System.out.printf("Received message for closed transaction %s - rejecting%n", txnId);
                    return value;
                }

                long ts = context.timestamp(); // message timestamp
                long windowStart = (ts / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;

                long windowEnd = windowStart + WINDOW_SIZE_MS;
                if (ts >= windowEnd) {
                    // message is outside (late beyond window) -> reject
                    System.out.printf("Late message for txn %s, ts=%d, windowEnd=%d - rejecting%n", txnId, ts, windowEnd);
                    return value;
                }

                // store key: txnId|windowStart
                String storeKey = txnId + "|" + windowStart;
                String existing = windowStore.get(storeKey);
                Map<String, ResponseMessage> map = existing == null ? new HashMap<>() : deserializeMap(existing);

                ResponseMessage resp;
                try {
                    String cleaned = value.trim();

                    // If payload is quoted JSON (e.g. "{\"amount\":120,...}") -> strip quotes
                    if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
                        cleaned = cleaned.substring(1, cleaned.length() - 1);
                    }

                    // Replace escaped quotes to restore valid JSON
                    cleaned = cleaned.replace("\\\"", "\"");

                    resp = mapper.readValue(cleaned, ResponseMessage.class);
                } catch (Exception e) {
                    System.err.println("Failed to parse incoming message json: " + e.getMessage());
                    return value;
                }


                // put response into map (latest wins for same requestId inside window)
                map.put(resp.getRequestId(), resp);
                windowStore.put(storeKey, serializeMap(map));

                // check expected set from DB by calling finalizer.service's repository indirectly
                // Note: transactionFinalizerService will call the repository inside finalizeTransaction.
                // Here we need to know expected count, so we fetch it using a small hack: call finalizer repository method via reflection? No.
                // Instead, we rely on finalizer to provide a small helper via public API: we can't change that now.
                // Simpler approach: ask finalizer to check completeness by fetching expected set size using DB.
                // We'll implement a small completeness-check by calling a new method on finalizer that returns expectedSize.
                // But to avoid changing class signatures now, we'll perform completeness detection by trying to finalize only if sizes match.
                // So for simplicity we'll fetch expected set by invoking a finalizer helper if available. But since we can't here,
                // we will rely on a small behavior: finalizer cannot be invoked until punctuator runs. To honor early-finalization requirement,
                // we attempt to detect expected set size by calling a repository via finalizer using reflection fallback.
                boolean completeEarly = false;
                try {
                    // try to call finalizer.getExpectedRequestIds(txnId) if exists
                    Set<String> expected = finalizer.getExpectedRequestIds(txnId);
                    if (expected != null && expected.size() == map.size()) {
                        completeEarly = true;
                    }

                } catch (Exception e) {
                    System.err.println("Error while checking expected set: " + e.getMessage());
                }

                if (completeEarly) {
                    // finalize now
                    try {
                        finalizer.finalizeTransaction(txnId, map);
                        closedStore.put(txnId, "CLOSED");
                        // remove window store entry
                        windowStore.delete(storeKey);
                    } catch (Exception ex) {
                        System.err.println("Error finalizing early for txn " + txnId + ": " + ex.getMessage());
                    }
                }

                // We don't forward transformed records downstream (this transformer returns the original value)
                // Returning null would drop it, but we return the original value to keep the stream semantics.
                return value;
            }

            @Override
            public void close() {
                // nothing
            }

            private String serializeMap(Map<String, ResponseMessage> map) {
                try {
                    return mapper.writeValueAsString(map);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            private Map<String, ResponseMessage> deserializeMap(String json) {
                try {
                    if (json == null) return new HashMap<>();
                    return mapper.readValue(json, new TypeReference<Map<String, ResponseMessage>>() {});
                } catch (Exception e) {
                    System.err.println("Failed to deserialize map: " + e.getMessage());
                    return new HashMap<>();
                }
            }
        };
    }
}

