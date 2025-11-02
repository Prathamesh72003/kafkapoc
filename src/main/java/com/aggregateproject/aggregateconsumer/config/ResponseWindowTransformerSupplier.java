package com.aggregateproject.aggregateconsumer.config;

import com.aggregateproject.aggregateconsumer.dto.ResponseMessage;
import com.aggregateproject.aggregateconsumer.service.TransactionFinalizerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Date;
import java.util.*;

public class ResponseWindowTransformerSupplier implements ValueTransformerWithKeySupplier<String, String, String> {

    private final TransactionFinalizerService finalizer;
    private final ObjectMapper mapper;

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

            private static final String WINDOW_STORE_NAME = "window-store";
            private static final String CLOSED_STORE_NAME = "closed-store";

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.windowStore = (KeyValueStore<String, String>) context.getStateStore(WINDOW_STORE_NAME);
                this.closedStore = (KeyValueStore<String, String>) context.getStateStore(CLOSED_STORE_NAME);

                System.out.println("[INIT] Transformer initialized at " + new Date());
                System.out.println("[INIT] Scheduling punctuator every 30s...");

                this.context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                    long now = System.currentTimeMillis();
                    System.out.println("[PUNCTUATOR] Running at " + new Date(now));

                    try (KeyValueIterator<String, String> iter = windowStore.all()) {
                        List<String> toRemove = new ArrayList<>();

                        while (iter.hasNext()) {
                            KeyValue<String, String> kv = iter.next();
                            String[] parts = kv.key.split("\\|");
                            if (parts.length != 2) continue;

                            String txnId = parts[0];
                            long rawWindowStart = Long.parseLong(parts[1]);

                            // Defensive: if windowStart looks like seconds (10-digit), convert to millis
                            long windowStart = (rawWindowStart < 1_000_000_000_000L) ? rawWindowStart * 1000L : rawWindowStart;
                            long windowEnd = windowStart + WINDOW_SIZE_MS;

                            if (now >= windowEnd) {
                                System.out.printf("[PUNCTUATOR] Expired window detected for txn %s (start=%d, end=%d) -> %s / %s%n",
                                        txnId, windowStart, windowEnd, new Date(windowStart), new Date(windowEnd));

                                WindowWrapper wrapper = deserializeWrapper(kv.value);
                                if (closedStore.get(txnId) == null) {
                                    try {
                                        finalizer.finalizeTransaction(txnId, wrapper.responses);
                                        closedStore.put(txnId, "CLOSED");
                                        System.out.printf("[PUNCTUATOR] Finalized txn %s successfully%n", txnId);
                                    } catch (Exception ex) {
                                        System.err.printf("[ERROR] Finalization failed for txn %s: %s%n", txnId, ex.getMessage());
                                        ex.printStackTrace();
                                    }
                                }
                                toRemove.add(kv.key);
                            }
                        }

                        for (String key : toRemove) {
                            windowStore.delete(key);
                            System.out.printf("[PUNCTUATOR] Cleaned up expired key %s%n", key);
                        }
                    } catch (Exception ex) {
                        System.err.println("[ERROR] Punctuator failed: " + ex.getMessage());
                        ex.printStackTrace();
                    }
                });
            }

            @Override
            public String transform(String txnId, String value) {
                if (!StringUtils.hasText(txnId)) return value;

                long now = System.currentTimeMillis();
                long eventTs = context.timestamp();

                if (closedStore.get(txnId) != null) {
                    System.out.printf("[SKIP] Message for closed txn %s ignored at %s%n", txnId, new Date(now));
                    return value;
                }

                Long windowStart = null;
                try {
                    windowStart = finalizer.getTransactionStartEpochMillis(txnId);
                    System.out.println("[DEBUG] Window started (epoch millis from DB): " + windowStart);
                } catch (Exception e) {
                    System.err.println("[WARN] Could not fetch DB start for txn " + txnId + ": " + e.getMessage());
                }

                if (windowStart == null) windowStart = eventTs;

                // Defensive: if windowStart looks like seconds (10-digit), convert to millis
                if (windowStart < 1_000_000_000_000L) {
                    System.out.println("[DEBUG] Detected windowStart appears to be seconds; converting to millis.");
                    windowStart = windowStart * 1000L;
                }

                String storeKey = txnId + "|" + windowStart;
                String existing = windowStore.get(storeKey);
                WindowWrapper wrapper = existing == null ? new WindowWrapper(windowStart) : deserializeWrapper(existing);
                long windowEnd = wrapper.windowStart + WINDOW_SIZE_MS;

                System.out.printf("[DEBUG] txn=%s eventTs=%d windowStart=%d (%s) windowEnd=%d (%s)%n",
                        txnId, eventTs, wrapper.windowStart, new Date(wrapper.windowStart),
                        windowEnd, new Date(windowEnd));

                if (eventTs > windowEnd) {
                    System.out.printf("[LATE] Event for txn %s arrived at %d (window ended %d)%n",
                            txnId, eventTs, windowEnd);
                    return value;
                }

                ResponseMessage resp;
                try {
                    String cleaned = (value == null ? "" : value.trim());
                    if (cleaned.startsWith("\"") && cleaned.endsWith("\""))
                        cleaned = cleaned.substring(1, cleaned.length() - 1);
                    cleaned = cleaned.replace("\\\"", "\"");
                    resp = mapper.readValue(cleaned, ResponseMessage.class);
                } catch (Exception e) {
                    System.err.println("[ERROR] Failed to parse message for txn " + txnId + ": " + e.getMessage());
                    return value;
                }

                wrapper.responses.put(resp.getRequestId(), resp);
                windowStore.put(storeKey, serializeWrapper(wrapper));

                System.out.printf("[UPDATE] Stored response for txn %s, total=%d (window end=%s)%n",
                        txnId, wrapper.responses.size(), new Date(windowEnd));

                try {
                    Set<String> expected = finalizer.getExpectedRequestIds(txnId);
                    if (expected != null && expected.size() == wrapper.responses.size()) {
                        System.out.printf("[EARLY FINALIZE] All responses received for txn %s at %s%n",
                                txnId, new Date(now));
                        finalizer.finalizeTransaction(txnId, wrapper.responses);
                        closedStore.put(txnId, "CLOSED");
                        windowStore.delete(storeKey);
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] During early finalization: " + e.getMessage());
                }

                return value;
            }

            @Override
            public void close() {
                System.out.println("[CLOSE] Transformer closed.");
            }

            private String serializeWrapper(WindowWrapper w) {
                try {
                    return mapper.writeValueAsString(w);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            private WindowWrapper deserializeWrapper(String json) {
                try {
                    if (json == null || json.isEmpty()) return new WindowWrapper();
                    return mapper.readValue(json, WindowWrapper.class);
                } catch (Exception e) {
                    System.err.println("[ERROR] Deserialization failed: " + e.getMessage());
                    return new WindowWrapper();
                }
            }
        };
    }

    private static class WindowWrapper {
        public long windowStart;
        public Map<String, ResponseMessage> responses = new HashMap<>();

        public WindowWrapper() {}
        public WindowWrapper(long start) { this.windowStart = start; }
    }
}
