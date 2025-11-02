package com.aggregateproject.aggregateconsumer.config;

import com.aggregateproject.aggregateconsumer.dto.ResponseMessage;
import com.aggregateproject.aggregateconsumer.service.TransactionFinalizerService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

                // Schedule cleanup
                this.context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                    long now = System.currentTimeMillis();
                    try (KeyValueIterator<String, String> iter = windowStore.all()) {
                        List<String> expired = new ArrayList<>();
                        while (iter.hasNext()) {
                            KeyValue<String, String> kv = iter.next();
                            String[] parts = kv.key.split("\\|");
                            if (parts.length != 2) continue;

                            String txnId = parts[0];
                            long windowStart = Long.parseLong(parts[1]);
                            long windowEnd = windowStart + WINDOW_SIZE_MS;

                            if (now >= windowEnd && closedStore.get(txnId) == null) {
                                WindowWrapper wrapper = deserializeWrapper(kv.value);
                                System.out.printf("[TIMEOUT] Txn %s expired -> finalizing with defaults%n", txnId);
                                finalizer.finalizeTransaction(txnId, wrapper.responses);
                                closedStore.put(txnId, "CLOSED");
                                expired.add(kv.key);
                            }
                        }
                        expired.forEach(windowStore::delete);
                    } catch (Exception e) {
                        System.err.println("[ERROR] Punctuator failed: " + e.getMessage());
                    }
                });
            }

            @Override
            public String transform(String txnId, String value) {
                if (!StringUtils.hasText(txnId) || value == null) return value;

                try {
                    JsonNode root = mapper.readTree(value);
                    String source = root.path("source").asText();
                    JsonNode payloadNode = root.path("payload");
                    String payload = payloadNode.isTextual() ? payloadNode.asText() : mapper.writeValueAsString(payloadNode);

                    if ("producer".equalsIgnoreCase(source)) {
                        return handleProducerEvent(txnId, payload);
                    } else if ("external".equalsIgnoreCase(source)) {
                        return handleExternalEvent(txnId, payload);
                    } else {
                        System.err.printf("[WARN] Unknown source for txn %s: %s%n", txnId, source);
                    }

                } catch (Exception e) {
                    System.err.println("[ERROR] transform() failed: " + e.getMessage());
                }

                return value;
            }

            private String handleProducerEvent(String txnId, String payload) {
                if (closedStore.get(txnId) != null) {
                    System.out.printf("[SKIP] Producer event for closed txn %s%n", txnId);
                    return payload;
                }

                long start = System.currentTimeMillis();
                String key = txnId + "|" + start;

                if (windowStore.get(key) == null) {
                    windowStore.put(key, serializeWrapper(new WindowWrapper(start)));
                    System.out.printf("[CREATE] Window created for txn %s at %s%n", txnId, new Date(start));
                } else {
                    System.out.printf("[INFO] Window already exists for txn %s%n", txnId);
                }
                return payload;
            }

            private String handleExternalEvent(String txnId, String payload) {
                long eventTs = context.timestamp();
                WindowWrapper wrapper = findExistingWindow(txnId);
                if (wrapper == null) {
                    System.out.printf("[INFO] No active window for %s, creating new one%n", txnId);
                    wrapper = new WindowWrapper(eventTs);
                }

                long windowEnd = wrapper.windowStart + WINDOW_SIZE_MS;
                if (eventTs > windowEnd) {
                    System.out.printf("[LATE] Ignoring late response for txn %s%n", txnId);
                    return payload;
                }

                try {
                    ResponseMessage resp = mapper.readValue(payload, ResponseMessage.class);
                    wrapper.responses.put(resp.getRequestId(), resp);
                    String key = txnId + "|" + wrapper.windowStart;
                    windowStore.put(key, serializeWrapper(wrapper));

                    System.out.printf("[UPDATE] Response stored for txn %s (total=%d)%n", txnId, wrapper.responses.size());

                    Set<String> expected = finalizer.getExpectedRequestIds(txnId);
                    if (expected != null && expected.size() == wrapper.responses.size()) {
                        System.out.printf("[EARLY FINALIZE] All responses received for txn %s%n", txnId);
                        finalizer.finalizeTransaction(txnId, wrapper.responses);
                        closedStore.put(txnId, "CLOSED");
                        windowStore.delete(key);
                    }

                } catch (Exception e) {
                    System.err.printf("[ERROR] Could not parse or handle external response for %s: %s%n", txnId, e.getMessage());
                }

                return payload;
            }

            private WindowWrapper findExistingWindow(String txnId) {
                try (KeyValueIterator<String, String> iter = windowStore.all()) {
                    while (iter.hasNext()) {
                        KeyValue<String, String> kv = iter.next();
                        if (kv.key.startsWith(txnId + "|")) {
                            return deserializeWrapper(kv.value);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] findExistingWindow failed: " + e.getMessage());
                }
                return null;
            }

            private String serializeWrapper(WindowWrapper w) {
                try { return mapper.writeValueAsString(w); }
                catch (Exception e) { throw new RuntimeException(e); }
            }

            private WindowWrapper deserializeWrapper(String json) {
                try {
                    if (json == null || json.isEmpty()) return new WindowWrapper();
                    return mapper.readValue(json, WindowWrapper.class);
                } catch (Exception e) {
                    return new WindowWrapper();
                }
            }

            @Override
            public void close() { System.out.println("[CLOSE] Transformer closed."); }
        };
    }

    private static class WindowWrapper {
        public long windowStart;
        public Map<String, ResponseMessage> responses = new HashMap<>();
        public WindowWrapper() {}
        public WindowWrapper(long start) { this.windowStart = start; }
    }
}
