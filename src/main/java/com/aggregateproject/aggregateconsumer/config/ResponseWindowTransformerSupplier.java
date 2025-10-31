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

/**
 * Transformer supplier that groups responses per transaction using a logical
 * window start obtained from DB (transaction start time). Expiration is based
 * on last-seen event time so windows won't be immediately expired if DB start
 * time is in the past.
 */
public class ResponseWindowTransformerSupplier implements ValueTransformerWithKeySupplier<String, String, String> {

    private final TransactionFinalizerService finalizer;
    private final ObjectMapper mapper;

    // window length (business), e.g. 2 minutes
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

                System.out.println("---- Active Window Store Entries ----");
                try (KeyValueIterator<String, String> it = windowStore.all()) {
                    while (it.hasNext()) {
                        KeyValue<String, String> kv = it.next();
                        System.out.printf("WINDOW [%s] -> %s%n", kv.key, kv.value);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println("---- Closed Store Entries ----");
                try (KeyValueIterator<String, String> it = closedStore.all()) {
                    while (it.hasNext()) {
                        KeyValue<String, String> kv = it.next();
                        System.out.printf("CLOSED [%s] -> %s%n", kv.key, kv.value);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // schedule punctuator every 30 seconds to check for expired windows
                this.context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                    try {
                        long now = System.currentTimeMillis();
                        try (KeyValueIterator<String, String> iter = windowStore.all()) {
                            List<String> toRemove = new ArrayList<>();
                            while (iter.hasNext()) {
                                KeyValue<String, String> kv = iter.next();
                                String storeKey = kv.key; // transactionId|logicalWindowStart
                                String[] parts = storeKey.split("\\|");
                                if (parts.length != 2) continue;
                                String txnId = parts[0];
                                // parse wrapper (lastSeen + responses)
                                WindowWrapper wrapper = deserializeWrapper(kv.value);
                                long lastSeen = wrapper.lastSeen;
                                long windowEnd = lastSeen + WINDOW_SIZE_MS; // expiry relative to last seen
                                if (windowEnd <= now) {
                                    // finalize if not yet closed
                                    if (closedStore.get(txnId) == null) {
                                        finalizer.finalizeTransaction(txnId, wrapper.responses);
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
                    System.out.printf("Received message for closed transaction %s - rejecting%n", txnId);
                    return value;
                }

                long eventTs = context.timestamp(); // event timestamp (processing uses this as lastSeen)
                // fetch DB logical window start (epoch millis). Falls back to event time when absent.
                Long logicalStartMillis = null;
                try {
                    logicalStartMillis = finalizer.getTransactionStartEpochMillis(txnId);
                } catch (Exception e) {
                    System.err.println("Error fetching transaction start time from DB: " + e.getMessage());
                }
                if (logicalStartMillis == null) {
                    // fallback to event-time-aligned bucket
                    long windowStart = (eventTs / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
                    logicalStartMillis = windowStart;
                }

                // compute store key using logical start from DB (so all events for txn map to same logical window)
                String storeKey = txnId + "|" + logicalStartMillis;

                // read existing wrapper (responses + lastSeen)
                String existing = windowStore.get(storeKey);
                WindowWrapper wrapper = existing == null ? new WindowWrapper() : deserializeWrapper(existing);

                // If this event's timestamp is after lastSeen, update lastSeen
                if (eventTs > wrapper.lastSeen) wrapper.lastSeen = eventTs;

                // reject if the event timestamp is so late that it's beyond expiry relative to lastSeen prior to update
                // (we already updated lastSeen using this event, so we need a different check: use previous lastSeen before update)
                // To keep semantics strict: compute windowEnd based on previous lastSeen (if any), else accept.
                // We'll allow the event itself to extend the lastSeen; the punctuator handles finalization.

                // parse incoming payload to ResponseMessage
                ResponseMessage resp;
                try {
                    String cleaned = value == null ? "" : value.trim();
                    if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
                        cleaned = cleaned.substring(1, cleaned.length() - 1);
                    }
                    cleaned = cleaned.replace("\\\"", "\"");
                    resp = mapper.readValue(cleaned, ResponseMessage.class);
                } catch (Exception e) {
                    System.err.println("Failed to parse incoming message json: " + e.getMessage());
                    return value;
                }

                // put response into map (latest wins for same requestId inside window)
                wrapper.responses.put(resp.getRequestId(), resp);

                // persist updated wrapper (with updated lastSeen)
                windowStore.put(storeKey, serializeWrapper(wrapper));

                // Early-finalization: check expected set from DB
                boolean completeEarly = false;
                try {
                    Set<String> expected = finalizer.getExpectedRequestIds(txnId);
                    if (expected != null && expected.size() == wrapper.responses.size()) {
                        completeEarly = true;
                    }
                } catch (Exception e) {
                    System.err.println("Error while checking expected set: " + e.getMessage());
                }

                if (completeEarly) {
                    try {
                        finalizer.finalizeTransaction(txnId, wrapper.responses);
                        closedStore.put(txnId, "CLOSED");
                        // remove window store entry
                        windowStore.delete(storeKey);
                    } catch (Exception ex) {
                        System.err.println("Error finalizing early for txn " + txnId + ": " + ex.getMessage());
                    }
                }

                // return original value (we are not altering downstream messages)
                return value;
            }

            @Override
            public void close() {
                // nothing
            }

            // wrapper helpers ------------------------------------------------

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
                    // generic map to wrapper (we rely on Jackson's binding)
                    return mapper.readValue(json, WindowWrapper.class);
                } catch (Exception e) {
                    System.err.println("Failed to deserialize wrapper: " + e.getMessage());
                    return new WindowWrapper();
                }
            }
        };
    }

    // simple helper holder class (serialized as JSON into state store)
    private static class WindowWrapper {
        public long lastSeen = 0L;
        public Map<String, ResponseMessage> responses = new HashMap<>();
    }
}
