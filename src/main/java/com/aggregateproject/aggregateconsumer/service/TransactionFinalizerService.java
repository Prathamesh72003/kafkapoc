package com.aggregateproject.aggregateconsumer.service;

import com.aggregateproject.aggregateconsumer.dto.ResponseMessage;
import com.aggregateproject.aggregateconsumer.entity.RequestResponse;
import com.aggregateproject.aggregateconsumer.repo.RequestResponseRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TransactionFinalizerService {

    private final RequestResponseRepository repository;
    private final ObjectMapper mapper;

    public TransactionFinalizerService(RequestResponseRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /**
     * Finalize a transaction window: update req_res rows based on provided responses map.
     *
     * This method is transactional and intended to be idempotent (writing same values repetitively is OK).
     *
     * @param transactionId transaction id
     * @param responses map requestId -> ResponseMessage (only responses collected within window)
     */
    @Transactional
    public void finalizeTransaction(String transactionId, Map<String, ResponseMessage> responses) {
        System.out.println("Finalizing transaction: " + transactionId);
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);

        // If no rows for that transaction => nothing to do
        if (rows == null || rows.isEmpty()) {
            return;
        }

        // update each row depending on whether a response was provided
        for (RequestResponse row : rows) {
            ResponseMessage resp = responses.get(row.getRequestId());
            if (resp != null) {
                try {
                    row.setResJson(mapper.writeValueAsString(resp));
                } catch (Exception e) {
                    row.setResJson(resp.toString());
                }
                row.setTaxRate(resp.getTaxRate());
            } else {
                // missing response -> default values
                row.setResJson(null);
                row.setTaxRate(5.0);
            }
            repository.save(row);
        }
    }

    /**
     * Returns the expected requestIds for the given transaction.
     */
    public Set<String> getExpectedRequestIds(String transactionId) {
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);
        if (rows == null) return Collections.emptySet();
        return rows.stream().map(RequestResponse::getRequestId).collect(Collectors.toSet());
    }

    /**
     * Return the transaction's start time as epoch milli according to the existing req_res rows.
     * We assume that the request rows for the same transaction share the same startTime (created earlier).
     * If none available, returns null (caller should fall back).
     */
    public Long getTransactionStartEpochMillis(String transactionId) {
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);
        if (rows == null || rows.isEmpty()) return null;
        // find first non-null startTime
        for (RequestResponse r : rows) {
            if (r.getStartTime() != null) {
                return r.getStartTime().toInstant(ZoneOffset.UTC).toEpochMilli();
            }
        }
        return null;
    }
}
