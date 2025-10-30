package com.aggregateproject.aggregateconsumer.service;

import com.aggregateproject.aggregateconsumer.dto.ResponseMessage;
import com.aggregateproject.aggregateconsumer.entity.RequestResponse;
import com.aggregateproject.aggregateconsumer.repo.RequestResponseRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
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
        System.out.println("Transactionid: "+ transactionId);
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);

        // If no rows for that transaction => nothing to do
        if (rows == null || rows.isEmpty()) {
            return;
        }

        Map<String, RequestResponse> byRequestId = new HashMap<>();
        for (RequestResponse r : rows) {
            byRequestId.put(r.getRequestId(), r);
        }

        for (RequestResponse row : rows) {
            ResponseMessage resp = responses.get(row.getRequestId());
            if (resp != null) {
                // update with response
                try {
                    row.setResJson(mapper.writeValueAsString(resp));
                } catch (Exception e) {
                    // fallback to basic string
                    row.setResJson(resp.toString());
                }
                row.setTaxRate(resp.getTaxRate());
            } else {
                // missing response -> default values
                row.setResJson(null);
                row.setTaxRate(5.0);
            }
            // update timestamp
            row.setStartTime(LocalDateTime.now());
            repository.save(row);
        }
    }

    public Set<String> getExpectedRequestIds(String transactionId) {
        System.out.println("Transactionid: "+ transactionId);
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);
        System.out.println("Expected count is: "+ rows);
        return rows.stream().map(RequestResponse::getRequestId).collect(Collectors.toSet());
    }

}

