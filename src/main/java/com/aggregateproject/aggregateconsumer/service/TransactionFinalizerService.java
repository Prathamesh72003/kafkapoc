package com.aggregateproject.aggregateconsumer.service;

import com.aggregateproject.aggregateconsumer.dto.ResponseMessage;
import com.aggregateproject.aggregateconsumer.entity.RequestResponse;
import com.aggregateproject.aggregateconsumer.repo.RequestResponseRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.ZoneId;
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

    @Transactional
    public void finalizeTransaction(String transactionId, Map<String, ResponseMessage> responses) {
        System.out.println("Finalizing transaction: " + transactionId);
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);

        if (rows == null || rows.isEmpty()) {
            return;
        }

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
                row.setResJson(null);
                row.setTaxRate(5.0);
            }
            repository.save(row);
        }
    }

    public Set<String> getExpectedRequestIds(String transactionId) {
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);
        if (rows == null) return Collections.emptySet();
        return rows.stream().map(RequestResponse::getRequestId).collect(Collectors.toSet());
    }

    /**
     * Return the transaction's start time as epoch milli according to the existing req_res rows.
     *
     * IMPORTANT: DB stores a LocalDateTime (no timezone). We must interpret that LocalDateTime
     * as the system/local zone (Asia/Kolkata) — not as UTC — otherwise we'll shift the time.
     */
    public Long getTransactionStartEpochMillis(String transactionId) {
        List<RequestResponse> rows = repository.findByTransactionId(transactionId);
        if (rows == null || rows.isEmpty()) return null;
        for (RequestResponse r : rows) {
            if (r.getStartTime() != null) {
                // Log what we got (debug)
                System.out.println("Time is from db: " + r.getStartTime());
                // Interpret the DB LocalDateTime in the system default zone (likely Asia/Kolkata)
                return r.getStartTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
        }
        return null;
    }
}
