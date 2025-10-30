package com.aggregateproject.aggregateconsumer.repo;

import com.aggregateproject.aggregateconsumer.entity.RequestResponse;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface RequestResponseRepository extends JpaRepository<RequestResponse, Long> {
    List<RequestResponse> findByTransactionId(String transactionId);
}

