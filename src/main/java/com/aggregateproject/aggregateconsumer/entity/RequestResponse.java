package com.aggregateproject.aggregateconsumer.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "req_res")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RequestResponse {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "transaction_id")
    private String transactionId;

    private String requestId;

    private String uniqueIdentifier;

    @Column(columnDefinition = "TEXT")
    private String reqJson;

    @Column(columnDefinition = "TEXT")
    private String resJson;

    private Double taxRate;

    private LocalDateTime startTime;
}

