package com.aggregateproject.aggregateconsumer.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ResponseMessage {
    @JsonProperty("transaction_id")
    private String transactionId;

    private Double amount;

    @JsonProperty("request_id")
    private String requestId;

    private Double taxRate;
}

