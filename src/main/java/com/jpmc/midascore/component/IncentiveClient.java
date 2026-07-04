package com.jpmc.midascore.component;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class IncentiveClient {

    private final RestTemplate restTemplate;
    private static final String URL = "http://localhost:8080/incentive";

    public IncentiveClient(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public Incentive getIncentive(Transaction transaction) {
        return restTemplate.postForObject(URL, transaction, Incentive.class);
    }
}