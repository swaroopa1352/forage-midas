package com.jpmc.midascore.incentive;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

@Service
public class IncentiveService {
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);

    private final RestTemplate restTemplate;
    private final String incentiveUrl;

    public IncentiveService(RestTemplate restTemplate, @Value("${incentive.url:http://localhost:8080/incentive}") String incentiveUrl) {
        this.restTemplate = restTemplate;
        this.incentiveUrl = incentiveUrl;
    }

    /**
     * Calls the external incentives API and returns the incentive amount (0 if none or on error).
     */
    public BigDecimal fetchIncentiveFor(Transaction tx) {
        try {
            Incentive resp = restTemplate.postForObject(incentiveUrl, tx, Incentive.class);
            if (resp != null && resp.getAmount() != null) {
                return resp.getAmount();
            }
        } catch (RestClientException e) {
            logger.warn("Failed to call incentives API at {}: {}", incentiveUrl, e.getMessage());
        }
        return BigDecimal.ZERO;
    }
}
