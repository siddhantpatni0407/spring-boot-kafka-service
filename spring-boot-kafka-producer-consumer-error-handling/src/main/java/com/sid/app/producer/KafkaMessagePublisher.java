package com.sid.app.producer;

import com.sid.app.config.AppProperties;
import com.sid.app.model.Customer;
import com.sid.app.utils.ApplicationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Message Publisher for sending customer events to Kafka topics.
 * Uses CompletableFuture for asynchronous execution.
 *
 * @author Siddhant Patni
 */
@Service
@Slf4j
public class KafkaMessagePublisher {

    @Autowired
    private AppProperties properties;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes customer events to Kafka asynchronously.
     *
     * @param customer the customer event object
     */
    public void publishEvents(Customer customer) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(properties.getKafkaTopic(), customer);

        future.thenAccept(result -> {
            log.info("publishEvents() : Sent event -> [{}] with offset -> [{}]",
                    ApplicationUtils.getJSONString(customer), result.getRecordMetadata().offset());
        }).exceptionally(ex -> {
            log.error("publishEvents() : Unable to send event -> [{}] due to error -> {}", customer, ex.getMessage());
            return null; // Exceptionally must return something (null is safe)
        });
    }

}