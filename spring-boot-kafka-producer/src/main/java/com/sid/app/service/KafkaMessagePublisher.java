package com.sid.app.service;

import com.sid.app.config.AppProperties;
import com.sid.app.model.Customer;
import com.sid.app.model.Response;
import com.sid.app.utils.ApplicationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Message Publisher Service
 * Handles publishing messages and events to Kafka topics.
 * Uses CompletableFuture for asynchronous execution.
 */
@Service
@Slf4j
public class KafkaMessagePublisher {

    @Autowired
    private AppProperties properties;

    @Autowired
    private KafkaTemplate<String, Object> template;

    /**
     * Sends a simple message to Kafka asynchronously.
     *
     * @param message the message to send
     * @return a Mono<Response> indicating success or failure
     */
    public Mono<Response> sendMessageToTopic(String message) {
        Response response = Response.builder().build();

        // Spring Boot 6+ KafkaTemplate.send() returns a CompletableFuture
        CompletableFuture<SendResult<String, Object>> future = template.send(properties.getKafkaTopic(), message);

        return Mono.fromFuture(future.thenApply(result -> {
            log.info("Sent message=[{}] with offset=[{}]", message, result.getRecordMetadata().offset());
            response.setStatus("Message sent to Kafka successfully. Offset -> " + result.getRecordMetadata().offset());
            return response;
        }).exceptionally(exception -> {
            log.error("Unable to send message=[{}] due to {}", message, exception.getMessage());
            response.setStatus("Unable to send message due to " + exception.getMessage());
            return response;
        }));
    }

    /**
     * Sends a customer event to Kafka asynchronously.
     *
     * @param customer the customer event data
     * @return a Mono<Response> indicating success or failure
     */
    public Mono<Response> sendEventsToTopic(Customer customer) {
        Response response = Response.builder().build();

        CompletableFuture<SendResult<String, Object>> future = template.send(properties.getKafkaTopic(), customer);

        return Mono.fromFuture(future.thenApply(result -> {
            log.info("Sent event=[{}] with offset=[{}]", ApplicationUtils.getJSONString(customer), result.getRecordMetadata().offset());
            response.setStatus("Event published successfully. Offset -> " + result.getRecordMetadata().offset());
            return response;
        }).exceptionally(exception -> {
            log.error("Unable to send event=[{}] due to {}", customer, exception.getMessage());
            response.setStatus("Unable to send event due to " + exception.getMessage());
            return response;
        }));
    }

}