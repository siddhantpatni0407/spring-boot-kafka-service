package com.sid.app.service;

import com.sid.app.model.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Message Publisher Service
 * Handles publishing messages to Kafka topics asynchronously.
 */
@Service
@Slf4j
public class KafkaPublisherService {

    @Autowired
    private KafkaTemplate<String, Object> template;

    /**
     * Sends a message to the specified Kafka topic asynchronously.
     *
     * @param topicName The Kafka topic to send the message to.
     * @param message   The message to send.
     * @return a Mono<Response> indicating success or failure.
     */
    public Mono<Response> sendMessageToTopic(String topicName, String message) {
        Response response = Response.builder().build();

        log.info("sendMessageToTopic() : Sending message to topic '{}' -> {}", topicName, message);

        // KafkaTemplate.send() returns a CompletableFuture in Spring Boot 6+
        CompletableFuture<SendResult<String, Object>> future = template.send(topicName, message);

        return Mono.fromFuture(future.thenApply(result -> {
            log.info("sendMessageToTopic() : Message sent to Topic -> {} with Partition -> {} and Offset ->{}", topicName, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            response.setStatus("Message sent to Topic -> " + topicName + " with Partition -> " + result.getRecordMetadata().partition() + " and Offset -> " + result.getRecordMetadata().offset());
            return response;
        }).exceptionally(exception -> {
            log.error("sendMessageToTopic() : Failed to send message to topic '{}' due to {}", topicName, exception.getMessage(), exception);
            response.setStatus("Unable to send message due to " + exception.getMessage());
            return response;
        }));
    }

}