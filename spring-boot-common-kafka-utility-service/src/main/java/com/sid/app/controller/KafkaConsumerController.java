package com.sid.app.controller;

import com.sid.app.constant.AppConstants;
import com.sid.app.model.Response;
import com.sid.app.service.KafkaConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * @author Siddhant Patni
 */
@RestController
@Slf4j
@CrossOrigin
public class KafkaConsumerController {

    @Autowired
    private KafkaConsumerService kafkaConsumerService;

    @GetMapping(value = AppConstants.KAFKA_CONSUME_MESSAGE_ENDPOINT, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Response>> consumeMessages(@RequestParam(value = "topicName") String topicName) {
        log.info("consumeMessages() : Starting Kafka consumer for topic '{}' - START", topicName);

        try {
            return kafkaConsumerService.consumeMessages(topicName)
                    .flatMap(response -> {
                        log.info("consumeMessages() : Messages consumed successfully from topic '{}'", topicName);
                        return Mono.just(ResponseEntity.ok(response));
                    });
        } catch (Exception ex) {
            log.error("consumeMessages() : Error consuming messages from topic '{}': {}", topicName, ex.getMessage(), ex);
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Response.builder().status("Error consuming messages: " + ex.getMessage()).build()));
        }
    }

}