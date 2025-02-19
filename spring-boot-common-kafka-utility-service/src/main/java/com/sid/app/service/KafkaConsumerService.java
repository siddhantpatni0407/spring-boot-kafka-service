package com.sid.app.service;

import com.sid.app.config.AppProperties;
import com.sid.app.model.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Siddhant Patni
 */
@Service
@Slf4j
public class KafkaConsumerService {

    @Autowired
    private AppProperties appProperties;

    public Mono<Response> consumeMessages(String topicName) {
        Response response = Response.builder().build();

        try {
            ProcessBuilder consumerProcessBuilder = new ProcessBuilder();
            consumerProcessBuilder.command(
                    "cmd.exe", "/c", "start", "cmd.exe", "/k", "call",
                    appProperties.getKafkaInstallationDirectory() + "\\kafka-console-consumer.bat",
                    "--bootstrap-server", "localhost:9092",
                    "--topic", topicName,
                    "--from-beginning"
            );

            Process consumerProcess = consumerProcessBuilder.start();

            // Check if the process started successfully
            if (consumerProcess.isAlive()) {
                log.info("consumeMessages() : Kafka consumer started successfully for topic '{}'", topicName);
                response.setStatus("Kafka consumer started successfully for topic: " + topicName);
            } else {
                log.error("consumeMessages() : Failed to start Kafka consumer for topic '{}'", topicName);
                response.setErrorMessage("Failed to start Kafka consumer.");
            }
        } catch (IOException e) {
            log.error("consumeMessages() : Failed to start consumer for topic '{}' due to {}", topicName, e.getMessage(), e);
            response.setErrorMessage("Unable to start consumer: " + e.getMessage());
        }

        return Mono.just(response);
    }

}