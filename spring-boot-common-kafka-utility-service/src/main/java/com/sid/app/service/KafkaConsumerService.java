package com.sid.app.service;

import com.sid.app.config.AppProperties;
import com.sid.app.model.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;

/**
 * @author Siddhant Patni
 * Service for consuming Kafka messages with various options.
 */
@Service
@Slf4j
public class KafkaConsumerService {

    @Autowired
    private AppProperties appProperties;

    /**
     * Consumes messages from the specified Kafka topic.
     *
     * @param topicName the name of the Kafka topic
     * @return a Mono containing the response status
     */
    public Mono<Response> consumeMessages(String topicName) {
        return executeKafkaConsumerCommand(topicName, "--from-beginning");
    }

    /**
     * Consumes the latest message from the specified Kafka topic.
     *
     * @param topicName the name of the Kafka topic
     * @return a Mono containing the response status
     */
    public Mono<Response> consumeLatestMessage(String topicName) {
        return executeKafkaConsumerCommand(topicName, "--max-messages 1");
    }

    /**
     * Consumes messages from the specified Kafka topic with additional options.
     *
     * @param topicName     the name of the Kafka topic
     * @param format        the message format (e.g., "plain" or "json")
     * @param maxMessages   the maximum number of messages to consume
     * @param fromBeginning whether to consume from the beginning
     * @param partition     the partition number (optional)
     * @param group         the consumer group (optional)
     * @param timeoutMs     the timeout in milliseconds
     * @return a Mono containing the response status
     */
    public Mono<Response> consumeMessagesWithOptions(String topicName, String format, int maxMessages,
                                                     boolean fromBeginning, Integer partition, String group,
                                                     long timeoutMs) {
        StringBuilder commandBuilder = new StringBuilder()
                .append(appProperties.getKafkaInstallationDirectory())
                .append("\\kafka-console-consumer.bat")
                .append(" --bootstrap-server ").append(appProperties.getBootstrapServers())
                .append(" --topic ").append(topicName);

        if ("json".equalsIgnoreCase(format)) {
            commandBuilder.append(" --property print.value=true");
        } else if ("plain".equalsIgnoreCase(format)) {
            commandBuilder.append(" --max-messages ").append(maxMessages);
        }

        if (fromBeginning) {
            commandBuilder.append(" --from-beginning");
        }

        if (partition != null) {
            commandBuilder.append(" --partition ").append(partition);
        }

        if (group != null && !group.isEmpty()) {
            commandBuilder.append(" --group ").append(group);
        }

        commandBuilder.append(" --timeout-ms ").append(timeoutMs);

        return executeKafkaConsumerCommand(topicName, commandBuilder.toString());
    }

    /**
     * Executes the Kafka consumer command with the specified options.
     *
     * @param topicName         the name of the Kafka topic
     * @param additionalOptions additional command-line options
     * @return a Mono containing the response status
     */
    private Mono<Response> executeKafkaConsumerCommand(String topicName, String additionalOptions) {
        Response response = Response.builder().build();
        String command = buildKafkaConsumerCommand(topicName, additionalOptions);

        try {
            ProcessBuilder consumerProcessBuilder = new ProcessBuilder(command.split(" "));
            Process consumerProcess = consumerProcessBuilder.start();

            if (consumerProcess.isAlive()) {
                log.info("Kafka consumer started successfully for topic '{}'", topicName);
                response.setStatus("Kafka consumer started successfully for topic: " + topicName);
            } else {
                log.error("Failed to start Kafka consumer for topic '{}'", topicName);
                response.setErrorMessage("Failed to start Kafka consumer.");
            }
        } catch (IOException e) {
            log.error("Failed to start consumer for topic '{}' due to {}", topicName, e.getMessage(), e);
            response.setErrorMessage("Unable to start consumer: " + e.getMessage());
        }

        return Mono.just(response);
    }

    /**
     * Builds the Kafka consumer command with the specified options.
     *
     * @param topicName         the name of the Kafka topic
     * @param additionalOptions additional command-line options
     * @return the complete Kafka consumer command
     */
    private String buildKafkaConsumerCommand(String topicName, String additionalOptions) {
        return "cmd.exe /c start cmd.exe /k call " +
                appProperties.getKafkaInstallationDirectory() +
                "\\kafka-console-consumer.bat" +
                " --bootstrap-server " + appProperties.getBootstrapServers() +
                " --topic " + topicName +
                " " + additionalOptions;
    }

}