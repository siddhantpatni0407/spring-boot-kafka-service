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
                    "--bootstrap-server", appProperties.getBootstrapServers(),
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

    public Mono<Response> consumeLatestMessage(String topicName) {
        Response response = Response.builder().build();

        try {
            ProcessBuilder consumerProcessBuilder = new ProcessBuilder();
            consumerProcessBuilder.command(
                    "cmd.exe", "/c", "start", "cmd.exe", "/k", "call",
                    appProperties.getKafkaInstallationDirectory() + "\\kafka-console-consumer.bat",
                    "--bootstrap-server", appProperties.getBootstrapServers(),
                    "--topic", topicName,
                    "--max-messages", "1" // This flag limits the consumer to consume only one message
            );

            Process consumerProcess = consumerProcessBuilder.start();

            // Check if the process started successfully
            if (consumerProcess.isAlive()) {
                log.info("consumeLatestMessage() : Kafka consumer started successfully for topic '{}'", topicName);
                response.setStatus("Kafka consumer started successfully for topic: " + topicName);
            } else {
                log.error("consumeLatestMessage() : Failed to start Kafka consumer for topic '{}'", topicName);
                response.setErrorMessage("Failed to start Kafka consumer.");
            }
        } catch (IOException e) {
            log.error("consumeLatestMessage() : Failed to start consumer for topic '{}' due to {}", topicName, e.getMessage(), e);
            response.setErrorMessage("Unable to start consumer: " + e.getMessage());
        }

        return Mono.just(response);
    }

    public Mono<Response> consumeMessagesWithOptions(String topicName, String format, int maxMessages, boolean fromBeginning,
                                                     Integer partition, String group, long timeoutMs) {
        Response response = Response.builder().build();
        // Build the Kafka consumer command dynamically
        String command = buildKafkaConsumerCommand(topicName, format, maxMessages, fromBeginning, partition, group, timeoutMs);

        try {
            // Execute the Kafka consumer process
            ProcessBuilder consumerProcessBuilder = new ProcessBuilder(command.split(" "));
            Process consumerProcess = consumerProcessBuilder.start();

            // Check if the process started successfully
            if (consumerProcess.isAlive()) {
                response.setStatus("Kafka consumer started successfully for topic: " + topicName);
            } else {
                response.setErrorMessage("Failed to start Kafka consumer.");
            }

        } catch (IOException e) {
            // Log and return the error response
            response.setErrorMessage("Unable to start consumer: " + e.getMessage());
        }
        return Mono.just(response);
    }

    // Helper method to dynamically build Kafka consumer command
    private String buildKafkaConsumerCommand(String topicName, String format, int maxMessages, boolean fromBeginning,
                                             Integer partition, String group, long timeoutMs) {
        String baseCommand = appProperties.getKafkaInstallationDirectory() + "\\kafka-console-consumer.bat";
        String bootstrapServers = appProperties.getBootstrapServers();

        // Start building the base command
        StringBuilder command = new StringBuilder()
                .append("cmd.exe /c start cmd.exe /k call ")
                .append(baseCommand)
                .append(" --bootstrap-server ").append(bootstrapServers)
                .append(" --topic ").append(topicName);

        // Add the format flag
        if ("json".equalsIgnoreCase(format)) {
            command.append(" --property print.value=true");  // Print message in JSON format
        } else if ("plain".equalsIgnoreCase(format)) {
            command.append(" --max-messages ").append(maxMessages);  // Limit the number of messages
        }

        // Handle the 'fromBeginning' flag
        if (fromBeginning) {
            command.append(" --from-beginning");  // Consume from the beginning of the topic
        }

        // Handle the 'partition' flag (optional)
        if (partition != null) {
            command.append(" --partition ").append(partition);  // Specify the partition number
        }

        // Handle the 'group' flag (optional)
        if (group != null && !group.isEmpty()) {
            command.append(" --group ").append(group);  // Specify the consumer group
        }

        // Handle the 'timeoutMs' flag (optional)
        command.append(" --timeout-ms ").append(timeoutMs);  // Timeout in milliseconds

        return command.toString();
    }

}