package com.sid.app.service;

import com.sid.app.config.AppProperties;
import com.sid.app.model.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Siddhant Patni
 */
@Service
@Slf4j
@SuppressWarnings("PMD")
public class KafkaService {

    @Autowired
    private AppProperties appProperties;

    public Mono<ResponseEntity<Response>> createTopic(String topicName, Integer partition) {
        return Mono.fromCallable(() -> {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBootstrapServers());

            try (AdminClient adminClient = AdminClient.create(properties)) {
                NewTopic newTopic;
                if (partition == null || partition.equals(0)) {
                    newTopic = new NewTopic(topicName, 1, (short) 1);
                } else {
                    newTopic = new NewTopic(topicName, partition, (short) 1);
                }
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                return ResponseEntity.ok(new Response("Topic -[" + topicName + "]  created successfully", null));
            } catch (InterruptedException | ExecutionException e) {
                if (log.isErrorEnabled()) {
                    log.error("Exception occurred : {}", e.getMessage());
                }
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(new Response("Failed to create topic", e.getMessage()));
            }
        });
    }

    public Mono<ResponseEntity<Response>> deleteTopic(String topicName) {
        return Mono.fromCallable(() -> {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBootstrapServers());

            try (AdminClient adminClient = AdminClient.create(properties)) {
                DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
                deleteTopicsResult.all().get();
                return ResponseEntity.ok(new Response("Topic " + topicName + " deleted successfully", null));
            } catch (InterruptedException | ExecutionException e) {
                if (log.isErrorEnabled()) {
                    log.error("Exception occurred : {}", e.getMessage());
                }
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(new Response("Failed to delete topic", e.getMessage()));
            }
        }).retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(5))
                .filter(throwable -> throwable instanceof IOException)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    if (log.isErrorEnabled()) {
                        log.error("Retry attempts exhausted: {}", retrySignal.totalRetriesInARow());
                    }
                    return new RuntimeException("Retry attempts exhausted: " + retrySignal.totalRetriesInARow());
                }));
    }

    public Mono<ResponseEntity<Collection<String>>> getAllTopics() {
        return Mono.fromCallable(() -> {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBootstrapServers());

            try (AdminClient adminClient = AdminClient.create(properties)) {
                ListTopicsResult topicsResult = adminClient.listTopics();
                Set<String> topics = topicsResult.names().get();
                return ResponseEntity.ok(new ArrayList<>(topics));
            } catch (InterruptedException | ExecutionException e) {
                if (log.isErrorEnabled()) {
                    log.error("Exception occurred : {}", e.getMessage());
                }
                // Return an empty collection in case of error
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(new ArrayList<>());
            }
        });
    }

    public Mono<ResponseEntity<Response>> startServers() {
        Response response = Response.builder().build();
        try {
            ProcessBuilder zookeeperProcessBuilder = new ProcessBuilder();
            zookeeperProcessBuilder.command("cmd.exe", "/c", "start", "cmd.exe", "/k", "call",
                    appProperties.getKafkaInstallationDirectory() + "\\zookeeper-server-start.bat",
                    appProperties.getZookeeperConfigPath());
            Process zookeeperProcess = zookeeperProcessBuilder.start();

            ProcessBuilder kafkaProcessBuilder = new ProcessBuilder();
            kafkaProcessBuilder.command("cmd.exe", "/c", "start", "cmd.exe", "/k", "call",
                    appProperties.getKafkaInstallationDirectory() + "\\kafka-server-start.bat",
                    appProperties.getKafkaConfigPath());
            Process kafkaProcess = kafkaProcessBuilder.start();

            // Check if both processes started successfully
            if (zookeeperProcess.isAlive() && kafkaProcess.isAlive()) {
                response.setStatus("Zookeeper and Kafka servers started successfully.");
            } else {
                response.setErrorMessage("Failed to start Zookeeper or Kafka server.");
            }
            return Mono.just(ResponseEntity.ok(response));
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("startServers() : Exception occurred : {}", e.getMessage());
            }
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new Response("Failed to start the zookeeper and kafka server with exception : ", e.getMessage())));
        }
    }

    public Mono<ResponseEntity<Response>> stopKafkaServers() {
        Response response = Response.builder().build();
        try {
            ProcessBuilder zookeeperProcessBuilder = new ProcessBuilder();
            zookeeperProcessBuilder.command("cmd.exe", "/c", "start", "cmd.exe", "/k", "call",
                    appProperties.getKafkaInstallationDirectory() + "\\zookeeper-server-stop.bat");
            Process zookeeperProcess = zookeeperProcessBuilder.start();

            ProcessBuilder kafkaProcessBuilder = new ProcessBuilder();
            kafkaProcessBuilder.command("cmd.exe", "/c", "start", "cmd.exe", "/k", "call",
                    appProperties.getKafkaInstallationDirectory() + "\\kafka-server-stop.bat");
            Process kafkaProcess = kafkaProcessBuilder.start();

            // Check if both processes started successfully
            if (zookeeperProcess.isAlive() && kafkaProcess.isAlive()) {
                response.setStatus("Zookeeper and Kafka servers stopped successfully.");
            } else {
                response.setErrorMessage("Failed to stop Zookeeper or Kafka server.");
            }
            return Mono.just(ResponseEntity.ok(response));
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("stopKafkaServers() : Exception occurred : {}", e.getMessage());
            }
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new Response("Failed to stop the zookeeper and kafka server with exception : ", e.getMessage())));
        }
    }

    public Mono<ResponseEntity<Response>> deleteKafkaLogs() {
        return Mono.fromCallable(() -> {
            try {
                // Specify the paths to Kafka and Zookeeper logs
                String kafkaLogsPath = appProperties.getKafkaLogsPath();
                String zookeeperLogsPath = appProperties.getZookeeperLogsPath();

                // Delete Kafka logs
                deleteDirectory(new File(kafkaLogsPath));
                // Delete Zookeeper logs
                deleteDirectory(new File(zookeeperLogsPath));
                return ResponseEntity.ok(new Response("Kafka and Zookeeper logs deleted successfully", null));
            } catch (Exception e) {
                // Handle any exceptions that occur during the log deletion
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(new Response("Failed to delete Kafka and Zookeeper logs", e.getMessage()));
            }
        });
    }

    // Helper method to delete directory recursively
    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    public Mono<TopicDescription> getTopicDescription(String topicName) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBootstrapServers());

        // Create the AdminClient and don't close it until we're done with it
        AdminClient adminClient = AdminClient.create(properties);
        try {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(
                    Collections.singletonList(topicName), new DescribeTopicsOptions().timeoutMs(5000));
            KafkaFuture<TopicDescription> topicDescriptionFuture = describeTopicsResult.values().get(topicName);

            // Wrap KafkaFuture in a CompletableFuture
            CompletableFuture<TopicDescription> completableFuture = new CompletableFuture<>();

            topicDescriptionFuture.whenComplete((result, exception) -> {
                if (exception != null) {
                    completableFuture.completeExceptionally(exception);
                } else {
                    completableFuture.complete(result);
                }
            });

            // Use Mono.fromFuture to wrap the CompletableFuture
            return Mono.fromFuture(() -> completableFuture)
                    .doFinally(signal -> {
                        // Ensure we close the AdminClient when done
                        adminClient.close();
                    });
        } catch (Exception e) {
            // Ensure we close the AdminClient on error too
            adminClient.close();
            return Mono.error(e);
        }
    }


}