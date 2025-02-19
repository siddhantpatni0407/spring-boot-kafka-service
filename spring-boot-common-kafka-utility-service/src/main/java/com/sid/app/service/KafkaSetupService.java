package com.sid.app.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.URL;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Service class responsible for setting up Kafka dynamically based on request parameters.
 */
@Getter
@Service
@Slf4j
public class KafkaSetupService {

    private static final String KAFKA_FOLDER_NAME = "kafka_setup";
    private static final String KAFKA_DOWNLOAD_URL = "https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz";
    private static final String KAFKA_ARCHIVE_NAME = "kafka.tgz";
    private static final String EXTRACTED_FOLDER_PREFIX = "kafka_2.13-3.9.0";
    private static final String RENAMED_FOLDER_NAME = "kafka";

    private final Sinks.Many<String> kafkaSetupSink = Sinks.many().multicast().onBackpressureBuffer();

    public ApplicationRunner kafkaSetupRunner(boolean kafkaAutoSetupRequired, boolean kafkaUserDefinedPathRequired, String kafkaUserDefinedPath) {
        return args -> {
            if (!kafkaAutoSetupRequired) {
                log.info("Kafka auto setup is disabled. Skipping setup.");
                kafkaSetupSink.tryEmitNext("Kafka setup skipped.");
                return;
            }

            log.info("=== Kafka Setup Initialization ===");

            String setupBasePath = (kafkaUserDefinedPathRequired && kafkaUserDefinedPath != null && !kafkaUserDefinedPath.isEmpty())
                    ? kafkaUserDefinedPath
                    : System.getProperty("user.home") + File.separator + "Downloads";

            Path kafkaSetupPath = Paths.get(setupBasePath, KAFKA_FOLDER_NAME);
            Path kafkaArchivePath = kafkaSetupPath.resolve(KAFKA_ARCHIVE_NAME);
            Path finalKafkaFolder = kafkaSetupPath.resolve(RENAMED_FOLDER_NAME);

            log.info("Kafka setup directory: {}", kafkaSetupPath);

            try {
                Files.createDirectories(kafkaSetupPath);

                if (!Files.exists(kafkaArchivePath)) {
                    log.info("Kafka archive not found. Downloading...");
                    downloadKafka(kafkaArchivePath.toString());
                }

                if (!Files.exists(finalKafkaFolder)) {
                    log.info("Extracting Kafka archive...");
                    extractKafkaArchive(kafkaArchivePath.toString(), kafkaSetupPath.toString());
                    TimeUnit.SECONDS.sleep(2);
                    renameKafkaFolder(kafkaSetupPath);
                    deleteKafkaTarFile(kafkaSetupPath);
                }
                log.info("=== Kafka Setup Completed Successfully ===");
                kafkaSetupSink.tryEmitNext("Kafka setup completed successfully.");
            } catch (Exception e) {
                log.error("Error during Kafka setup: {}", e.getMessage(), e);
                kafkaSetupSink.tryEmitNext("Error during Kafka setup: " + e.getMessage());
            }
        };
    }

    private void downloadKafka(String filePath) {
        try {
            log.info("Downloading Kafka from {} to {}", KAFKA_DOWNLOAD_URL, filePath);
            FileUtils.copyURLToFile(new URL(KAFKA_DOWNLOAD_URL), new File(filePath));
            log.info("Kafka downloaded successfully.");
        } catch (IOException e) {
            log.error("Failed to download Kafka: {}", e.getMessage(), e);
        }
    }

    private void extractKafkaArchive(String archivePath, String destinationDir) {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(archivePath));
             FileOutputStream fileOutputStream = new FileOutputStream(destinationDir + File.separator + "kafka.tar")) {

            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
            extractTarFile(destinationDir + File.separator + "kafka.tar", destinationDir);
        } catch (IOException e) {
            log.error("Failed to extract Kafka archive: {}", e.getMessage(), e);
        }
    }

    private void extractTarFile(String tarFilePath, String destinationDir) {
        try (TarArchiveInputStream tis = new TarArchiveInputStream(new BufferedInputStream(new FileInputStream(tarFilePath)))) {
            TarArchiveEntry entry;
            while ((entry = tis.getNextTarEntry()) != null) {
                File outputFile = new File(destinationDir, entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(outputFile.toPath());
                } else {
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile))) {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = tis.read(buffer)) != -1) {
                            bos.write(buffer, 0, bytesRead);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Failed to extract Kafka TAR file: {}", e.getMessage(), e);
        }
    }

    private void renameKafkaFolder(Path kafkaSetupPath) {
        try (Stream<Path> paths = Files.list(kafkaSetupPath)) {
            paths.filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().startsWith(EXTRACTED_FOLDER_PREFIX))
                    .findFirst()
                    .ifPresent(extractedFolder -> {
                        Path renamedPath = kafkaSetupPath.resolve(RENAMED_FOLDER_NAME);
                        try {
                            Files.move(extractedFolder, renamedPath, StandardCopyOption.REPLACE_EXISTING);
                            log.info("Successfully renamed Kafka folder to {}", renamedPath);
                        } catch (IOException e) {
                            log.error("Failed to rename Kafka folder: {}", e.getMessage(), e);
                        }
                    });
        } catch (IOException e) {
            log.error("Error renaming Kafka folder: {}", e.getMessage(), e);
        }
    }

    private void deleteKafkaTarFile(Path kafkaSetupPath) {
        Path kafkaTarPath = kafkaSetupPath.resolve("kafka.tar");
        try {
            Files.deleteIfExists(kafkaTarPath);
            log.info("Deleted temporary kafka.tar file.");
        } catch (IOException e) {
            log.error("Failed to delete kafka.tar: {}", e.getMessage(), e);
        }
    }

}