package com.sid.app.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Configuration class responsible for setting up Kafka dynamically upon application startup.
 * Downloads, extracts, and configures Kafka based on system properties.
 */
@Configuration
@Slf4j
public class KafkaSetupConfig {

    private static final String KAFKA_FOLDER_NAME = "kafka_setup";
    private static final String KAFKA_DOWNLOAD_URL = "https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz";
    private static final String KAFKA_ARCHIVE_NAME = "kafka.tgz";
    private static final String EXTRACTED_FOLDER_PREFIX = "kafka_2.13-3.9.0";
    private static final String RENAMED_FOLDER_NAME = "kafka";

    private final AppProperties appProperties;

    public KafkaSetupConfig(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    /**
     * Application runner that sets up Kafka if required.
     *
     * @return ApplicationRunner instance
     */
    @Bean
    public ApplicationRunner kafkaSetupRunner() {
        return args -> {
            if (!appProperties.isKafkaAutoSetupRequired()) {
                log.info("Kafka auto setup is disabled. Skipping Kafka setup.");
                return;
            }

            log.info("=== Kafka Setup Initialization ===");

            // Determine setup path based on user configuration
            String setupBasePath = appProperties.isKafkaUserDefinedPathRequired()
                    ? appProperties.getKafkaUserDefinedPath()
                    : System.getProperty("user.home") + File.separator + "Downloads";

            Path kafkaSetupPath = Paths.get(setupBasePath, KAFKA_FOLDER_NAME);
            Path kafkaArchivePath = kafkaSetupPath.resolve(KAFKA_ARCHIVE_NAME);
            Path finalKafkaFolder = kafkaSetupPath.resolve(RENAMED_FOLDER_NAME);

            log.info("Kafka setup directory: {}", kafkaSetupPath);

            // Step 1: Create setup folder if it does not exist
            Files.createDirectories(kafkaSetupPath);

            // Step 2: Download Kafka if archive does not exist
            if (!Files.exists(kafkaArchivePath)) {
                log.info("Kafka archive not found. Downloading...");
                downloadKafka(kafkaArchivePath.toString());
            }

            // Step 3: Extract Kafka archive if not already extracted
            if (!Files.exists(finalKafkaFolder)) {
                log.info("Extracting Kafka archive...");
                extractKafkaArchive(kafkaArchivePath.toString(), kafkaSetupPath.toString());
                TimeUnit.SECONDS.sleep(2);
                renameKafkaFolder(kafkaSetupPath);
                deleteKafkaTarFile(kafkaSetupPath);
            }

            log.info("=== Kafka Setup Completed ===");
        };
    }

    /**
     * Downloads Kafka from the specified URL.
     *
     * @param filePath Path to save the downloaded file.
     */
    private void downloadKafka(String filePath) {
        try {
            log.info("Downloading Kafka from {} to {}", KAFKA_DOWNLOAD_URL, filePath);
            FileUtils.copyURLToFile(new URL(KAFKA_DOWNLOAD_URL), new File(filePath));
            log.info("Kafka downloaded successfully at {}", filePath);
        } catch (IOException e) {
            log.error("Failed to download Kafka: {}", e.getMessage(), e);
        }
    }

    /**
     * Extracts the Kafka archive.
     *
     * @param archivePath    Path of the compressed archive.
     * @param destinationDir Destination directory for extraction.
     */
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

    /**
     * Extracts a TAR file.
     *
     * @param tarFilePath    Path to the TAR file.
     * @param destinationDir Directory to extract the contents.
     */
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

    /**
     * Renames the extracted Kafka folder.
     *
     * @param kafkaSetupPath Base setup path containing extracted files.
     */
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

    /**
     * Deletes the temporary Kafka TAR file after extraction.
     *
     * @param kafkaSetupPath Path containing the TAR file.
     */
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