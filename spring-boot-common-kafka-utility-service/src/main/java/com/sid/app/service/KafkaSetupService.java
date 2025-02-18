package com.sid.app.service;

import com.sid.app.config.AppProperties;
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
 * Service to dynamically set up Kafka upon application startup based on system properties.
 */
@Configuration
@Slf4j
public class KafkaSetupService {

    private static final String KAFKA_FOLDER_NAME = "kafka_setup";
    private static final String KAFKA_DOWNLOAD_URL = "https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz";
    private static final String KAFKA_ARCHIVE_NAME = "kafka.tgz";
    private static final String EXTRACTED_FOLDER_PREFIX = "kafka_2.13-3.9.0";
    private static final String RENAMED_FOLDER_NAME = "kafka";

    private final AppProperties appProperties;

    public KafkaSetupService(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

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
            File kafkaSetupFolder = kafkaSetupPath.toFile();
            Path kafkaArchivePath = kafkaSetupPath.resolve(KAFKA_ARCHIVE_NAME);
            Path finalKafkaFolder = kafkaSetupPath.resolve(RENAMED_FOLDER_NAME);

            log.info("Kafka setup directory: {}", kafkaSetupPath);

            // Step 1: Create setup folder if it does not exist
            if (!Files.exists(kafkaSetupPath)) {
                log.info("Kafka setup folder not found. Creating directory...");
                Files.createDirectories(kafkaSetupPath);
            } else {
                log.info("Kafka setup folder already exists. Skipping directory creation.");
            }

            // Step 2: Check if Kafka archive exists, if not, download it
            if (!Files.exists(kafkaArchivePath)) {
                log.info("Kafka archive not found. Preparing to download...");
                downloadKafka(kafkaArchivePath.toString());
            } else {
                log.info("Kafka archive already exists at {}. Skipping download.", kafkaArchivePath);
            }

            // Step 3: Extract Kafka archive if not already extracted
            if (!Files.exists(finalKafkaFolder)) {
                log.info("Extracting Kafka archive...");
                extractKafkaArchive(kafkaArchivePath.toString(), kafkaSetupPath.toString());

                // Ensure all streams are closed before renaming
                TimeUnit.SECONDS.sleep(2);
                renameKafkaFolder(kafkaSetupPath);

                // Step 4: Delete the `kafka.tar` file after extraction
                deleteKafkaTarFile(kafkaSetupPath);
            } else {
                log.info("Kafka is already extracted and renamed. Skipping extraction.");
            }

            log.info("=== Kafka Setup Completed ===");
        };
    }

    private void downloadKafka(String filePath) {
        try {
            log.info("Downloading Kafka from {} to {}", KAFKA_DOWNLOAD_URL, filePath);
            long fileSize = getFileSize(KAFKA_DOWNLOAD_URL);
            log.info("File size before download: {} MB", fileSize / (1024 * 1024));

            long startTime = System.currentTimeMillis();
            FileUtils.copyURLToFile(new URL(KAFKA_DOWNLOAD_URL), new File(filePath));
            long endTime = System.currentTimeMillis();

            log.info("Kafka downloaded successfully at {}", filePath);
            log.info("Download completed in {} seconds.", (endTime - startTime) / 1000.0);
        } catch (IOException e) {
            log.error("Failed to download Kafka: {}", e.getMessage(), e);
        }
    }

    private void extractKafkaArchive(String archivePath, String destinationDir) {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(archivePath));
             FileOutputStream fileOutputStream = new FileOutputStream(destinationDir + File.separator + "kafka.tar")) {

            log.info("Extracting Kafka archive...");
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }

            extractTarFile(destinationDir + File.separator + "kafka.tar", destinationDir);

            log.info("Kafka extraction completed.");
        } catch (IOException e) {
            log.error("Failed to extract Kafka archive: {}", e.getMessage(), e);
        }
    }

    private void extractTarFile(String tarFilePath, String destinationDir) {
        try (FileInputStream fis = new FileInputStream(tarFilePath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             TarArchiveInputStream tis = new TarArchiveInputStream(bis)) {

            TarArchiveEntry entry;
            while ((entry = tis.getNextTarEntry()) != null) {
                File outputFile = new File(destinationDir, entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(outputFile.toPath());
                } else {
                    try (FileOutputStream fos = new FileOutputStream(outputFile);
                         BufferedOutputStream bos = new BufferedOutputStream(fos)) {
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
                        int attempts = 0;
                        boolean renamed = false;

                        while (attempts < 3 && !renamed) {
                            try {
                                log.info("Attempting to rename '{}' to '{}'", extractedFolder, renamedPath);
                                Files.move(extractedFolder, renamedPath, StandardCopyOption.REPLACE_EXISTING);
                                log.info("Successfully renamed '{}' to '{}'", extractedFolder, renamedPath);
                                renamed = true;
                            } catch (IOException e) {
                                attempts++;
                                log.warn("Rename attempt {} failed: {}. Retrying in 2 seconds...", attempts, e.getMessage());
                                try {
                                    Thread.sleep(3000);
                                } catch (InterruptedException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }
                        }

                        if (!renamed) {
                            log.error("Failed to rename Kafka folder after multiple attempts.");
                        }
                    });
        } catch (IOException e) {
            log.error("Error renaming Kafka folder: {}", e.getMessage(), e);
        }
    }

    private void deleteKafkaTarFile(Path kafkaSetupPath) {
        Path kafkaTarPath = kafkaSetupPath.resolve("kafka.tar");
        try {
            if (Files.exists(kafkaTarPath)) {
                Files.delete(kafkaTarPath);
                log.info("Deleted temporary kafka.tar file.");
            }
        } catch (IOException e) {
            log.error("Failed to delete kafka.tar: {}", e.getMessage(), e);
        }
    }

    private long getFileSize(String fileUrl) throws IOException {
        URL url = new URL(fileUrl);
        return url.openConnection().getContentLengthLong();
    }

}