package com.sid.app.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author Siddhant Patni
 */
@Component
@Data
public class AppProperties {

    @Value("${spring.kafka.consumer.topic}")
    private String kafkaTopic;

    @Value("${spring.kafka.consumer.group-id}")
    private String kafkaGroupId;

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.key-deserializer}")
    private String keyDeserializer;

    @Value("${spring.kafka.consumer.value-deserializer}")
    private String valueDeserializer;

    @Value("${spring.json.trusted.packages}")
    private String jsonTrustedPackage;

}