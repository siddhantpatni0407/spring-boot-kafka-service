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

    @Value("${spring.kafka.producer.topic}")
    private String kafkaTopic;

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.json.trusted.packages}")
    private String jsonTrustedPackage;

}