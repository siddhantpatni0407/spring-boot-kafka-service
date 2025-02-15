package com.sid.app.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Siddhant Patni
 */
@Configuration
public class KafkaProducerConfig {

    @Autowired
    private AppProperties properties;

    @Bean
    public NewTopic createTopic() {
        return new NewTopic(properties.getKafkaTopic(), 5, (short) 1);
    }

}