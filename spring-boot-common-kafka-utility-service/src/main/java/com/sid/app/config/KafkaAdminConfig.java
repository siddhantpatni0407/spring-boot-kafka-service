package com.sid.app.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaAdminConfig {

    private final AppProperties appProperties; // Injected application properties

    public KafkaAdminConfig(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    @Bean
    public AdminClient adminClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBootstrapServers());
        return AdminClient.create(properties);
    }

}