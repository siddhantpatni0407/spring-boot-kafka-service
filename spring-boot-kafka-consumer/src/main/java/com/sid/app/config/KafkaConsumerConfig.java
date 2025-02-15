package com.sid.app.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * @author Siddhant Patni
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    /*@Autowired
    private AppProperties properties;

    @Bean
    public Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getValueDeserializer());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, properties.getJsonTrustedPackage());
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }*/

}