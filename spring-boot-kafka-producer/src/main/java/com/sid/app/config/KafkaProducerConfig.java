package com.sid.app.config;

import org.springframework.context.annotation.Configuration;

/**
 * @author Siddhant Patni
 */
@Configuration
public class KafkaProducerConfig {

    /*@Autowired
    private AppProperties properties;

    @Bean
    public NewTopic createTopic() {
        return new NewTopic(properties.getKafkaTopic(), 3, (short) 1);
    }

    @Bean
    public Map<String, Object> producerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getValueSerializer());
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }*/

}