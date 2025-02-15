package com.sid.app.consumer;

import com.sid.app.config.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @author Siddhant Patni
 */
@Component
@Slf4j
public class KafkaMessageListener {

    @Autowired
    private AppProperties properties;

    // Define your Kafka listener method
    @KafkaListener(topics = "${spring.kafka.consumer.topic}", concurrency = "${spring.kafka.consumer.concurrency}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMessageOrEvents(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {
        // Process the received message
        processMessage(message);

        // Acknowledge the message
        acknowledgment.acknowledge();
    }

    private void processMessage(ConsumerRecord<String, String> message) {
        if (log.isInfoEnabled()) {
            log.info("processMessage() : Received => Topic -> {}, Partition -> {}, Offset -> {}, Message -> {}",
                    message.topic(), message.partition(), message.offset(), message.value());
        }
    }

}