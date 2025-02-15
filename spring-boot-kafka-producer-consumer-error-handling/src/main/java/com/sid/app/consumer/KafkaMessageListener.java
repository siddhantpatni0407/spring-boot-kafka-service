package com.sid.app.consumer;

import com.sid.app.config.AppProperties;
import com.sid.app.model.Customer;
import com.sid.app.utils.ApplicationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Siddhant Patni
 */
@Service
@Slf4j
public class KafkaMessageListener {

    @Autowired
    private AppProperties properties;

    // Define your Kafka listener method
    @RetryableTopic(attempts = "4")// 3 topic N-1
    //@RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000))      // add delay in retry
    //@RetryableTopic(attempts = "4", exclude = {NullPointerException.class, RuntimeException.class})            // exclude exception from retry
    @KafkaListener(topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvents(Customer customer, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        if (log.isInfoEnabled()) {
            log.info("consumeEvents() : Received => Topic -> {}, Offset -> {}, Message -> {}",
                    topic, offset, ApplicationUtils.getJSONString(customer));
        }
        List<String> restrictedCountryList = Stream.of("China", "Ukraine", "Russia").collect(Collectors.toList());
        if (restrictedCountryList.contains(customer.getCountry())) {
            throw new RuntimeException("Invalid customer received !!!");
        }
    }

    @DltHandler
    public void listenDLT(Customer customer, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        if (log.isInfoEnabled()) {
            log.info("listenDLT() : DLT Received => Topic -> {}, Offset -> {}, Message -> {}",
                    topic, offset, ApplicationUtils.getJSONString(customer));
        }
    }

}