package com.sid.app.controller;

import com.sid.app.constant.AppConstants;
import com.sid.app.model.Customer;
import com.sid.app.model.Response;
import com.sid.app.producer.KafkaMessagePublisher;
import com.sid.app.utils.ApplicationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * @author Siddhant Patni
 */
@RestController
@Slf4j
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    @PostMapping(value = AppConstants.KAFKA_PUBLISH_MESSAGE_WITH_ERROR_HANDLING__ENDPOINT)
    public Mono<ResponseEntity<Response>> publishEvents() {
        if (log.isInfoEnabled()) {
            log.info("publishEvents() : Publish Event - START");
        }
        try {
            Response response = Response.builder().build();
            List<Customer> customers = ApplicationUtils.readDataFromCsv();
            assert customers != null;
            customers.forEach(customer -> publisher.publishEvents(customer));
            response.setStatus("Message published successfully");
            if (log.isInfoEnabled()) {
                log.info("publishEvents() : Publish Event - END");
            }
            return Mono.just(ResponseEntity.ok(response));
        } catch (Exception ex) {
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build());
        }
    }

}