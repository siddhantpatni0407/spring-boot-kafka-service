package com.sid.app.constant;

/**
 * @author Siddhant Patni
 * Utility class containing constants used for defining endpoints.
 */
public class AppConstants {

    /**
     * Constant for the Kafka publish message endpoint.
     */
    public static final String START_KAFKA_SERVERS_ENDPOINT = "/api/v1/kafka-service/kafka/start-server";
    public static final String STOP_KAFKA_SERVERS_ENDPOINT = "/api/v1/kafka-service/kafka/stop-server";
    public static final String KAFKA_CREATE_TOPIC_ENDPOINT = "/api/v1/kafka-service/kafka/create-topic";
    public static final String KAFKA_TOPIC_ENDPOINT = "/api/v1/kafka-service/kafka/topic";
    public static final String KAFKA_TOPIC_DETAILS_ENDPOINT = "/api/v1/kafka-service/kafka/topic/details";
    public static final String DELETE_KAFKA_LOGS_ENDPOINT = "/api/v1/kafka-service/kafka/logs";

}