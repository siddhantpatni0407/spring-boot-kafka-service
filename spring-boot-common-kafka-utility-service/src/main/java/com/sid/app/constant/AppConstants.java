package com.sid.app.constant;

/**
 * @author Siddhant Patni
 * Utility class containing constants used for defining endpoints.
 */
public class AppConstants {

    /**
     * Constant for the Kafka publish message endpoint.
     */
    public static final String START_KAFKA_SETUP_ENDPOINT = "/api/v1/kafka-service/kafka/setup";
    public static final String START_KAFKA_SERVERS_ENDPOINT = "/api/v1/kafka-service/kafka/start-server";
    public static final String STOP_KAFKA_SERVERS_ENDPOINT = "/api/v1/kafka-service/kafka/stop-server";
    public static final String KAFKA_CREATE_TOPIC_ENDPOINT = "/api/v1/kafka-service/kafka/create-topic";
    public static final String KAFKA_TOPIC_ENDPOINT = "/api/v1/kafka-service/kafka/topic";
    public static final String KAFKA_TOPIC_DETAILS_ENDPOINT = "/api/v1/kafka-service/kafka/topic/details";
    public static final String DELETE_KAFKA_LOGS_ENDPOINT = "/api/v1/kafka-service/kafka/logs";
    public static final String KAFKA_PUBLISH_MESSAGE_ENDPOINT = "/api/v1/kafka-service/kafka/publish";
    public static final String KAFKA_CONSUME_MESSAGE_ENDPOINT = "/api/v1/kafka-service/kafka/consume";
    public static final String KAFKA_CONSUME_MESSAGE_WITH_OPTIONS_ENDPOINT = "/api/v1/kafka-service/kafka/consume/options";
    public static final String KAFKA_CONSUME_LATEST_MESSAGE_ENDPOINT = "/api/v1/kafka-service/kafka/consume/latest-message";
    public static final String START_KAFKA_HEALTH_CHECK_ENDPOINT = "/api/v1/kafka-service/kafka/health";

    public static final String HEALTH_CHECK_TOPIC = "kafka_health_check";
    public static final String HEALTH_CHECK_TEXT_MESSAGE = "health_check_";

}