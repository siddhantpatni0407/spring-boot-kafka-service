package com.sid.app.constant;

/**
 * @author Siddhant Patni
 * Utility class containing constants used for defining endpoints.
 */
public class AppConstants {

    /**
     * Constant for the Kafka publish message endpoint.
     */
    public static final String KAFKA_PUBLISH_MESSAGE_WITH_ERROR_HANDLING__ENDPOINT = "/api/v1/kafka-service/error-handling/producer";
    public static final String CSV_FILE_PATH = "src/main/resources/customers.csv";

}