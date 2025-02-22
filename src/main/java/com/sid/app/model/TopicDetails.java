package com.sid.app.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @author Siddhant Patni
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TopicDetails {

    @JsonProperty("topicName")
    private String topicName;

    @JsonProperty("partitionCount")
    private int partitionCount;

    @JsonProperty("totalMessages")
    private long totalMessages;

    @JsonProperty("totalLag")
    private long totalLag;

}