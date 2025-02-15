package com.sid.app.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;

/**
 * @author Siddhant Patni
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TopicDetails {

    @JsonProperty("name")
    private String name;

    @JsonProperty("numPartitions")
    private Integer numPartitions;

    @JsonProperty("partitions")
    private List<TopicPartitionInfo> partitions;

}