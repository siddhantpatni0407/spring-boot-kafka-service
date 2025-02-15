package com.sid.app.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Siddhant Patni
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer {

    @JsonProperty("id")
    private int id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("email")
    private String email;

    @JsonProperty("contactNo")
    private String contactNo;

}