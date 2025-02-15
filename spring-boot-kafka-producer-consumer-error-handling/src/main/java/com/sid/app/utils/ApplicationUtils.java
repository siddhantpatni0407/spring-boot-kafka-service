package com.sid.app.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.sid.app.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

@Component
@Slf4j
public class ApplicationUtils {

    /**
     * Gets json string.
     *
     * @param <T>    the type parameter
     * @param object the object
     * @return the json string
     */
    public static <T> String getJSONString(T object) {
        if (object != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(object);
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error("getJSONString() : Error occurred [{}] while converting to string [{}]", e.getMessage(), object);
                }
            }
        }
        return "";
    }

    public static List<Customer> readDataFromCsv() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ClassPathResource("customers.csv").getInputStream()))) {
            CsvToBean<Customer> csvToBean = new CsvToBeanBuilder<Customer>(reader)
                    .withType(Customer.class)
                    .build();
            return csvToBean.parse();
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("readDataFromCsv() : Error occurred while reading the csv [{}]", e.getMessage());
            }
            return null;
        }
    }

}