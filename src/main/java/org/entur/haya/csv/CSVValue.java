package org.entur.haya.csv;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public record CSVValue(Object value, boolean json) {

    static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Override
    public String toString() {
        if (value == null) {
            return "";
        } else if (json) {
            try {
                return mapper.writeValueAsString(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return value.toString();
        }
    }
}
