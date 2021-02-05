package com.yevhenii.usingspringkafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.kafka.support.converter.ConversionException;

public final class Mapper {

  public static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

  public static String toJson(Object object) {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException x) {
      throw new ConversionException(x.getMessage(), x);
    }
  }

  public static <T> T toObject(String json, Class<T> clazz) {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (JsonProcessingException x) {
      throw new ConversionException(x.getMessage(), x);
    }
  }
}
