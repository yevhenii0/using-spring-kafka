package com.yevhenii.usingspringkafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class Mapper {

  private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  public static String toJson(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (JsonProcessingException x) {
      throw new RuntimeException(x.getMessage(), x);
    }
  }

  public static <T> T toObject(String json, Class<T> clazz) {
    try {
      return mapper.readValue(json, clazz);
    } catch (JsonProcessingException x) {
      throw new RuntimeException(x.getMessage(), x);
    }
  }
}
