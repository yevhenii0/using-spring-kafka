package com.yevhenii.usingspringkafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfiguration {

  @Bean
  RecordMessageConverter defaultRecordMessageConverter() {
    return new JsonMessageConverter();
  }

  @Bean
  ErrorHandler defaultErrorHandler() {
    return new SeekToCurrentErrorHandler(new FixedBackOff(2000, FixedBackOff.UNLIMITED_ATTEMPTS));
  }
}
