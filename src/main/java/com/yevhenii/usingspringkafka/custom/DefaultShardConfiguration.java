package com.yevhenii.usingspringkafka.custom;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.stereotype.Component;

/**
 * Reuses default spring-kafka configuration.
 */
@Component
@RequiredArgsConstructor
public class DefaultShardConfiguration implements ShardConfiguration {

  private final ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory;

  @Override
  public ShardId shardId() {
    return ShardId.DEFAULT;
  }

  @Override
  public ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory() {
    return kafkaListenerContainerFactory;
  }
}
