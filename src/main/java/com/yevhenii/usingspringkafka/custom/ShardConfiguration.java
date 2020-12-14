package com.yevhenii.usingspringkafka.custom;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

/**
 * Implements registers of a new shard in the system.
 * Each shard requires registration to be defined.
 */
public interface ShardConfiguration {

  ShardId getShardId();

  ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory();
}
