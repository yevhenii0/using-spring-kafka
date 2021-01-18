package com.yevhenii.usingspringkafka.custom;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

/**
 * Configuration of a new shard in the system.
 * Each shard requires configuration to be defined.
 */
public interface ShardConfiguration {

  ShardId shardId();

  ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory();
}
