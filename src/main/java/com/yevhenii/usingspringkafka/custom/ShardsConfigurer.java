package com.yevhenii.usingspringkafka.custom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;


@Slf4j
@RequiredArgsConstructor
@Configuration
public class ShardsConfigurer implements KafkaListenerConfigurer {

  private final List<ShardConfiguration> configurations;
  private final List<KafkaHandler> handlers;

  @Override
  public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
    log.info("Number of registered listeners: {}", handlers.size());

    Map<ShardId, ShardConfiguration> id2configuration = configurations.stream().collect(
            Collectors.toMap(
                    ShardConfiguration::shardId,
                    Function.identity()
            )
    );

    Map<ShardId, List<KafkaHandler>> id2handlers = new HashMap<>();
    for (KafkaHandler handler : handlers) {
      id2handlers.putIfAbsent(handler.shardId(), new ArrayList<>());
      id2handlers.get(handler.shardId()).add(handler);
    }

    // verify there is shard configuration present for all listeners
    for (Map.Entry<ShardId, List<KafkaHandler>> entry : id2handlers.entrySet()) {
      ShardId shardId = entry.getKey();
      ShardConfiguration configuration = id2configuration.get(shardId);
      if (configuration == null) {
        throw new ShardIsNotConfiguredException(shardId);
      }
    }

    // make sure that endpoint can be created for every batch of listeners
    Map<ShardId, ShardEndpoint> endpoints = new HashMap<>();
    for (ShardConfiguration configuration : configurations) {
      ShardId shardId = configuration.shardId();
      List<KafkaHandler> handlers = id2handlers.get(shardId);
      if (handlers == null || handlers.isEmpty()) {
        log.warn("Shard {} is configured, but no handlers using it", shardId.value());
      } else {
        ShardEndpoint endpoint = new ShardEndpoint(shardId, handlers);
        endpoints.put(shardId, endpoint);
      }
    }

    // if we reached here, everything is good in terms of configuration
    for (Map.Entry<ShardId, ShardEndpoint> entry : endpoints.entrySet()) {
      ConcurrentKafkaListenerContainerFactory<?, ?> factory = id2configuration.get(entry.getKey()).containerFactory();
      registrar.registerEndpoint(entry.getValue(), factory);
      log.info("Registered endpoint {} for shard {}", entry.getValue().getId(), entry.getKey().value());
    }
  }

  private static class ShardIsNotConfiguredException extends RuntimeException {
    ShardIsNotConfiguredException(ShardId shardId) {
      super(String.format("Shard %s is not registered. Implement ShardConfiguration", shardId));
    }
  }
}
