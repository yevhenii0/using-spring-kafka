package com.yevhenii.usingspringkafka.custom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ShardManager {

  private final Map<ShardId, Shard> shards;

  ShardManager(
          List<ShardConfiguration> configurations,
          List<CustomMessageListener<?>> listeners,
          KafkaListenerEndpointRegistry endpointRegistry
  ) {
    log.info("Number of registered listeners: {}", listeners.size());

    Map<ShardId, ShardConfiguration> id2configuration = configurations.stream().collect(
            Collectors.toMap(
                    ShardConfiguration::getShardId,
                    Function.identity()
            )
    );

    Map<ShardId, List<CustomMessageListener<?>>> id2listeners = new HashMap<>();
    for (CustomMessageListener<?> listener : listeners) {
      id2listeners.putIfAbsent(listener.shardId(), new ArrayList<>());
      id2listeners.get(listener.shardId()).add(listener);
    }

    Map<ShardId, Shard> allShards = new HashMap<>();
    for (Map.Entry<ShardId, List<CustomMessageListener<?>>> entry : id2listeners.entrySet()) {
      ShardId shardId = entry.getKey();
      ShardConfiguration configuration = id2configuration.get(shardId);
      if (configuration == null) {
        throw new ShardIsNotRegisteredException(shardId);
      }
      Shard shard = new Shard(configuration, entry.getValue());
      shard.register(endpointRegistry);
      allShards.put(shardId, shard);
    }
    this.shards = Map.copyOf(allShards);
  }

  private static class ShardIsNotRegisteredException extends RuntimeException {
    ShardIsNotRegisteredException(ShardId shardId) {
      super(String.format("Shard %s is not registered. Implement ShardConfiguration", shardId));
    }
  }
}
