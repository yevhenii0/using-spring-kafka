package com.yevhenii.usingspringkafka.custom;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
public class ShardEndpoint implements KafkaListenerEndpoint {

  public static final String CONTAINERS_GROUP = "custom-kafka-shards";

  private final String id;
  private final Map<String, RecordConsumer<?, ?>> topic2consumer;

  public ShardEndpoint(ShardId shardId, List<KafkaHandler> handlers) {
    this.id = "shard." + shardId.value();
    Map<String, RecordConsumer<?, ?>> topic2consumer = new HashMap<>();
    for (KafkaHandler handler : handlers) {
      for (String topic : handler.topics()) {
        if (topic2consumer.put(topic, handler.consumer()) != null) {
          log.error("Same topic registered by 2 separate handlers {} in the same shard {}", topic, shardId);
        }
      }
    }
    this.topic2consumer = Map.copyOf(topic2consumer);
  }

  /**
   * Identifies this shard in unique way, on order to avoid collision with
   * existing kafka endpoints every shard endpoint is prefixed with 'shard.'.
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * Reusing the one configured by consumer factory.
   */
  @Override
  public String getGroupId() {
    return null;
  }

  /**
   * All shards have the same group, so it's possible to inject
   * corresponding ConcurrentMessageListenerContainer instances e.g.
   *
   * <pre>{@code
   *   @Autowired
   *   @Qualifier(ShardEndpoint.CONTAINERS_GROUP)
   *   List<ConcurrentMessageListenerContainer<?, ?>> containers;
   * }</pre>
   */
  @Override
  public String getGroup() {
    return CONTAINERS_GROUP;
  }

  /**
   * The kafka consumer will be consuming messages from these topics.
   * Topics list is derived from all the topics defined by listeners in this shard.
   */
  @Override
  public Collection<String> getTopics() {
    return topic2consumer.keySet();
  }

  /**
   * Not allowing to configure custom partition offsets to assign.
   * In case it's needed use {@link @KafkaListener directly).
   */
  @Override
  public TopicPartitionOffset[] getTopicPartitionsToAssign() {
    return new TopicPartitionOffset[0];
  }

  /**
   * Not allowing to configure patterns, using the exact topic names.
   */
  @Override
  public Pattern getTopicPattern() {
    return null;
  }

  /**
   * Using shard's id, will be logged as part of kafka consumer configuration.
   */
  @Override
  public String getClientIdPrefix() {
    return id;
  }

  /**
   * At this point configuring it to be always 1, in case higher concurrency is needed, consider using
   * {@link @KafkaListener} directly.
   */
  @Override
  public Integer getConcurrency() {
    return 1;
  }

  /**
   * Not allowing to change it at this point.
   */
  @Override
  public Boolean getAutoStartup() {
    return null;
  }

  @Override
  public void setupListenerContainer(MessageListenerContainer listenerContainer, MessageConverter messageConverter) {
    // messages conversion is implemented separately
    listenerContainer.setupMessageListener(new CompositeKafkaMessageListener(topic2consumer));
  }

  @Override
  public boolean isSplitIterables() {
    return true;
  }

  private static class CompositeKafkaMessageListener implements MessageListener<Object, Object> {

    final Map<String, RecordConsumer<?, ?>> topic2consumer;

    private CompositeKafkaMessageListener(Map<String, RecordConsumer<?, ?>> topic2consumer) {
      this.topic2consumer = topic2consumer;
    }

    @Override
    @SuppressWarnings({"all"})
    public void onMessage(ConsumerRecord<Object, Object> record) {
      RecordConsumer recordConsumer = topic2consumer.get(record.topic());
      recordConsumer.onRecord(record);
    }
  }
}
