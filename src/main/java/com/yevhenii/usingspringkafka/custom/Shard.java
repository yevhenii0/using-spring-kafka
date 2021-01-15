package com.yevhenii.usingspringkafka.custom;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

@Slf4j
public class Shard {

  public static final String CONTAINERS_GROUP = "custom-kafka-shards";

  private final AtomicBoolean registered = new AtomicBoolean(false);
  private final ShardConfiguration configuration;
  private final Map<String, CustomMessageListener<?>> topic2listener;
  private final KafkaListenerContainerFactory<?> containerFactory;

  public Shard(ShardConfiguration configuration, List<CustomMessageListener<?>> listeners) {
    this.configuration = configuration;
    Map<String, CustomMessageListener<?>> topic2listener = new HashMap<>();
    for (CustomMessageListener<?> customMessageListener : listeners) {
      for (String topic : customMessageListener.topics()) {
        if (topic2listener.put(topic, customMessageListener) != null) {
          log.error("Same topic registered by 2 separate listeners {} in the same shard {}", topic, configuration.getShardId());
        }
      }
    }
    this.topic2listener = Map.copyOf(topic2listener);
    this.containerFactory = configuration.containerFactory();
  }

  public void register(KafkaListenerEndpointRegistrar registrar) {
    if (registered.compareAndSet(false, true)) {
      registrar.registerEndpoint(new ShardToKafkaEndpointAdapter(), containerFactory);
    }
  }

  private static class CompositeKafkaMessageListener implements MessageListener<Object, Object> {

    final Map<String, CustomMessageListener<?>> topic2listener;
    final RecordMessageConverter recordMessageConverter;

    private CompositeKafkaMessageListener(Map<String, CustomMessageListener<?>> topic2listener, RecordMessageConverter messageConverter) {
      this.topic2listener = topic2listener;
      this.recordMessageConverter = messageConverter;
    }

    @Override
    @SuppressWarnings({"all"})
    public void onMessage(ConsumerRecord<Object, Object> record) {
      CustomMessageListener customMessageListener = topic2listener.get(record.topic());
      Message<?> message = recordMessageConverter.toMessage(record, null, null, customMessageListener.messageType());
      customMessageListener.onMessage(message.getPayload());
    }
  }

  @RequiredArgsConstructor
  private class ShardToKafkaEndpointAdapter implements KafkaListenerEndpoint {

    @Override
    public String getId() {
      return "shard." + configuration.getShardId().value();
    }

    @Override
    public String getGroupId() {
      return null;
    }

    @Override
    public String getGroup() {
      return CONTAINERS_GROUP;
    }

    @Override
    public Collection<String> getTopics() {
      return topic2listener.keySet();
    }

    @Override
    public TopicPartitionOffset[] getTopicPartitionsToAssign() {
      return new TopicPartitionOffset[0];
    }

    @Override
    public Pattern getTopicPattern() {
      return null;
    }

    @Override
    public String getClientIdPrefix() {
      return null;
    }

    @Override
    public Integer getConcurrency() {
      return null;
    }

    @Override
    public Boolean getAutoStartup() {
      return null;
    }

    @Override
    public void setupListenerContainer(MessageListenerContainer listenerContainer, MessageConverter messageConverter) {
      listenerContainer.setupMessageListener(new CompositeKafkaMessageListener(topic2listener, (RecordMessageConverter) messageConverter));
    }

    @Override
    public boolean isSplitIterables() {
      return false;
    }
  }
}
