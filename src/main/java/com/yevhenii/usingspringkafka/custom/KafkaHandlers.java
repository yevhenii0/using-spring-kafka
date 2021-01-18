package com.yevhenii.usingspringkafka.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class KafkaHandlers {

  private final ObjectMapper objectMapper;

  public KafkaHandlerBuilder builder(String topic1, String... other) {
    return new KafkaHandlerBuilder(Lists.asList(topic1, other));
  }

  @RequiredArgsConstructor
  public class KafkaHandlerBuilder {
    private final List<String> topics;
    private ShardId shardId = ShardId.DEFAULT;

    public KafkaHandlerBuilder shardId(ShardId shardId) {
      this.shardId = shardId;
      return this;
    }

    public <T> KafkaHandlerForMessageConsumerBuilder<T> messageType(Class<T> messageType) {
      return new KafkaHandlerForMessageConsumerBuilder<T>(this, messageType);
    }

    public <K, V> KafkaHandler build(RecordConsumer<K, V> recordConsumer) {
      return new GenericKafkaHandler(shardId, topics, recordConsumer);
    }
  }

  public class KafkaHandlerForMessageConsumerBuilder<T> {

    private final KafkaHandlerBuilder parentBuilder;
    private final Class<T> messageType;

    private KafkaHandlerForMessageConsumerBuilder(KafkaHandlerBuilder parentBuilder, Class<T> messageType) {
      this.parentBuilder = parentBuilder;
      this.messageType = messageType;
    }

    public KafkaHandler buildJsonHandler(MessageConsumer<T> messageConsumer) {
      return new GenericKafkaHandler(
              parentBuilder.shardId,
              parentBuilder.topics,
              new JsonMessageConsumerToRecordConsumerAdapter<>(
                      objectMapper,
                      messageType,
                      messageConsumer
              )
      );
    }
  }

  @RequiredArgsConstructor
  private static class GenericKafkaHandler implements KafkaHandler {

    private final ShardId shardId;
    private final List<String> topics;
    private final RecordConsumer<?, ?> recordConsumer;

    @Override
    public ShardId shardId() {
      return shardId;
    }

    @Override
    public List<String> topics() {
      return topics;
    }

    @Override
    public RecordConsumer<?, ?> consumer() {
      return recordConsumer;
    }
  }

  @RequiredArgsConstructor
  private static class JsonMessageConsumerToRecordConsumerAdapter<T> implements RecordConsumer<Object, Object> {

    private final ObjectMapper objectMapper;
    private final Class<T> messageType;
    private final MessageConsumer<T> messageHandler;

    @Override
    public void onRecord(ConsumerRecord<Object, Object> record) {
      try {
        // at this point it's expected that record value type is string
        T message = objectMapper.readValue(record.value().toString(), messageType);
        messageHandler.onMessage(message, record);
      } catch (JsonProcessingException x) {
        throw new RuntimeException(x.getMessage(), x);
      }
    }
  }
}
