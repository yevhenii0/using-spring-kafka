package com.yevhenii.usingspringkafka.custom;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageConsumer<T> {
  void onMessage(T message, ConsumerRecord<Object, Object> record);
}
