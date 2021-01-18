package com.yevhenii.usingspringkafka.custom;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordConsumer<K, V> {
  void onRecord(ConsumerRecord<K, V> record);
}
