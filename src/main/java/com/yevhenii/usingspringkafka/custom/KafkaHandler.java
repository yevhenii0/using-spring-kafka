package com.yevhenii.usingspringkafka.custom;

import java.util.List;

public interface KafkaHandler {
  ShardId shardId();
  List<String> topics();
  RecordConsumer<?, ?> consumer();
}
