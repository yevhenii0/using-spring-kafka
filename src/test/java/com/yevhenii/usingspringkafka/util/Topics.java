package com.yevhenii.usingspringkafka.util;

import java.util.List;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public final class Topics {

  public static NewTopic rand() throws ExecutionException, InterruptedException {
    return rand(1, 1);
  }

  public static NewTopic rand(int partitions, int repFac) throws ExecutionException, InterruptedException {
    NewTopic topic = new NewTopic(Names.randTopic(), partitions, (short) repFac);
    try (AdminClient client = KafkaFactories.createAdminClient()) {
      client.createTopics(List.of(topic)).all().get();
    }
    log.info("Created a new topic. Name: {}, partitions {}, replication factor: {}", topic.name(), partitions, repFac);
    return topic;
  }

  public static void remove(String topic) throws ExecutionException, InterruptedException {
    try (AdminClient client = KafkaFactories.createAdminClient()) {
      client.deleteTopics(List.of(topic)).all().get();
    }
    log.info("Removed topic {}", topic);
  }
}
