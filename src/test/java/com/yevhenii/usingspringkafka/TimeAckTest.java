package com.yevhenii.usingspringkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


import com.yevhenii.usingspringkafka.util.KafkaFactories;
import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;

@SpringBootTest
@Slf4j
public class TimeAckTest {

  private static final String CONSUMER_GROUP = Names.randGroupId();
  private static final String T1 = "time-ack-topic-1";
  private static final String CONTAINER_GROUP = "TimeAckTest";
  private static final long ACK_TIME_MS = Duration.ofSeconds(2).toMillis();
  private static final long POLL_TIMEOUT_MS = 200;

  @Autowired
  private Listener listener;

  private AdminClient adminClient;

  @BeforeEach
  void createAdminClient() {
    adminClient = KafkaFactories.createAdminClient();
  }

  @AfterEach
  void closeAdminClient() {
    adminClient.close();
  }

  @Test
  void timeAckWorks() {
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.setDefaultTopic(T1);
    template.sendDefault("x1");
    template.flush();

    // Expect that offset will be committed at some point
    await().pollInterval(Duration.ofMillis(500)).atMost(Duration.ofMillis(ACK_TIME_MS * 2)).until(() -> {
      OffsetAndMetadata metadata = requestOffsetAndMetadata();
      log.info("Received metadata: " + metadata);
      return metadata != null && metadata.offset() == 1;
    });

    template.sendDefault("x2");
    template.sendDefault("x3");
    template.sendDefault("x4");
    template.flush();

    // Expect that offset will be committed at some point
    await().pollInterval(Duration.ofMillis(500)).atMost(Duration.ofMillis(ACK_TIME_MS * 2)).until(() -> {
      OffsetAndMetadata metadata = requestOffsetAndMetadata();
      log.info("Received metadata: " + metadata);
      return metadata != null && metadata.offset() == 4;
    });

    assertThat(listener.messagesConsumed).containsExactly("x1", "x2", "x3", "x4");
  }

  private OffsetAndMetadata requestOffsetAndMetadata() throws ExecutionException, InterruptedException {
    ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(CONSUMER_GROUP);
    Map<TopicPartition, OffsetAndMetadata> metadataMap = result.partitionsToOffsetAndMetadata().get();
    if (metadataMap.isEmpty()) {
      return null;
    } else {
      return metadataMap.values().iterator().next();
    }
  }

  @TestConfiguration
  static class Cnf {

    @Bean
    Listener listener() {
      return new Listener();
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
      ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(SpringKafkaFactories.createConsumerFactory(CONSUMER_GROUP));
      factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.TIME);
      factory.getContainerProperties().setAckTime(ACK_TIME_MS);
      // more frequent polls to trigger more frequent ack checks
      factory.getContainerProperties().setPollTimeout(POLL_TIMEOUT_MS);
      return factory;
    }
  }

  static class Listener {

    private final CopyOnWriteArrayList<String> messagesConsumed = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = T1, containerGroup = CONTAINER_GROUP)
    void consume(String message) {
      messagesConsumed.add(message);
    }
  }
}
