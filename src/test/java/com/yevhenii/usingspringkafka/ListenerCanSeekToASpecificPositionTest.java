package com.yevhenii.usingspringkafka;

import static org.assertj.core.api.Assertions.assertThat;


import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;

@SpringBootTest
class ListenerCanSeekToASpecificPositionTest {

  private static final String T1 = "consumer-can-seek-to-position-test-topic";

  @Autowired
  private Listener listener;

  @Test
  void consumesMessagesFromTheSpecifiedOffset() {
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.setDefaultTopic(T1);
    template.sendDefault("1", "x1");
    template.sendDefault("2", "x2");
    template.sendDefault("3", "x3");
    template.sendDefault("4", "x4");
    template.sendDefault("5", "x5");
    template.flush();

    Awaitility.await().until(() -> listener.messages.size() == 2);
    assertThat(listener.messages).containsExactly("x4", "x5");
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
      factory.setConsumerFactory(consumerFactory());
      return factory;
    }

    @Bean
    ConsumerFactory<String, String> consumerFactory() {
      return SpringKafkaFactories.createConsumerFactory(Names.randGroupId());
    }
  }

  static class Listener implements ConsumerSeekAware {

    private final List<String> messages = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = {T1})
    void consume(String message) {
      messages.add(message);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
      callback.seek(T1, 0, 3);
    }
  }
}
