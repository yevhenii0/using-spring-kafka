package com.yevhenii.usingspringkafka;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;


import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import com.yevhenii.usingspringkafka.util.Names;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest
@Import(AnnotationBasedListenerIsConsumingMessagesTest.Cnf.class)
class AnnotationBasedListenerIsConsumingMessagesTest {

  private static final String T1 = "topic-annotation-based-listener-1";

  @Autowired
  private Listener listener;

  @Test
  void messagesCanBeConsumed() {
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.setDefaultTopic(T1);
    template.sendDefault("1", "x1");
    template.sendDefault("2", "x2");
    template.sendDefault("3", "x3");
    template.sendDefault("4", "x4");
    template.flush();

    await().until(() -> listener.messagesConsumed.size() == 4);
    assertEquals(listener.messagesConsumed, Set.of("x1", "x2", "x3", "x4"));
  }

  @TestConfiguration
  static class Cnf {

    @Bean
    Listener listener() {
      return new Listener();
    }

    /**
     * Note that it's mandatory to have bean named this way if we want to override a default one!
     * Check out {@link KafkaListener} documentation for more details.
     */
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

  static class Listener {

    private final Set<String> messagesConsumed = new CopyOnWriteArraySet<>();

    @KafkaListener(topics = {T1})
    void consume(String message) {
      messagesConsumed.add(message);
    }
  }
}
