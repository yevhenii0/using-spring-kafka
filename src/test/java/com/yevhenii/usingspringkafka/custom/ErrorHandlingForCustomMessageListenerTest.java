package com.yevhenii.usingspringkafka.custom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.RequiredArgsConstructor;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@SpringBootTest
@Import(ErrorHandlingForCustomMessageListenerTest.Cnf.class)
class ErrorHandlingForCustomMessageListenerTest {

  private static final int RETRIES = 3;
  private static final String T1 = "topic-error-handling-for-custom-message-listener";

  @Autowired
  private Listener listener;

  @Test
  void failedMessageGetsRetried() {
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.send(T1, "x1");
    template.send(T1, "x2");

    await().until(() -> listener.messagesConsumed.contains("x2"));
    assertThat(listener.messagesConsumed).containsExactly("x1", "x1", "x1", "x2");
  }

  @TestConfiguration
  @RequiredArgsConstructor
  static class Cnf {

    private final ConcurrentKafkaListenerContainerFactoryConfigurer configurer;
    private final KafkaProperties kafkaProperties;

    @Bean
    Listener listener() {
      return new Listener();
    }

    @Bean
    ShardConfiguration shardConfiguration() {
      return new ShardConfiguration() {
        @Override
        public ShardId getShardId() {
          return new ShardId(ErrorHandlingForCustomMessageListenerTest.class.getSimpleName());
        }

        @Override
        public ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory() {
          ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
          configurer.configure(factory, new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties()));
          factory.setErrorHandler(new SeekToCurrentErrorHandler(new FixedBackOff(100, RETRIES - 1)));
          factory.setMessageConverter(new MessagingMessageConverter());
          return factory;
        }
      };
    }
  }

  static class Listener implements CustomMessageListener<String> {

    private final CopyOnWriteArrayList<String> messagesConsumed = new CopyOnWriteArrayList<>();

    @Override
    public Class<String> messageType() {
      return String.class;
    }

    @Override
    public void onMessage(String message) {
      messagesConsumed.add(message);
      if (message.equals("x1")) {
        throw new RuntimeException("oops");
      }
    }

    @Override
    public List<String> topics() {
      return List.of(T1);
    }

    @Override
    public ShardId shardId() {
      return new ShardId(ErrorHandlingForCustomMessageListenerTest.class.getSimpleName());
    }
  }
}
