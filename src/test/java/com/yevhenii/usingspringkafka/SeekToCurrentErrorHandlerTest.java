package com.yevhenii.usingspringkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.yevhenii.usingspringkafka.payment.PaymentCreatedEvent;
import com.yevhenii.usingspringkafka.util.DataGenerator;
import com.yevhenii.usingspringkafka.util.Mapper;
import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.util.backoff.FixedBackOff;

@SpringBootTest
class SeekToCurrentErrorHandlerTest {

  private static final String T1 = "topic-continue-processing-when-error-encountered-1";
  private static final String T2 = "topic-continue-processing-when-error-encountered-2";
  private static final int RETRIES = 5;

  @Autowired
  private NotRetryingDeserializationFailuresListener t1Listener;

  @Autowired
  private RetryingDeserializationFailuresListener t2Listener;

  @Autowired
  private Dlq dlq;

  @AfterEach
  void cleanup() {
    dlq.records.clear();
  }

  @Test
  void brokenMessageEndsUpInDlqImmediately() {
    DataGenerator<PaymentCreatedEvent> generator = DataGenerator.paymentCreatedEvents();
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.send(T1, Mapper.toJson(generator.gen()));
    template.send(T1, "{\"brokenJson");
    template.send(T1, "{\"brokenJson2");
    template.send(T1, Mapper.toJson(generator.gen()));
    await().until(() -> t1Listener.paymentIds.size() == 2);

    assertThat(t1Listener.failures.get()).isEqualTo(2);
    assertThat(dlq.records.size()).isEqualTo(2);
    assertThat(dlq.records.get(0).value()).isEqualTo("{\"brokenJson");
    assertThat(dlq.records.get(1).value()).isEqualTo("{\"brokenJson2");
  }

  @Test
  void brokenMessageIsRetriedAndThenEndsUpInDlq() {
    DataGenerator<PaymentCreatedEvent> generator = DataGenerator.paymentCreatedEvents();
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.send(T2, Mapper.toJson(generator.gen()));
    template.send(T2, "{\"brokenJson");
    template.send(T2, "{\"brokenJson2");
    template.send(T2, Mapper.toJson(generator.gen()));

    await().until(() -> t2Listener.paymentIds.size() == 2);

    assertThat(t2Listener.failures.get()).isEqualTo(RETRIES * 2);
    assertThat(dlq.records.size()).isEqualTo(2);
    assertThat(dlq.records.get(0).value()).isEqualTo("{\"brokenJson");
    assertThat(dlq.records.get(1).value()).isEqualTo("{\"brokenJson2");
  }

  @TestConfiguration
  static class Cnf {

    @Bean
    NotRetryingDeserializationFailuresListener notRetryingDeserializationFailuresListener() {
      return new NotRetryingDeserializationFailuresListener();
    }

    @Bean
    RetryingDeserializationFailuresListener retryingDeserializationFailuresListener() {
      return new RetryingDeserializationFailuresListener();
    }

    @Bean
    Dlq dlq() {
      return new Dlq();
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
      ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(consumerFactory());
      factory.setErrorHandler(new SeekToCurrentErrorHandler(dlq(), new FixedBackOff(100, RETRIES - 1)));
      return factory;
    }

    @Bean
    ConsumerFactory<String, String> consumerFactory() {
      return SpringKafkaFactories.createConsumerFactory(Names.randGroupId());
    }
  }

  @RequiredArgsConstructor
  private static class TrackingListener {
    final Set<UUID> paymentIds = new CopyOnWriteArraySet<>();
    final AtomicInteger failures = new AtomicInteger();

    final Function<String, PaymentCreatedEvent> mapper;

    void handle(String content) {
      try {
        var paymentCreatedEvent = mapper.apply(content);
        paymentIds.add(paymentCreatedEvent.getPayment().getUuid());
      } catch (RuntimeException x) {
        failures.incrementAndGet();
        // Note that it will be logged the the KafkaMessageListenerContainer#ListenerConsumer
        throw x;
      }
    }
  }

  static class NotRetryingDeserializationFailuresListener extends TrackingListener {
    public NotRetryingDeserializationFailuresListener() {
      super(content -> {
        try {
          return Mapper.MAPPER.readValue(content, PaymentCreatedEvent.class);
        } catch (JsonProcessingException x) {
          throw new ConversionException(x.getMessage(), x);
        }
      });
    }

    @KafkaListener(topics = T1)
    public void consume(String content) {
      handle(content);
    }
  }

  static class RetryingDeserializationFailuresListener extends TrackingListener {
    public RetryingDeserializationFailuresListener() {
      super(content -> {
        try {
          return Mapper.MAPPER.readValue(content, PaymentCreatedEvent.class);
        } catch (JsonProcessingException x) {
          throw new RuntimeException(x.getMessage(), x);
        }
      });
    }

    @KafkaListener(topics = T2)
    public void consume(String content) {
      handle(content);
    }
  }

  static class Dlq implements BiConsumer<ConsumerRecord<?, ?>, Exception> {
    final List<ConsumerRecord<?, ?>> records = new CopyOnWriteArrayList<>();

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception e) {
      records.add(record);
    }
  }
}
