package com.yevhenii.usingspringkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


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
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.util.backoff.FixedBackOff;

@SpringBootTest
@Import(SeekToCurrentErrorHandlerTest.Cnf.class)
class SeekToCurrentErrorHandlerTest {

  private static final String T1 = "topic-continue-processing-when-error-encountered-1";
  private static final int RETRIES = 5;

  @Autowired
  private Listener listener;

  @Autowired
  private Dlq dlq;

  @Test
  void brokenMessageGetsRetriedSeveralTimesAndThenSentToDlq() {
    DataGenerator<PaymentCreatedEvent> generator = DataGenerator.paymentCreatedEvents();
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.setDefaultTopic(T1);
    template.sendDefault(Mapper.toJson(generator.gen()));
    template.sendDefault("{\"brokenJson");
    template.sendDefault("{\"brokenJson2");
    template.sendDefault(Mapper.toJson(generator.gen()));

    await().until(() -> listener.paymentIds.size() == 2);

    assertThat(listener.failures.get()).isEqualTo(RETRIES * 2);
    assertThat(dlq.records.size()).isEqualTo(2);
    assertThat(dlq.records.get(0).value()).isEqualTo("{\"brokenJson");
    assertThat(dlq.records.get(1).value()).isEqualTo("{\"brokenJson2");
  }

  @TestConfiguration
  static class Cnf {

    @Bean
    Listener listener() {
      return new Listener();
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

  static class Listener {
    final Set<UUID> paymentIds = new CopyOnWriteArraySet<>();
    final AtomicInteger failures = new AtomicInteger();

    @KafkaListener(topics = T1)
    void consume(String message) {
      try {
        var paymentCreatedEvent = Mapper.toObject(message, PaymentCreatedEvent.class);
        paymentIds.add(paymentCreatedEvent.getPayment().getUuid());
      } catch (RuntimeException x) {
        failures.incrementAndGet();
        // Note that it will be logged the the KafkaMessageListenerContainer#ListenerConsumer
        throw x;
      }
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
