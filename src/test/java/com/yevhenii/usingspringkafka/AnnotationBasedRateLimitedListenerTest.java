package com.yevhenii.usingspringkafka;

import com.google.common.util.concurrent.RateLimiter;
import com.yevhenii.usingspringkafka.util.DataGenerator;
import com.yevhenii.usingspringkafka.util.MessageSender;
import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import(AnnotationBasedRateLimitedListenerTest.Cnf.class)
@Slf4j
public class AnnotationBasedRateLimitedListenerTest {

  private static final String T1 = "topic-rate-limited-consumer-1";

  @Autowired
  Listener listener;

  @Test
  void listenerIsRateLimited() throws Exception {
    new MessageSender(T1, DataGenerator.paymentCreatedEvents()).send(1_000_000);

    await().atMost(Duration.ofSeconds(30)).until(() -> listener.messagesProcessed.get() > 300);

    log.info("Values: {}", listener.messagesPerSecond);
    // rate limited to 10, but it might vary a bit
    // skipping first elements due to warmup
    for (Integer value : listener.messagesPerSecond.subList(10, listener.messagesPerSecond.size())) {
      assertThat(value).isLessThan(15);
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
      factory.setConsumerFactory(consumerFactory());
      factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
      return factory;
    }

    @Bean
    ConsumerFactory<String, String> consumerFactory() {
      return SpringKafkaFactories.createConsumerFactory(Names.randGroupId());
    }
  }


  static class Listener {

    private final RateLimiter rateLimiter = RateLimiter.create(10);
    private final long startTimeOffsetSeconds;
    private final CopyOnWriteArrayList<Integer> messagesPerSecond;
    private final AtomicInteger messagesProcessed = new AtomicInteger();

    Listener() {
      startTimeOffsetSeconds = System.currentTimeMillis() / 1000;
      messagesPerSecond = new CopyOnWriteArrayList<>();
      // second slots for 10m
      for (int i = 0; i < 10 * 60; i++) {
        messagesPerSecond.add(0);
      }
    }

    @KafkaListener(topics = T1)
    void consume(String message, Acknowledgment acknowledgment) {
      // redeliver the message if it rate limit has been reached
      if (!rateLimiter.tryAcquire()) {
        acknowledgment.nack(100);
        return;
      }

      messagesProcessed.incrementAndGet();
      int idx = (int) (System.currentTimeMillis() / 1000 - startTimeOffsetSeconds);
      messagesPerSecond.set(idx, messagesPerSecond.get(idx) + 1);
      acknowledgment.acknowledge();
    }
  }
}
