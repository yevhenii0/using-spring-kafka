package com.yevhenii.usingspringkafka.ext;

import static org.assertj.core.api.Assertions.assertThat;


import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerSeekAware;

@SpringBootTest
class KafkaListenerRetriesPollWhenExceptionOccursInRebalanceListener {

  private static final String T1 = "retry-rebalance-listener-failures";

  @Autowired
  private Listener listener;

  @Autowired
  private EventuallyApplyingRelativeOffsetRebalanceListener eventuallyApplyingRelativeOffsetRebalanceListener;

  @Autowired
  @Qualifier(Listener.CONTAINER_GROUP)
  private List<ConcurrentMessageListenerContainer<?, ?>> containers;

  @Test
  void resetsToDefaultOffsetWhenRelativeOffsetWhenConsumerRebalanceListenerThrowsException() {
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.setDefaultTopic(T1);
    template.sendDefault("x1");
    template.sendDefault("x2");
    template.flush();

    Awaitility.await().until(() -> listener.messages.size() == 2);
    assertThat(listener.messages).containsExactly("x1", "x2");
    assertThat(eventuallyApplyingRelativeOffsetRebalanceListener.onPartitionAssignedCalls.get()).isEqualTo(3);

    containers.get(0).stop(true);
    listener.messages.clear();

    template.sendDefault("x3");
    template.flush();

    containers.get(0).start();
    Awaitility.await().until(() -> listener.messages.size() == 1);
    assertThat(listener.messages).contains("x3");
    assertThat(eventuallyApplyingRelativeOffsetRebalanceListener.onPartitionAssignedCalls.get()).isEqualTo(6);
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
      factory.getContainerProperties().setConsumerRebalanceListener(eventuallyApplyingRelativeOffsetListenerRebalanceListener());
      return factory;
    }

    @Bean
    ConsumerFactory<String, String> consumerFactory() {
      return SpringKafkaFactories.createConsumerFactory(Names.randGroupId());
    }

    @Bean
    EventuallyApplyingRelativeOffsetRebalanceListener eventuallyApplyingRelativeOffsetListenerRebalanceListener() {
      return new EventuallyApplyingRelativeOffsetRebalanceListener();
    }
  }

  static class Listener implements ConsumerSeekAware {

    static final String CONTAINER_GROUP = "ConsumerAwareListener";

    final List<String> messages = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = {T1}, containerGroup = CONTAINER_GROUP)
    void consume(String message) {
      messages.add(message);
    }
  }

  static class EventuallyApplyingRelativeOffsetRebalanceListener implements ConsumerAwareRebalanceListener {

    final List<BiConsumer<Consumer<?, ?>, Collection<TopicPartition>>> actions = List.of(
        (c, p) -> { throw new RuntimeException("oops"); },
        (c, p) -> { throw new KafkaException("oops"); },
        (c, p) -> new RelativeOffsetAssigningRebalanceListener(Duration.ofMinutes(30)).onPartitionsAssigned(c, p)
    );

    final AtomicInteger onPartitionAssignedCalls = new AtomicInteger();

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
      actions.get(onPartitionAssignedCalls.getAndIncrement() % actions.size()).accept(consumer, partitions);
    }
  }
}
