package com.yevhenii.usingspringkafka.custom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


import com.yevhenii.usingspringkafka.ext.RelativeOffsetAssigningRebalanceListener;
import com.yevhenii.usingspringkafka.util.Mapper;
import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

@SpringBootTest
@Import(CustomMessageListenerTest.Cnf.class)
class CustomMessageListenerTest {

  @Autowired
  ListenerA listenerA;

  @Autowired
  ListenerB listenerB;

  @Autowired
  ListenerC listenerC;

  @Autowired
  @Qualifier(Shard.CONTAINERS_GROUP)
  List<ConcurrentMessageListenerContainer<?, ?>> containers;

  @Test
  void allMessagesGetConsumed() {
    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.send(A.TOPIC, Mapper.toJson(new A("a1")));
    template.send(A.TOPIC, Mapper.toJson(new A("a2")));
    template.send(B.TOPIC, Mapper.toJson(new B("b1")));
    template.send(B.TOPIC, Mapper.toJson(new B("b2")));
    template.send(B.TOPIC, Mapper.toJson(new B("b3")));
    template.send(C.TOPIC, Mapper.toJson(new C("c1")));
    template.send(C.TOPIC, Mapper.toJson(new C("c2")));
    template.send(C.TOPIC, Mapper.toJson(new C("c3")));

    await().until(() -> listenerA.messages.size() == 2);
    await().until(() -> listenerB.messages.size() == 3);
    await().until(() -> listenerC.messages.size() == 3);
    assertThat(listenerA.messages).containsExactly(new A("a1"), new A("a2"));
    assertThat(listenerB.messages).containsExactly(new B("b1"), new B("b2"), new B("b3"));
    assertThat(listenerC.messages).containsExactly(new C("c1"), new C("c2"), new C("c3"));

    // 3 listeners, but 2 containers
    assertThat(containers.size()).isEqualTo(2);
    assertThat(containers.stream().map(AbstractMessageListenerContainer::getBeanName)).containsExactlyInAnyOrder(
            "shard." + ShardId.DEFAULT.value(),
            "shard." + MyIntegrationShardId.P1.value()
    );
  }

  @TestConfiguration
  @RequiredArgsConstructor
  static class Cnf {

    private final ConcurrentKafkaListenerContainerFactoryConfigurer configurer;
    private final KafkaProperties kafkaProperties;

    @Bean
    MyIntegrationShardConfiguration myIntegrationShardConfiguration() {
      return new MyIntegrationShardConfiguration(configurer, kafkaProperties);
    }

    @Bean
    ListenerA listenerA() {
      return new ListenerA();
    }

    @Bean
    ListenerB listenerB() {
      return new ListenerB();
    }

    @Bean
    ListenerC listenerC() {
      return new ListenerC();
    }
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class A {
    static final String TOPIC = Names.randTopic();

    String id;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class B {
    static final String TOPIC = Names.randTopic();

    String id;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class C {
    static final String TOPIC = Names.randTopic();

    String id;
  }

  static abstract class TrackingListener<T> implements CustomMessageListener<T> {

    final CopyOnWriteArrayList<T> messages = new CopyOnWriteArrayList<>();

    @Override
    public final void onMessage(T message) {
      messages.add(message);
    }
  }

  static class ListenerA extends TrackingListener<A> {
    @Override
    public Class<A> messageType() {
      return A.class;
    }

    @Override
    public List<String> topics() {
      return List.of(A.TOPIC);
    }
  }

  static class ListenerB extends TrackingListener<B> {

    @Override
    public Class<B> messageType() {
      return B.class;
    }

    @Override
    public List<String> topics() {
      return List.of(B.TOPIC);
    }
  }

  // Uses non default shard!
  static class ListenerC extends TrackingListener<C> {

    @Override
    public Class<C> messageType() {
      return C.class;
    }

    @Override
    public List<String> topics() {
      return List.of(C.TOPIC);
    }

    @Override
    public ShardId shardId() {
      return MyIntegrationShardId.P1;
    }
  }

  static class MyIntegrationShardId extends ShardId {

    public static final MyIntegrationShardId P1 = new MyIntegrationShardId(1);

    public MyIntegrationShardId(int priority) {
      super("MyIntegration.Priority." + priority);
    }
  }

  @RequiredArgsConstructor
  static class MyIntegrationShardConfiguration implements ShardConfiguration {

    private final ConcurrentKafkaListenerContainerFactoryConfigurer configurer;
    private final KafkaProperties kafkaProperties;

    @Override
    public ShardId getShardId() {
      return MyIntegrationShardId.P1;
    }

    @Override
    public ConcurrentKafkaListenerContainerFactory<Object, Object> containerFactory() {
      // configuring some custom consumer properties
      Map<String, Object> consumerConfigs = kafkaProperties.buildConsumerProperties();
      consumerConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
      consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

      ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
      configurer.configure(factory, new DefaultKafkaConsumerFactory<>(consumerConfigs));
      ContainerProperties containerProperties = factory.getContainerProperties();
      containerProperties.setConsumerRebalanceListener(new RelativeOffsetAssigningRebalanceListener(Duration.ofMinutes(10).negated()));
      return factory;
    }
  }
}
