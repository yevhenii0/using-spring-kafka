package com.yevhenii.usingspringkafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import com.yevhenii.usingspringkafka.util.Names;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

class ManuallyCreatedKafkaMessageListenerContainerIsConsumingMessagesTest {

  @Test
  void messageListenerWorks() throws Exception {
    String topic1 = Names.randTopic();
    String topic2 = Names.randTopic();
    ContainerProperties containerProps = new ContainerProperties(topic1, topic2);
    final CountDownLatch latch = new CountDownLatch(4);

    CopyOnWriteArraySet<String> messages = new CopyOnWriteArraySet<>();
    containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
      messages.add(message.value());
      latch.countDown();
    });

    KafkaMessageListenerContainer<String, String> container = SpringKafkaFactories.createContainer(containerProps, Names.randGroupId());
    container.setBeanName("messageListenerTest");
    container.start();

    KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
    template.setDefaultTopic(topic1);
    template.sendDefault("1", "x1");
    template.sendDefault("2", "x2");
    template.sendDefault("3", "x3");
    template.sendDefault("4", "x4");
    template.flush();

    assertTrue(latch.await(60, TimeUnit.SECONDS));
    assertEquals(messages, Set.of("x1", "x2", "x3", "x4"));

    container.stop();
  }
}
