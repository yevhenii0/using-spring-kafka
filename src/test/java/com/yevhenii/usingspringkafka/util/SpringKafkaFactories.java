package com.yevhenii.usingspringkafka.util;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

public class SpringKafkaFactories {

  public static KafkaTemplate<String, String> createTemplate() {
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(KafkaFactories.getDefaultProducerConfigs()));
  }

  public static KafkaMessageListenerContainer<String, String> createContainer(ContainerProperties containerProps, String groupId) {
    return new KafkaMessageListenerContainer<>(createConsumerFactory(groupId), containerProps);
  }

  public static ConsumerFactory<String, String> createConsumerFactory(String groupId) {
    return new DefaultKafkaConsumerFactory<>(
        KafkaFactories.getDefaultConsumerConfigs(Map.of(
            ConsumerConfig.GROUP_ID_CONFIG, groupId
        ))
    );
  }
}
