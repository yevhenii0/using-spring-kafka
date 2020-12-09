package com.yevhenii.usingspringkafka.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaFactories {

  public static AdminClient createAdminClient() {
    final HashMap<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return AdminClient.create(configs);
  }

  public static KafkaConsumer<String, String> createConsumer(String groupId) {
    return createConsumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, groupId));
  }

  public static KafkaConsumer<String, String> createConsumer(Map<String, Object> configOverride) {
    final Map<String, Object> configs = getDefaultConsumerConfigs(configOverride);
    return new KafkaConsumer<>(configs);
  }

  public static KafkaProducer<String, String> createProducer(Map<String, Object> configOverride) {
    final Map<String, Object> configs = getDefaultProducerConfigs();
    configs.putAll(configOverride);
    return new KafkaProducer<>(configs);
  }

  public static KafkaProducer<String, String> createProducer() {
    return createProducer(Map.of());
  }

  public static Map<String, Object> getDefaultProducerConfigs() {
    final HashMap<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return configs;
  }

  public static Map<String, Object> getDefaultConsumerConfigs(Map<String, Object> overrides) {
    final HashMap<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    configs.putAll(overrides);
    return configs;
  }

  public static String getBootstrapServers() {
    return String.join(
        ",",
        ContainerProperties.host("kafka0") + ":" + ContainerProperties.tcpPort("kafka0", 9092),
        ContainerProperties.host("kafka1") + ":" + ContainerProperties.tcpPort("kafka1", 9092),
        ContainerProperties.host("kafka2") + ":" + ContainerProperties.tcpPort("kafka2", 9092)
    );
  }
}
