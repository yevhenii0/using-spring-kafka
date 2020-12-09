package com.yevhenii.usingspringkafka.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class MessageSender {

  private final String topic;
  private final DataGenerator<?> generator;
  private final KafkaTemplate<String, String> template;

  public MessageSender(String topic, DataGenerator<?> generator) {
    this.topic = topic;
    this.generator = generator;
    this.template = SpringKafkaFactories.createTemplate();
  }

  public void send(int num) {
    long started = System.currentTimeMillis();

    for (int i = 0; i < num; i++) {
      template.send(topic, Mapper.toJson(generator.gen(i)));
      if (i % 5000 == 0) {
        template.flush();
      }
    }
    template.flush();

    log.info("Sent: {} in {}ms", num, System.currentTimeMillis() - started);
  }
}
