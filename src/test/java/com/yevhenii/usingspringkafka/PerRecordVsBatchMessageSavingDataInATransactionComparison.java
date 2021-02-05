package com.yevhenii.usingspringkafka;


import static org.assertj.core.api.Assertions.assertThat;


import com.yevhenii.usingspringkafka.payment.PaymentCreatedEvent;
import com.yevhenii.usingspringkafka.util.DataGenerator;
import com.yevhenii.usingspringkafka.util.Mapper;
import com.yevhenii.usingspringkafka.util.MessageSender;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import com.yevhenii.usingspringkafka.payment.Payment;
import com.yevhenii.usingspringkafka.payment.PaymentRepository;
import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.Topics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

/**
 * Results.
 *
 * <p>MAC (50k records).
 * Per record consumer: 163s/224s.
 * Batch consumer: 5s/8s.
 *
 * <p>Linux PC (50k records).
 * Per record consumer: 12s.
 * Batch consumer: 1s.
 *
 * <p>Linux PC (1mln records).
 * Per record consumer: 247s.
 * Batch consumer: 19s.
 */
@Slf4j
@SpringBootTest
@Import(PerRecordVsBatchMessageSavingDataInATransactionComparison.Cnf.class)
class PerRecordVsBatchMessageSavingDataInATransactionComparison {

  private static final int RECORDS = 50_000;

  @Autowired
  private Listener listener;

  @Autowired
  private BatchListener batchListener;

  @Autowired
  private PaymentRepository paymentRepository;

  @Test
  void test() throws Exception {
    NewTopic topic = Topics.rand();

    new MessageSender(topic.name(), DataGenerator.paymentCreatedEvents()).send(RECORDS);

    var batchConsumerElapsed = consumeAllMessages(topic.name(), batchListener);
    assertThat(paymentRepository.count()).isEqualTo(RECORDS);
    paymentRepository.deleteAll();

    var recordConsumerElapsed = consumeAllMessages(topic.name(), listener);
    assertThat(paymentRepository.count()).isEqualTo(RECORDS);
    paymentRepository.deleteAll();

    log.info("\n\n\nPer record consumer: {}s\nBatch consumer: {}s\n\n\n", recordConsumerElapsed / 1000, batchConsumerElapsed / 1000);
  }

  private static long consumeAllMessages(String topic, CountingListener listener) {
    var containerProps = new ContainerProperties(topic);
    containerProps.setMessageListener(listener);
    var container = SpringKafkaFactories.createContainer(containerProps, Names.randGroupId());

    var startTime = System.currentTimeMillis();
    var lastLogTime = startTime;

    log.info("Starting consuming: {}", listener.getClass().getSimpleName());
    container.start();

    while (listener.consumedCount() != RECORDS) {
      if (System.currentTimeMillis() - lastLogTime > 1000) {
        log.info("Consumed messages: {}", listener.consumedCount());
        lastLogTime = System.currentTimeMillis();
      }
    }
    var elapsed = System.currentTimeMillis() - startTime;
    container.stop();
    log.info("Finished: {}s", elapsed / 1000);
    return elapsed;
  }

  interface CountingListener {
    int consumedCount();
  }

  @TestConfiguration
  static class Cnf {

    @Autowired
    PaymentRepository paymentRepository;

    @Bean
    Listener listener() {
      return new Listener(paymentRepository);
    }

    @Bean
    BatchListener batchListener() {
      return new BatchListener(paymentRepository);
    }
  }

  @RequiredArgsConstructor
  static class Listener implements MessageListener<String, String>, CountingListener {

    private final AtomicInteger messagesProcessed = new AtomicInteger();

    private final PaymentRepository paymentRepository;

    @Override
    public void onMessage(ConsumerRecord<String, String> data) {
      messagesProcessed.incrementAndGet();
      try {
        PaymentCreatedEvent event = Mapper.toObject(data.value(), PaymentCreatedEvent.class);
        paymentRepository.save(event.getPayment());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int consumedCount() {
      return messagesProcessed.get();
    }
  }

  @RequiredArgsConstructor
  static class BatchListener implements BatchMessageListener<String, String>, CountingListener {

    private final AtomicInteger messagesProcessed = new AtomicInteger();

    private final PaymentRepository paymentRepository;

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data) {
      messagesProcessed.addAndGet(data.size());
      try {
        ArrayList<Payment> payments = new ArrayList<>(data.size());
        for (ConsumerRecord<String, String> r : data) {
          payments.add(Mapper.toObject(r.value(), PaymentCreatedEvent.class).getPayment());
        }
        paymentRepository.save(payments);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int consumedCount() {
      return messagesProcessed.get();
    }
  }
}
