package com.yevhenii.usingspringkafka;

import static com.yevhenii.usingspringkafka.util.Defer.deferClose;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


import com.yevhenii.usingspringkafka.util.KafkaFactories;
import com.yevhenii.usingspringkafka.util.Defer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.yevhenii.usingspringkafka.util.Names;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(Defer.class)
class KafkaClientsTest {

  @Test
  void availableMessageIsInstantlyConsumedDespiteTheMaxPollRecords() throws Exception {
    var producer = deferClose(KafkaFactories.createProducer());
    producer.send(new ProducerRecord<>("topic1", "value1")).get();

    var consumer = deferClose(KafkaFactories.createConsumer(Map.of(
        ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100
    )));
    consumer.subscribe(Collections.singletonList("topic1"));

    new Thread(() -> {
      sleep(2000);

      try {
        producer.send(new ProducerRecord<>("topic1", "value2")).get();
      } catch (InterruptedException | ExecutionException x) {
        throw new RuntimeException(x.getMessage(), x);
      }

    }).start();

    var numOfPolls = new AtomicInteger(0);
    var values = new HashSet<>();
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
      for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(20))) { // <- long duration
        values.add(record.value());
        numOfPolls.incrementAndGet();
      }
      return values.equals(Set.of("value1", "value2"));
    });

    assertEquals(numOfPolls.get(), 2);
  }

  @Test
  void availableMessageIsInstantlyConsumedDespiteTheMaxPollRecordsAndMinFetchSizeWhenFetchWaitMsIsLow() throws Exception {
    var producer = deferClose(KafkaFactories.createProducer());
    producer.send(new ProducerRecord<>("topic2", "value1")).get();

    var consumer = deferClose(KafkaFactories.createConsumer(Map.of(
        ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100,
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048 * 8,
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100
    )));
    consumer.subscribe(Collections.singletonList("topic2"));

    new Thread(() -> {
      sleep(2000);

      try {
        producer.send(new ProducerRecord<>("topic2", "value2")).get();
      } catch (InterruptedException | ExecutionException x) {
        throw new RuntimeException(x.getMessage(), x);
      }

    }).start();

    var numOfPolls = new AtomicInteger(0);
    var values = new HashSet<>();
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
      for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(20))) { // <- long duration
        values.add(record.value());
        numOfPolls.incrementAndGet();
      }
      return values.equals(Set.of("value1", "value2"));
    });

    assertEquals(numOfPolls.get(), 2);
  }

  @Test
  void pollIsWaitingForFetchMsWhenSizeIsLowerThanMinBytes() throws Exception {
    var topic = Names.randTopic();
    var producer = deferClose(KafkaFactories.createProducer());
    producer.send(new ProducerRecord<>(topic, "value1")).get();

    var consumer = deferClose(KafkaFactories.createConsumer(Map.of(
        ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100,
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048 * 8,
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 5000
    )));
    consumer.subscribe(List.of(topic));

    assertThrows(
        ConditionTimeoutException.class,
        () -> Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> {
          consumer.poll(Duration.ofSeconds(20));
          return true;
        })
    );
  }

  @Test
  void pollReturnsWhenDurationPassesNotWaitingForFetchTimeout() throws Exception {
    var producer = deferClose(KafkaFactories.createProducer());
    producer.send(new ProducerRecord<>("topic4", "value1")).get();

    var consumer = deferClose(KafkaFactories.createConsumer(Map.of(
        ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100,
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048 * 8,
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 5000
    )));
    consumer.subscribe(Collections.singletonList("topic4"));

    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> {
      consumer.poll(Duration.ofSeconds(1));
      return true;
    });
  }

  private static void sleep(int ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException x) {
      throw new RuntimeException(x.getMessage(), x);
    }
  }
}
