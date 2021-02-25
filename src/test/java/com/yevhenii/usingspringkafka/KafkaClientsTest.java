package com.yevhenii.usingspringkafka;

import static com.yevhenii.usingspringkafka.util.Defer.deferClose;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


import com.yevhenii.usingspringkafka.util.KafkaFactories;
import com.yevhenii.usingspringkafka.util.Defer;
import com.yevhenii.usingspringkafka.util.Topics;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.yevhenii.usingspringkafka.util.Names;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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

  @Test
  void callToPositionDoesNotCommitTheOffset() throws Exception {
    var topic = Topics.rand().name();
    var producer = deferClose(KafkaFactories.createProducer());
    producer.send(new ProducerRecord<>(topic, "value1")).get();

    var consumer = deferClose(KafkaFactories.createConsumer(Names.randGroupId()));
    consumer.subscribe(Collections.singletonList(topic));

    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
    assertEquals(1, records.count());
    assertEquals(1, consumer.assignment().size());
    TopicPartition partition = consumer.assignment().iterator().next();
    assertEquals(1, consumer.position(partition));

    Map<TopicPartition, OffsetAndMetadata> metadata = consumer.committed(consumer.assignment());
    assertTrue(metadata.containsKey(partition));
    assertNull(metadata.get(partition));
  }

  @Test
  void pollCanBeRetriedIfConsumerRebalanceListenerThrowsException() throws Exception {
    var topic = Topics.rand().name();
    var producer = deferClose(KafkaFactories.createProducer());
    producer.send(new ProducerRecord<>(topic, "value1")).get();

    var onPartitionsAssignedCalls = new AtomicInteger(0);
    List<Collection<TopicPartition>> assignHistory = new ArrayList<>();
    var consumer = deferClose(KafkaFactories.createConsumer(Names.randGroupId()));
    consumer.subscribe(
        Collections.singletonList(topic),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assignHistory.add(partitions);
            if (onPartitionsAssignedCalls.getAndIncrement() == 0) {
              throw new RuntimeException("oops");
            }
          }
        }
    );

    try {
      consumer.poll(Duration.ofSeconds(5));
    } catch (Exception x) {
      assertThat(x.getCause().getMessage()).isEqualTo("oops");
    }

    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
    assertThat(records.count()).isEqualTo(1);
    assertThat(records.iterator().next().value()).isEqualTo("value1");
    assertThat(onPartitionsAssignedCalls.get()).isEqualTo(2);
    assertThat(assignHistory.get(0).size()).isEqualTo(1);
    assertThat(assignHistory.get(1)).isEmpty();
  }

  private static void sleep(int ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException x) {
      throw new RuntimeException(x.getMessage(), x);
    }
  }
}
