package com.yevhenii.usingspringkafka.ext;

import com.yevhenii.usingspringkafka.util.Defer;
import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import com.yevhenii.usingspringkafka.util.Topics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.awaitility.Awaitility.await;

@ExtendWith(Defer.class)
public class RelativeOffsetAssigningRebalanceListenerTest {

    @Test
    void resetsOffsetsToBeginningOrLatestMinusSpecifiedDuration() throws Exception {
        String topic = Topics.rand(3, 1).name();

        KafkaTemplate<String, String> kafkaTemplate = SpringKafkaFactories.createTemplate();
        Instant now = Instant.now();
        kafkaTemplate.send(topic, 0, now.minus(Duration.ofDays(10)).toEpochMilli(), "key", "1"); // older, won't be consumed
        kafkaTemplate.send(topic, 0, now.minus(Duration.ofDays(3)).toEpochMilli(), "key", "2");
        kafkaTemplate.send(topic, 0, now.minus(Duration.ofDays(2)).toEpochMilli(), "key", "3");
        kafkaTemplate.send(topic, 1, now.minus(Duration.ofDays(7)).toEpochMilli(), "key", "4"); // older, won't be consumed
        kafkaTemplate.send(topic, 1, now.minus(Duration.ofDays(6)).toEpochMilli(), "key", "5"); // older, won't be consumed
        kafkaTemplate.send(topic, 1, now.minus(Duration.ofDays(2)).toEpochMilli(), "key", "6");
        kafkaTemplate.send(topic, 1, now.minus(Duration.ofDays(1)).toEpochMilli(), "key", "7");
        kafkaTemplate.send(topic, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "8");
        kafkaTemplate.send(topic, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "9");
        kafkaTemplate.send(topic, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "10");

        CopyOnWriteArraySet<String> messages = new CopyOnWriteArraySet<>();
        ContainerProperties containerProps = new ContainerProperties(topic);
        containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
            messages.add(message.value());
        });
        containerProps.setConsumerRebalanceListener(new RelativeOffsetAssigningRebalanceListener(Duration.ofDays(4).negated()));
        KafkaMessageListenerContainer<String, String> container = SpringKafkaFactories.createContainer(containerProps, Names.randGroupId());
        container.start();
        Defer.defer(container::stop);

        await().until(() -> messages.equals(Set.of("2", "3", "6", "7", "8", "9", "10")));
    }
}
