package com.yevhenii.usingspringkafka;

import com.yevhenii.usingspringkafka.util.KafkaFactories;
import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import com.yevhenii.usingspringkafka.util.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import(AnnotationBasedListenerManualAcknowledgmentTest.Cnf.class)
class AnnotationBasedListenerManualAcknowledgmentTest {

    private static final String T1 = "topic-annotation-based-listener-man-ack-1";

    @Autowired
    private Listener listener;

    @AfterEach
    void cleanup() throws Exception {
        Topics.remove(T1);
        listener.messagesConsumed.clear();
        listener.oneTimeNAckPredicate.set(null);
    }

    @Test
    void allMessagesConsumed() {
        KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
        template.setDefaultTopic(T1);
        template.sendDefault("x1");
        template.sendDefault("x2");
        template.sendDefault("x3");
        template.sendDefault("x4");

        await().until(() -> listener.messagesConsumed.size() == 4);
        assertThat(listener.messagesConsumed).containsExactly("x1", "x2", "x3", "x4");
    }

    @Test
    void nAckTriggersMessageToBeRedelivered() {
        AtomicInteger x2MessageDeliveredTimes = new AtomicInteger();
        listener.oneTimeNAckPredicate.set(message -> message.equals("x2") && x2MessageDeliveredTimes.incrementAndGet() < 3);

        KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
        template.setDefaultTopic(T1);
        template.sendDefault("x1");
        template.sendDefault("x2");
        template.sendDefault("x3");
        template.sendDefault("x4");

        await().until(() -> listener.messagesConsumed.size() == 6);
        assertThat(listener.messagesConsumed).isEqualTo(List.of("x1", "x2", "x2", "x2", "x3", "x4"));
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
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
            return factory;
        }

        @Bean
        ConsumerFactory<String, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(
                    KafkaFactories.getDefaultConsumerConfigs(
                            Map.of(
                                    ConsumerConfig.GROUP_ID_CONFIG, Names.randGroupId(),
                                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2
                            )
                    )
            );
        }
    }

    static class Listener {

        private final List<String> messagesConsumed = new CopyOnWriteArrayList<>();
        private final AtomicReference<Predicate<String>> oneTimeNAckPredicate = new AtomicReference<>();

        @KafkaListener(topics = T1)
        void consume(String message, Acknowledgment acknowledgment) {
            messagesConsumed.add(message);
            if (oneTimeNAckPredicate.get() != null && oneTimeNAckPredicate.get().test(message)) {
                acknowledgment.nack(1000);
            } else {
                acknowledgment.acknowledge();
            }
        }
    }
}

