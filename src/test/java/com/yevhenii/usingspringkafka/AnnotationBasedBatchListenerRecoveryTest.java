package com.yevhenii.usingspringkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.yevhenii.usingspringkafka.util.Names;
import com.yevhenii.usingspringkafka.util.SpringKafkaFactories;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.RecoveringBatchErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@SpringBootTest
@Slf4j
public class AnnotationBasedBatchListenerRecoveryTest {

    private static final String T1 = "topic-annotation-based-batch-listener-recovery-1";

    @Autowired
    private BatchListener listener;

    @Autowired
    private Dlq dlq;

    @Autowired
    @Qualifier(BatchListener.CONTAINER_GROUP)
    private List<ConcurrentMessageListenerContainer<?, ?>> containers;

    @Test
    void brokenMessageEndsUpInDql() {
        KafkaTemplate<String, String> template = SpringKafkaFactories.createTemplate();
        template.setDefaultTopic(T1);
        template.sendDefault("1", "x1");
        template.sendDefault("2", "x2");
        template.sendDefault("3", "x3");
        template.sendDefault("4", "x4");
        template.flush();

        // start listener and check if it works
        startListener();
        await().until(() -> listener.messages.size() == 4);

        // since it does, let's stop it to make sure that we poll messages in batch next time
        stopListener();

        // sending a batch that includes a corrupted message
        template.sendDefault("1", "x5");
        template.sendDefault("2", "x6");
        template.sendDefault("3", "x7");
        template.sendDefault("4", "x8");
        template.flush();

        // start the listener again so it can consume a new batch
        startListener();

        await().until(() -> dlq.messages.size() == 1);
        assertThat(dlq.messages).containsExactly("x7");

        await().until(() -> listener.messages.size() == 7);
        assertThat(listener.messages).containsExactly("x1", "x2", "x3", "x4", "x5", "x6" ,"x8");
    }

    private void startListener() {
        containers.get(0).start();
    }

    private void stopListener() {
        containers.get(0).stop(true);
    }

    @TestConfiguration
    static class Cnf {

        @Bean
        BatchListener listener() {
            return new BatchListener();
        }

        @Bean
        Dlq dlq() {
            return new Dlq();
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            factory.setBatchListener(true);
            factory.setBatchErrorHandler(new RecoveringBatchErrorHandler(dlq(), new FixedBackOff(100, 2)));
            return factory;
        }

        @Bean
        ConsumerFactory<String, String> consumerFactory() {
            return SpringKafkaFactories.createConsumerFactory(Names.randGroupId());
        }
    }

    static class Dlq implements BiConsumer<ConsumerRecord<?, ?>, Exception> {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception x) {
            log.info("DLQ received: " + record);
            messages.add((String) record.value());
        }
    }

    @Slf4j
    static class BatchListener {

        private static final String CONTAINER_GROUP = "batch-recovery-test-listener-1";

        private final Set<String> messages = new CopyOnWriteArraySet<>();

        @KafkaListener(topics = {T1}, autoStartup = "false", containerGroup = CONTAINER_GROUP)
        void consume(List<ConsumerRecord<String, String>> batch) {
            for (ConsumerRecord<String, String> record : batch) {
                if (record.value().equals("x7")) {
                    throw new BatchListenerFailedException("oops", record);
                }
                messages.add(record.value());
            }
        }
    }
}
