package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.integrationTests.KafkaTest;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
public class Bug25AppTest extends KafkaTest<String, String> {

    @SneakyThrows
    @Test
    public void testTransactional() {
        boolean tx = true;
        AppUnderTest coreApp = new AppUnderTest(tx, ParallelConsumerOptions.builder().ordering(KEY).usingTransactionalProducer(tx).build());

        ensureTopic(coreApp.inputTopic, 1);
        ensureTopic(coreApp.outputTopic, 1);

        log.info("Producing 1000 messages before starting application");
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < 1000; i++) {
                kafkaProducer.send(new ProducerRecord<>(coreApp.inputTopic, "key-" + i, "value-" + i));
            }
        }

        log.info("Starting application...");
        coreApp.runPollAndProduce();

        waitAtMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            log.info("Processed-count: " + coreApp.messagesProcessed.get());
            log.info("Produced-count: " + coreApp.messagesProduced.get());
            assertThat(coreApp.messagesProcessed.get()).isEqualTo(1000);
            assertThat(coreApp.messagesProduced.get()).isEqualTo(1000);
        });

        coreApp.close();
    }

    @SneakyThrows
    @Test
    public void testNonTransactional() {
        boolean tx = false;
        AppUnderTest coreApp = new AppUnderTest(tx, ParallelConsumerOptions.builder().ordering(KEY).usingTransactionalProducer(tx).build());

        ensureTopic(coreApp.inputTopic, 1);
        ensureTopic(coreApp.outputTopic, 1);

        log.info("Producing 1000 messages before starting application");
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < 1000; i++) {
                kafkaProducer.send(new ProducerRecord<>(coreApp.inputTopic, "key-" + i, "value-" + i));
            }
        }

        log.info("Starting application...");
        coreApp.runPollAndProduce();

        waitAtMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            log.info("Processed-count: " + coreApp.messagesProcessed.get());
            log.info("Produced-count: " + coreApp.messagesProduced.get());
            assertThat(coreApp.messagesProcessed.get()).isEqualTo(1000);
            assertThat(coreApp.messagesProduced.get()).isEqualTo(1000);
        });

        coreApp.close();
    }

    @RequiredArgsConstructor
    class AppUnderTest extends CoreApp {

        final boolean tx;
        final ParallelConsumerOptions options;

        @Override
        Consumer<String, String> getKafkaConsumer() {
            Properties props = kcu.props;
//            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1); // Sometimes causes test to fail (default 500)
//            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
            return new KafkaConsumer<>(props);
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            return kcu.createNewProducer(tx);
        }

        @Override
        ParallelConsumerOptions getOptions() {
            return options;
        }
    }
}
