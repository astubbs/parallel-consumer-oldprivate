package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.KafkaTest;
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

@Slf4j
public class Bug25AppTest extends KafkaTest<String, String> {

    @SneakyThrows
    @Test
    public void test() {
        log.info("Test start");
        ensureTopic(CoreApp.inputTopic, 1);
        ensureTopic(CoreApp.outputTopic, 1);


        AppUnderTest coreApp = new AppUnderTest();

        log.info("Producing 1000 messages before starting application");
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < 1000; i++) {
                kafkaProducer.send(new ProducerRecord<>(CoreApp.inputTopic, "key-" + i, "value-" + i));
            }
        }

        log.info("Starting application...");
        coreApp.runPollAndProduce();

        Awaitility.waitAtMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            log.info("Processed-count: " + coreApp.messagesProcessed.get());
            log.info("Produced-count: " + coreApp.messagesProduced.get());
            Assertions.assertThat(coreApp.messagesProcessed.get()).isEqualTo(1000);
            Assertions.assertThat(coreApp.messagesProduced.get()).isEqualTo(1000);
        });

        coreApp.close();
    }

    class AppUnderTest extends CoreApp {

        @Override
        Consumer<String, String> getKafkaConsumer() {
            Properties props = kcu.props;
//            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1); // Sometimes causes test to fail
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
            return new KafkaConsumer<>(props);
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            return kcu.createNewProducer(true);
        }
    }
}
