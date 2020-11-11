package io.confluent.parallelconsumer;

import io.confluent.TransactionState;
import io.confluent.csid.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Future;

import static io.confluent.csid.utils.StringUtils.isBlank;
import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class ProducerManager<K, V> extends AbstractOffsetCommitter<K, V> {

    protected final Producer<K, V> producer;
    private final ParallelConsumerOptions options;

    protected final TransactionState ts = new TransactionState();

    public ProducerManager(final Producer<K, V> newProducer, final Consumer<K, V> newConsumer, final WorkManager<K, V> wm, ParallelConsumerOptions options) {
        super(newConsumer, wm);
        this.producer = newProducer;
        this.options = options;

        String transactionIdProp = options.getProducerConfig().getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        if (options.isUsingTransactionalProducer()) {
            if (isBlank(transactionIdProp)) {
                throw new IllegalArgumentException("Producer needs a transaction id");
            }
            try {
                log.debug("Initialising producer transaction session...");
                producer.initTransactions();
                producer.beginTransaction();
                ts.setInTransaction();
            } catch (KafkaException e) {
                log.error("Make sure your producer is setup for transactions - specifically make sure it's {} is set.", ProducerConfig.TRANSACTIONAL_ID_CONFIG, e);
                throw e;
            }
        } else {
            if (!isBlank(transactionIdProp)) {
                throw new IllegalArgumentException("Producer must not have a transaction id");
            }
        }
    }

    /**
     * Produce a message back to the broker.
     * <p>
     * Implementation uses the blocking API, performance upgrade in later versions, is not an issue for the more common
     * use case where messages aren't produced.
     *
     * @see ParallelConsumer#poll
     * @see ParallelStreamProcessor#pollAndProduce
     */
    RecordMetadata produceMessage(ProducerRecord<K, V> outMsg) {
        // only needed if not using tx
        Callback callback = (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("Error producing result message", exception);
                throw new RuntimeException("Error producing result message", exception);
            }
        };
        Future<RecordMetadata> send = producer.send(outMsg, callback);
        try {
            return send.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) {
        if (!options.isUsingTransactionalProducer()) {
            throw new IllegalStateException("Bug: cannot use if not using transactional producer");
        }

        producer.sendOffsetsToTransaction(offsetsToSend, groupMetadata);
        // see {@link KafkaProducer#commit} this can be interrupted and is safe to retry
        boolean committed = false;
        int retryCount = 0;
        int arbitrarilyChosenLimitForArbitraryErrorSituation = 200;
        Exception lastErrorSavedForRethrow = null;
        while (!committed) {
            if (retryCount > arbitrarilyChosenLimitForArbitraryErrorSituation) {
                String msg = msg("Retired too many times ({} > limit of {}), giving up. See error above.", retryCount, arbitrarilyChosenLimitForArbitraryErrorSituation);
                log.error(msg, lastErrorSavedForRethrow);
                throw new RuntimeException(msg, lastErrorSavedForRethrow);
            }
            try {
                if (producer instanceof MockProducer) {
                    // see bug https://issues.apache.org/jira/browse/KAFKA-10382
                    // KAFKA-10382 - MockProducer is not ThreadSafe, ideally it should be as the implementation it mocks is
                    synchronized (producer) {
                        producer.commitTransaction();
                    }
                } else {
                    producer.commitTransaction();
                }

                ts.setNotInTransaction();

                onOffsetCommitSuccess(offsetsToSend);

                committed = true;
                if (retryCount > 0) {
                    log.warn("Commit success, but took {} tries.", retryCount);
                }
            } catch (Exception e) {
                log.warn("Commit exception, will retry, have tried {} times (see KafkaProducer#commit)", retryCount, e);
                lastErrorSavedForRethrow = e;
                retryCount++;
            }
        }
    }

    @Override
    protected void afterCommit() {
        // begin tx for next cycle
        producer.beginTransaction();
        ts.setInTransaction();
    }

    public void close(final Duration timeout) {
        log.debug("Closing producer, assuming no more in flight...");
        if (options.isUsingTransactionalProducer() && ts.isInTransaction()) {
            // close started after tx began, but before work was done, otherwise a tx wouldn't have been started
            producer.abortTransaction();
        }
        producer.close(timeout);
    }
}
