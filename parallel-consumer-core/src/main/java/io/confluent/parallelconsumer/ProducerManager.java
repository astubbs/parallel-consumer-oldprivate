package io.confluent.parallelconsumer;

import io.confluent.TransactionState;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class ProducerManager<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter<K, V> {

    protected final Producer<K, V> producer;
    private final ParallelConsumerOptions options;

    protected final TransactionState ts = new TransactionState();

    /**
     * The {@link KafkaProducer) isn't actually completely thread safe, at least when using it transactionally. We must
     * be careful not to send messages to the producer, while we are committing a transaction - "Cannot call send in
     * state COMMITTING_TRANSACTION".
     */
    private ReentrantReadWriteLock producerCommitLock;

    public ProducerManager(final Producer<K, V> newProducer, final Consumer<K, V> newConsumer, final WorkManager<K, V> wm, ParallelConsumerOptions options) {
        super(newConsumer, wm);
        this.producer = newProducer;
        this.options = options;

        initProducer(newProducer);
    }

    private void initProducer(final Producer<K, V> newProducer) {
        producerCommitLock = new ReentrantReadWriteLock(true);

        boolean producerIsActuallyTransactional = getProducerIsTransactional(newProducer);
//        String transactionIdProp = options.getProducerConfig().getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
//        boolean txIdSupplied = isBlank(transactionIdProp);
        if (options.isUsingTransactionalProducer()) {
            if (!producerIsActuallyTransactional) {
                throw new IllegalArgumentException("Using non-transactional option, yet Producer doesn't have a transaction ID - Producer needs a transaction id");
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
            if (producerIsActuallyTransactional) {
                throw new IllegalArgumentException("Not using transactional option, but Producer has a transaction ID - Producer must not have a transaction ID for this option");
            }
        }
    }

    /**
     * Nasty reflection but better than relying on user supplying their config
     *
     * @see ParallelEoSStreamProcessor#checkAutoCommitIsDisabled
     */
    @SneakyThrows
    private boolean getProducerIsTransactional(final Producer<K, V> newProducer) {
        Field coordinatorField = newProducer.getClass().getDeclaredField("transactionManager");
        coordinatorField.setAccessible(true);
        TransactionManager transactionManager = (TransactionManager) coordinatorField.get(newProducer);
        if (transactionManager == null) {
            return false;
        } else {
            return transactionManager.isTransactional();
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

        ReentrantReadWriteLock.ReadLock readLock = producerCommitLock.readLock();
        readLock.lock();
        Future<RecordMetadata> send;
        try {
            send = producer.send(outMsg, callback);
        } finally {
            readLock.unlock();
        }

        // wait on the send results
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

                    // lock the producer so other threads can't send messages
                    ReentrantReadWriteLock.WriteLock writeLock = producerCommitLock.writeLock();
                    writeLock.lock();
                    try {
                        producer.commitTransaction();
                    } finally {
                        writeLock.unlock();
                    }

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
