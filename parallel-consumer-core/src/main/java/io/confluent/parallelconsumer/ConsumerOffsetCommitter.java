package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.stats.Count;

import java.nio.file.ProviderMismatchException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class ConsumerOffsetCommitter<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter<K, V> {

    @Getter
    private final boolean isSync = false;

    private CountDownLatch commitBarrier = new CountDownLatch(1);
    CyclicBarrier b = new CyclicBarrier(2);
    ReentrantLock lock = new ReentrantLock(false);

    public ConsumerOffsetCommitter(final Consumer<K, V> newConsumer, final WorkManager<K, V> newWorkManager) {
        super(newConsumer, newWorkManager);
    }

    @Override
    protected void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) {
        if (isSync) {
            consumer.commitSync(offsetsToSend);
        } else {
            consumer.commitAsync(offsetsToSend, (offsets, exception) -> {
                if (exception == null) {
                    log.error("Error committing offsets", exception);
                }
                commitBarrier.countDown();
            });
        }
    }

    void waitForCommit() {
        while (commitBarrier.getCount() > 0) {
            try {
                commitBarrier.await();
            } catch (InterruptedException e) {
                log.warn(e.getLocalizedMessage(), e);
            }
        }
    }

    public CountDownLatch setupLock() {
        commitBarrier = new CountDownLatch(1);
//        Condition commitFinished = lock.newCondition();
//        commitFinished.signalAll();
//        commitFinished.await();
        return commitBarrier;
    }
}
