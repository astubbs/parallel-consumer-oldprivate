package io.confluent.parallelconsumer;

public interface OffsetCommitter<K, V> {
    void commit();
}
