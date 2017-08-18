package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

public class PriorityQueueDeserializer<T> implements Deserializer<PriorityQueue<T>> {

    private final Comparator<T> comparator;
    private final Deserializer<T> valueDeserializer;

    public PriorityQueueDeserializer(final Comparator<T> comparator, final Deserializer<T> valueDeserializer) {
        this.comparator = comparator;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public PriorityQueue<T> deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        final PriorityQueue<T> priorityQueue = new PriorityQueue<>(comparator);
        final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            final int records = dataInputStream.readInt();
            for (int i = 0; i < records; i++) {
                final byte[] valueBytes = new byte[dataInputStream.readInt()];
                dataInputStream.read(valueBytes);
                priorityQueue.add(valueDeserializer.deserialize(s, valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize PriorityQueue", e);
        }
        return priorityQueue;
    }

    @Override
    public void close() {

    }
}
