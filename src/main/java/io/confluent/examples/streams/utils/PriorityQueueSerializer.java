package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

public class PriorityQueueSerializer<T> implements Serializer<PriorityQueue<T>> {

    private final Comparator<T> comparator;
    private final Serializer<T> valueSerializer;

    public PriorityQueueSerializer(final Comparator<T> comparator, final Serializer<T> valueSerializer) {
        this.comparator = comparator;
        this.valueSerializer = valueSerializer;
    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, PriorityQueue<T> queue) {
        final int size = queue.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);
        final Iterator<T> iterator = queue.iterator();
        try {
            out.writeInt(size);
            while (iterator.hasNext()) {
                final byte[] bytes = valueSerializer.serialize(topic, iterator.next());
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            out.close();
        } catch (IOException e) {
            throw new RuntimeException("unable to serialize PriorityQueue", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {

    }
}
