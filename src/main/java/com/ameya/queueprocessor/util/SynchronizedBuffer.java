package com.ameya.queueprocessor.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SynchronizedBuffer<T> implements Buffer<T> {

    private static final float BUFFER_FACTOR = 1.5f;
    private final Set<T> buffer;
    private final int approximateCapacity;

    public SynchronizedBuffer(final int approximateCapacity) {
        int initialCapacity = Math.round(approximateCapacity * BUFFER_FACTOR);
        this.buffer = Collections.synchronizedSet(new HashSet(initialCapacity));
        this.approximateCapacity = approximateCapacity;
    }

    /**
     * Adds elements to buffer if there is capacity. Either all or none of the elements will be added to buffer.
     *
     * @return true if elements were added to buffer successfully, else false.
     */
    @Override
    public boolean bufferElements(Collection<T> elements) {
        if (!hasCapacity()) {
            return false;
        }
        elements.addAll(elements);
        return true;
    }

    @Override
    public List<T> getElements(int size) {
        List<T> removed = buffer.stream().limit(size).collect(Collectors.toList());
        buffer.removeAll(removed);
        return removed;
    }

    private synchronized boolean hasCapacity() {
        return buffer.size() < approximateCapacity;
    }
}
