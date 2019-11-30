package com.ameya.queueprocessor.util;

import java.util.Collection;
import java.util.List;

public interface Buffer<T> {

    /**
     * Adds elements to buffer if there is capacity. Either all or none of the elements will be added to buffer.
     *
     * @return true if elements were added to buffer successfully, else false.
     */
    public boolean bufferElements(Collection<T> elements);

    public List<T> getElements(int size);
}
