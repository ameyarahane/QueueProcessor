package com.ameya.queueprocessor.pollers.impl;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.daos.SqsAccessor;
import com.ameya.queueprocessor.pollers.MessagePoller;
import com.ameya.queueprocessor.util.SynchronizedBuffer;
import lombok.extern.log4j.Log4j2;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A message poller, which should ideally be run in its own thread so that it can be stopped from the calling thread.
 */
@Log4j2
public class SimpleMessagePoller implements MessagePoller {

    private static final int DEFAULT_BATCH_SIZE = 5;
    private final SqsAccessor queueAccessor;
    private final int pollDelayMs;
    private final SynchronizedBuffer buffer;
    private final AtomicBoolean running;
    private final int batchSize;

    public SimpleMessagePoller(final SqsAccessor queueAccessor, final int pollDelayMs,
                               final SynchronizedBuffer buffer, final int batchSize) {
        this.queueAccessor = queueAccessor;
        this.pollDelayMs = pollDelayMs;
        this.buffer = buffer;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.running = new AtomicBoolean(false);
    }

    @Override
    public void poll() {

        running.set(true);

        while (running.get()) {
            Collection<Message> messages = queueAccessor.getMessages(batchSize);
            boolean buffered = false;
            /* Internal loop that runs to test that any polled messages are being buffered.
             * There can be additional logic to ensure that the buffer isn't constantly full and there can be metrics
             *  that get published that talk about rate of messages being processed, or the time for which the buffer
             *  was running full, etc. so that alarming can be build on top of it.
             */
            do {
                if (messages.isEmpty()) {
                    buffered = true;
                } else {
                    buffered = buffer.addToBuffer(messages);
                }
                sleep();
            } while (!buffered);
        }
    }

    @Override
    public void stop() {
        running.set(false);
    }

    private void sleep() {
        try {
            Thread.sleep(pollDelayMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format("Thread %s interrupted!", Thread.currentThread().getName(), e));
        }
    }

}
