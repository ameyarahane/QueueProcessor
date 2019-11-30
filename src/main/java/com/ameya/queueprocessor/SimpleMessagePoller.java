package com.ameya.queueprocessor;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.daos.SqsAccessor;
import com.ameya.queueprocessor.util.SynchronizedBuffer;
import lombok.extern.log4j.Log4j2;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

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

        while (running.get()) {
            Collection<Message> messages = queueAccessor.getMessages(batchSize);
            boolean buffered = false;
            do {
                if (messages.isEmpty()) {
                    buffered = true;
                } else {
                    buffered = buffer.bufferElements(messages);
                }
                sleep();
            } while (!buffered);
        }
    }

    @Override
    public void start() {
        running.set(true);
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
