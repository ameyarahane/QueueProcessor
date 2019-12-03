package com.ameya.queueprocessor.processors;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.util.SynchronizedBuffer;
import lombok.extern.log4j.Log4j2;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class RunnableBatchProcessor implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 5;
    private final AtomicBoolean running;
    private final SynchronizedBuffer<Message> buffer;
    private final BatchMessageProcessor batchMessageProcessor;
    private final int sleepDelayMs;
    private final int batchSize;

    public RunnableBatchProcessor(final SynchronizedBuffer<Message> buffer,
                                  final BatchMessageProcessor batchMessageProcessor,
                                  final int sleepDelayMs, final int batchSize) {
        this.buffer = buffer;
        this.batchMessageProcessor = batchMessageProcessor;
        this.sleepDelayMs = sleepDelayMs;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.running = new AtomicBoolean(false);
    }

    public void start() {
        running.set(true);
    }

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {

        while (running.get()) {
            Collection<Message> messages = buffer.getElements(batchSize);
            if (messages.isEmpty()) {
                sleep();
            }

            try {
                batchMessageProcessor.processBatch(messages);
            } catch (RuntimeException e) {
                log.error("Thread {} encountered unhandled exception when processing messages: {}",
                        Thread.currentThread().getName(), e.getCause(), e);
                throw e;
            }
        }
    }

    private void sleep() {
        try {
            Thread.sleep(sleepDelayMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(Thread.currentThread().getName() + " was interrupted!", e);
        }
    }
}
