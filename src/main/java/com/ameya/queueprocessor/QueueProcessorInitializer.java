package com.ameya.queueprocessor;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.configuration.Configuration;
import com.ameya.queueprocessor.daos.SqsAccessor;
import com.ameya.queueprocessor.handlers.FailureHandler;
import com.ameya.queueprocessor.handlers.SuccessHandler;
import com.ameya.queueprocessor.pollers.MessagePoller;
import com.ameya.queueprocessor.pollers.RunnableMessagePoller;
import com.ameya.queueprocessor.pollers.impl.SimpleMessagePoller;
import com.ameya.queueprocessor.processors.BatchMessageProcessor;
import com.ameya.queueprocessor.processors.RunnableBatchProcessor;
import com.ameya.queueprocessor.processors.impl.SimpleBatchMessageProcessor;
import com.ameya.queueprocessor.util.SynchronizedBuffer;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class that initializes the queue poller and processor threads based on configured values.
 */
@Log4j2
public class QueueProcessorInitializer {

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final QueueProcessor processor;

    public QueueProcessorInitializer(final SqsAccessor queueAccessor,
                                     final SuccessHandler successHandler,
                                     final FailureHandler failureHandler) {
        this.processor = initializeQueueProcessor(queueAccessor, successHandler, failureHandler);
    }

    /**
     * Entry point for starting the execution process after the configuration has been loaded. This can be called
     * from the main method after the DI process is completed.
     */
    public boolean start() {
        log.info("Starting the QueueProcessor.");

        if (isInitialized.get()) {
            log.info("QueueProcessor is already initialized. Nothing to do.");
            return true;
        }

        try {
            processor.start();
        } catch (RuntimeException e) {
            log.error("Exception attempting to start QueueProcessor:[{}], stopping.", e.getMessage(), e);
            processor.stop();
            throw e;
        }
        return true;
    }

    /**
     * Stop processing the messages.
     */
    public boolean stop() {
        log.info("Terminating the QueueProcessor.");
        processor.stop();
        isInitialized.set(false);
        return true;
    }

    private QueueProcessor initializeQueueProcessor(final SqsAccessor queueAccessor,
                                                    final SuccessHandler successHandler,
                                                    final FailureHandler failureHandler) {
        List<RunnableMessagePoller> pollers = new ArrayList<>();
        List<RunnableBatchProcessor> processors = new ArrayList<>();

        // There will be multiple pollers, each with its own buffer to store messages
        // Each poller will also have multiple message processor threads that read from the buffer and process messages
        for (int i = 0; i < Configuration.NUM_POLLERS; i++) {
            SynchronizedBuffer<Message> buffer = createBuffer();
            pollers.add(createPoller(queueAccessor, buffer));
            processors.addAll(createProcessors(successHandler, failureHandler, buffer));
        }

        return new QueueProcessor(pollers, processors);
    }

    /**
     * Create processor runnables that all use the shared synchronized buffer to read messages from.
     */
    private List<RunnableBatchProcessor> createProcessors(final SuccessHandler successHandler,
                                                          final FailureHandler failureHandler,
                                                          final SynchronizedBuffer<Message> buffer) {
        List<RunnableBatchProcessor> processorThreads = new ArrayList<>();
        for (int i = 0; i < Configuration.NUM_PROCESSORS; i++) {
            BatchMessageProcessor processor = new SimpleBatchMessageProcessor(successHandler, failureHandler);
            RunnableBatchProcessor processorThread = new RunnableBatchProcessor(buffer, processor,
                    Configuration.POLLING_DELAY_MS, Configuration.BATCH_SIZE);
            processorThreads.add(processorThread);
        }
        return processorThreads;
    }

    /**
     * Create a runnable poller instance using the synchronized buffer where the poller will store messages.
     */
    private RunnableMessagePoller createPoller(final SqsAccessor queueAccessor,
                                               final SynchronizedBuffer<Message> buffer) {
        MessagePoller poller = new SimpleMessagePoller(queueAccessor, Configuration.POLLING_DELAY_MS, buffer,
                Configuration.BATCH_SIZE);
        return new RunnableMessagePoller(poller);
    }

    /**
     * Create a synchronized buffer to store messages temporarily after they've been read from the queue.
     */
    private SynchronizedBuffer<Message> createBuffer() {
        return new SynchronizedBuffer<>(Configuration.BUFFER_CAPACITY);
    }
}
