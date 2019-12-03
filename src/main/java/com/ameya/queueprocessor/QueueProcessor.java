package com.ameya.queueprocessor;

import com.ameya.queueprocessor.configuration.Configuration;
import com.ameya.queueprocessor.pollers.RunnableMessagePoller;
import com.ameya.queueprocessor.processors.RunnableBatchProcessor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements the start / stop operations on the pollers and processors that process messages from the SQS queue.
 */
@Log4j2
public class QueueProcessor {

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final List<RunnableMessagePoller> pollers;
    private final List<RunnableBatchProcessor> processors;
    private final ExecutorService executorService;

    /**
     * Construct a QueueProcessor instance with threads equal to the number of pollers * number of processors.
     */
    public QueueProcessor(final List<RunnableMessagePoller> pollers,
                          final List<RunnableBatchProcessor> processors) {
        this(pollers, processors,
                Executors.newFixedThreadPool(Configuration.NUM_POLLERS * Configuration.NUM_PROCESSORS));
    }

    public QueueProcessor(@NonNull final List<RunnableMessagePoller> pollers,
                          @NonNull final List<RunnableBatchProcessor> processors,
                          @NonNull final ExecutorService executorService) {
        this.processors = processors;
        this.pollers = pollers;
        this.executorService = executorService;
    }

    /**
     * Starts the polling and processing of SQS messages from a queue.
     */
    public void start() {

        if (isInitialized.get()) {
            return;
        }

        pollers.forEach(RunnableMessagePoller::start);
        pollers.forEach(executorService::execute);
        processors.forEach(RunnableBatchProcessor::start);
        processors.forEach(executorService::execute);

        isInitialized.set(true);
    }

    /**
     * Stop polling and processing messages from the queue and shut down the queue processor.
     */
    public void stop() {
        pollers.forEach(RunnableMessagePoller::stop);
        processors.forEach(RunnableBatchProcessor::stop);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.info("Shutdown was interrupted, shutting down now.");
            executorService.shutdownNow();
        }

        isInitialized.set(false);
    }
}
