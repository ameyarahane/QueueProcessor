package com.ameya.queueprocessor;

import com.ameya.queueprocessor.configuration.Configuration;
import com.ameya.queueprocessor.processors.BatchProcessorRunnable;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class Executor {

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final List<MessagePollerRunnable> pollers;
    private final List<BatchProcessorRunnable> processors;
    private final ExecutorService executorService;

    public Executor(final List<MessagePollerRunnable> pollers,
                    final List<BatchProcessorRunnable> processors) {
        this.processors = processors;
        this.pollers = pollers;
        int nThreads = Configuration.NUM_POLLERS * Configuration.NUM_PROCESSORS;
        this.executorService = Executors.newFixedThreadPool(nThreads);
    }

    public void initialize() {

        if (isInitialized.get()) {
            return;
        }

        pollers.forEach(MessagePollerRunnable::start);
        pollers.forEach(executorService::execute);
        processors.forEach(BatchProcessorRunnable::start);
        processors.forEach(executorService::execute);

        isInitialized.set(true);
    }

    public void stopProcessing() {
        pollers.forEach(MessagePollerRunnable::stop);
        processors.forEach(BatchProcessorRunnable::stop);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.info("Shutdown was interrupted, shutting down now.");
            executorService.shutdownNow();
        }
    }
}
