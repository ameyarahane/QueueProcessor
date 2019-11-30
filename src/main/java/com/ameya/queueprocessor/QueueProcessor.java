package com.ameya.queueprocessor;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.configuration.Configuration;
import com.ameya.queueprocessor.daos.SqsAccessor;
import com.ameya.queueprocessor.processors.BatchMessageProcessor;
import com.ameya.queueprocessor.processors.BatchProcessorRunnable;
import com.ameya.queueprocessor.processors.impl.SimpleBatchMessageProcessor;
import com.ameya.queueprocessor.util.SynchronizedBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueProcessor {

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final BatchProcessingSuccessHandler successHandler;
    private final BatchProcessingFailureHandler failureHandler;
    private final SqsAccessor queueAccessor;

    public QueueProcessor(final SqsAccessor queueAccessor,
                          final BatchProcessingSuccessHandler successHandler,
                          final BatchProcessingFailureHandler failureHandler) {
        this.queueAccessor = queueAccessor;
        this.successHandler = successHandler;
        this.failureHandler = failureHandler;
    }

    public void start() {

        if (isInitialized.get()) {
            return;
        }

        List<MessagePollerRunnable> pollers = new ArrayList<>();
        List<BatchProcessorRunnable> processors = new ArrayList<>();

        for (int i = 0; i < Configuration.NUM_POLLERS; i++) {
            SynchronizedBuffer<Message> buffer = createBuffer();
            pollers.add(createPoller(buffer));
            processors.addAll(createProcessors(buffer));
        }

        Executor processor = new Executor(pollers, processors);
        try {
            processor.initialize();
        } catch (RuntimeException e) {
            processor.stopProcessing();
        }

    }

    private List<BatchProcessorRunnable> createProcessors(final SynchronizedBuffer<Message> buffer) {
        List<BatchProcessorRunnable> processorThreads = new ArrayList<>();
        for (int i = 0; i < Configuration.NUM_PROCESSORS; i++) {
            BatchMessageProcessor processor = new SimpleBatchMessageProcessor(successHandler, failureHandler);
            BatchProcessorRunnable processorThread = new BatchProcessorRunnable(buffer, processor,
                    Configuration.POLLING_DELAY_MS, Configuration.BATCH_SIZE);
            processorThreads.add(processorThread);
        }
        return processorThreads;
    }

    private MessagePollerRunnable createPoller(final SynchronizedBuffer<Message> buffer) {
        MessagePoller poller = new SimpleMessagePoller(queueAccessor, Configuration.POLLING_DELAY_MS, buffer,
                Configuration.BATCH_SIZE);
        return new MessagePollerRunnable(poller);
    }

    private SynchronizedBuffer<Message> createBuffer() {
        return new SynchronizedBuffer<>(Configuration.BUFFER_CAPACITY);
    }
}
