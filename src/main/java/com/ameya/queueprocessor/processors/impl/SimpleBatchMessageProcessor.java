package com.ameya.queueprocessor.processors.impl;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.BatchProcessingFailureHandler;
import com.ameya.queueprocessor.BatchProcessingSuccessHandler;
import lombok.NonNull;

/**
 * A simple batch message processor that processes
 */
public class SimpleBatchMessageProcessor extends AbstractBatchMessageProcessor {

    public SimpleBatchMessageProcessor(@NonNull final BatchProcessingSuccessHandler successHandler,
                                       @NonNull final BatchProcessingFailureHandler failureHandler) {
        super(successHandler, failureHandler);
    }

    @Override
    protected void processMessage(final Message m) {
        // Implement your processing logic for processing a single message. A default implementation of the batch
        // processing is found in the AbstractMessageProcessor
    }

}
