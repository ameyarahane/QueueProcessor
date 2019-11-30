package com.ameya.queueprocessor.processors.impl;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.BatchProcessingFailureHandler;
import com.ameya.queueprocessor.BatchProcessingSuccessHandler;
import com.ameya.queueprocessor.exceptions.NonRetryableException;
import com.ameya.queueprocessor.exceptions.RetryableException;
import com.ameya.queueprocessor.processors.BatchMessageProcessor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Getter
public abstract class AbstractBatchMessageProcessor implements BatchMessageProcessor {

    @NonNull
    private final BatchProcessingSuccessHandler successHandler;
    @NonNull
    private final BatchProcessingFailureHandler failureHandler;

    @Override
    public void processBatch(Collection<Message> messages) {
        Map<Message, RetryableException> retryableFailures = new HashMap();
        Map<Message, NonRetryableException> permanentFailures = new HashMap();

        messages.forEach(m -> {
            try {
                processMessage(m);
                handleSuccess(m);
            } catch (RuntimeException e) {
                if (isRetryableFailure(e)) {
                    retryableFailures.put(m, new RetryableException(e));
                } else {
                    permanentFailures.put(m, new NonRetryableException(e));
                }
            }
        });

        if (!retryableFailures.isEmpty()) {
            handleBatchRetryableFailures(retryableFailures);
        }

        if (!permanentFailures.isEmpty()) {
            handleBatchNonRetryableFailures(permanentFailures);
        }
    }

    protected void handleSuccess(final Message message) {
        // Typically if everything went well, all you need to do is delete the message from the queue, publish
        // metrics, etc
        successHandler.handleSuccess(message);
    }

    protected void handleBatchRetryableFailures(final Map<Message, RetryableException> retryableFailures) {
        failureHandler.handleBatchRetryableFailures(retryableFailures);
    }

    protected void handleBatchNonRetryableFailures(final Map<Message, NonRetryableException> nonRetryableFailures) {
        failureHandler.handleBatchNonRetryableFailures(nonRetryableFailures);
    }

    protected boolean isRetryableFailure(final RuntimeException e) {
        // Implement whatever is your logic to determine if the error is retryable or not.
        // Example could be where a downstream service is throttling this client or is down intermittently
        return false;
    }

    protected abstract void processMessage(final Message m);
}
