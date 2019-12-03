package com.ameya.queueprocessor.processors.impl;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.exceptions.NonRetryableException;
import com.ameya.queueprocessor.exceptions.RetryableException;
import com.ameya.queueprocessor.handlers.FailureHandler;
import com.ameya.queueprocessor.handlers.SuccessHandler;
import com.ameya.queueprocessor.processors.BatchMessageProcessor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Getter
@Log4j2
public abstract class AbstractBatchMessageProcessor implements BatchMessageProcessor {

    @NonNull
    private final SuccessHandler successHandler;
    @NonNull
    private final FailureHandler failureHandler;

    @Override
    public void processBatch(Collection<Message> messages) {
        Map<Message, RetryableException> retryableFailures = new HashMap();
        Map<Message, NonRetryableException> nonRetryableFailures = new HashMap();

        messages.forEach(m -> {
            try {
                processMessage(m);
                handleSuccess(m);
            } catch (RuntimeException e) {
                if (isRetryableFailure(e)) {
                    retryableFailures.put(m, new RetryableException(e));
                } else {
                    nonRetryableFailures.put(m, new NonRetryableException(e));
                }
            }
        });

        if (!retryableFailures.isEmpty()) {
            handleRetryableFailures(retryableFailures);
        }

        if (!nonRetryableFailures.isEmpty()) {
            handleNonRetryableFailures(nonRetryableFailures);
        }
    }

    protected void handleSuccess(final Message message) {
        // Typically if everything went well, all you need to do is delete the message from the queue, publish
        // metrics, etc
        successHandler.handleSuccess(message);
    }

    protected void handleRetryableFailures(final Map<Message, RetryableException> retryableFailures) {
        retryableFailures.forEach((message, exception) ->
                log.error("Error processing message: [{}] due to exception: [{}]", message, exception.getMessage(),
                        exception));
        failureHandler.handleRetryableFailures(retryableFailures);
    }

    protected void handleNonRetryableFailures(final Map<Message, NonRetryableException> nonRetryableFailures) {
        nonRetryableFailures.forEach((message, exception) ->
                log.error("Error processing message: [{}] due to exception: [{}]", message, exception.getMessage(),
                        exception));
        failureHandler.handleNonRetryableFailures(nonRetryableFailures);
    }

    protected boolean isRetryableFailure(final RuntimeException e) {
        // Implement whatever is your logic to determine if the error is retryable or not.
        // Example could be where a downstream service is throttling this client or is down intermittently
        return false;
    }

    protected abstract void processMessage(final Message m);
}
