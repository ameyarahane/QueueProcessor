package com.ameya.queueprocessor.handlers.impl;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.daos.SqsAccessor;
import com.ameya.queueprocessor.exceptions.NonRetryableException;
import com.ameya.queueprocessor.exceptions.RetryableException;
import com.ameya.queueprocessor.handlers.FailureHandler;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;

/**
 * A simple implementation that does nothing with messages and offloads message failures to SQS.
 * The behavior can be extended to do different things with it.
 */
@RequiredArgsConstructor
public class BatchProcessingFailureHandler implements FailureHandler {

    private final SqsAccessor dlqAccessor;
    private final SqsAccessor queueAccessor;

    @Override
    public void handleRetryableFailures(final Map<Message, RetryableException> retryableFailures) {
        // Handle the retryable failures from the batch.
        handleFailures(retryableFailures.keySet());
    }

    @Override
    public void handleNonRetryableFailures(final Map<Message, NonRetryableException> nonRetryableFailures) {
        // Handle the non-retryable failures form the batch by doing things like explicitly moving them to the DLQ
        // instead of having SQS retry these messages.
        handleFailures(nonRetryableFailures.keySet());
    }

    private void handleFailures(final Collection<Message> failures) {
        // Do nothing, error handling is handled by the SQS queue itself.
        // Alternatively, if you had to move the messages to DLQ right away uncomment lines below.
        /*
         * failures.stream().forEach(message -> {
         *     dlqAccessor.sendMessage(message.getBody());
         *     queueAccessor.deleteMessage(message.getReceiptHandle());
         * });
         */
    }
}
