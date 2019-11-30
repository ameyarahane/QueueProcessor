package com.ameya.queueprocessor;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.daos.SqsAccessor;
import com.ameya.queueprocessor.exceptions.NonRetryableException;
import com.ameya.queueprocessor.exceptions.RetryableException;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;

/**
 * A simple implementation that does nothing with messages and offloads message failures to SQS.
 */
@RequiredArgsConstructor
public class BatchProcessingFailureHandler {

    private SqsAccessor dlqAccessor;
    private SqsAccessor queueAccessor;

    public void handleBatchRetryableFailures(final Map<Message, RetryableException> retryableFailures) {
        // handle the retryable failures from the batch.
        handleFailures(retryableFailures.keySet());
    }

    public void handleBatchNonRetryableFailures(final Map<Message, NonRetryableException> nonRetryableFailures) {
        // handle the non-retryable failures form the batch by explicitly moving them to a DLQ and deleting them from
        // the main queue to avoid unnecessary retries.
        handleFailures(nonRetryableFailures.keySet());
    }

    private void handleFailures(final Collection<Message> failures) {
        // do nothing. Alternatively, if you had to move the messages to DLQ right away uncomment lines below
        /*
         * failures.stream().forEach(message -> {
         *     dlqAccessor.sendMessage(message.getBody());
         *     queueAccessor.deleteMessage(message.getReceiptHandle());
         * });
         */
    }
}
