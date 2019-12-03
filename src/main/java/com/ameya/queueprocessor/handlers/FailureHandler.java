package com.ameya.queueprocessor.handlers;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.exceptions.NonRetryableException;
import com.ameya.queueprocessor.exceptions.RetryableException;

import java.util.Map;

/**
 * Interface for handling different types of failures from a message processing perspective.
 */
public interface FailureHandler {

    void handleRetryableFailures(final Map<Message, RetryableException> retryableFailures);

    void handleNonRetryableFailures(final Map<Message, NonRetryableException> nonRetryableFailures);
}
