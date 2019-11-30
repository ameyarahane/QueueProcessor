package com.ameya.queueprocessor.processors.impl;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.exceptions.NonRetryableException;
import com.ameya.queueprocessor.exceptions.RetryableException;

public abstract class AbstractMessageProcessor {

    public abstract void handleRetryableFailure(final Message m, RetryableException e);

    public abstract void handlePermanentFailure(final Message m, NonRetryableException e);

    public abstract void handleSuccess(final Message m);

    protected abstract void process(final Message m);

    public void processMessage(Message m) {
        try {
            process(m);
            handleSuccess(m);
        } catch (RuntimeException e) {
            if (isRetryableFailure(e)) {
                handleRetryableFailure(m, new RetryableException(e));
            } else {
                handlePermanentFailure(m, new NonRetryableException(e));
            }
        }
    }

    protected abstract boolean isRetryableFailure(final RuntimeException e);
}
