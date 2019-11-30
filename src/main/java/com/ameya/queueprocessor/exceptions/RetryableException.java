package com.ameya.queueprocessor.exceptions;

/**
 * A default implementation of class of RuntimeExceptions that are retryable by the message processors.
 */
public class RetryableException extends RuntimeException {

    public RetryableException(final Throwable cause) {
        super(cause);
    }
}
