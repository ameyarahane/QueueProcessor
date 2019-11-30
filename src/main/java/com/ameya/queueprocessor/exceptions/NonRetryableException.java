package com.ameya.queueprocessor.exceptions;

/**
 * A default implementation of class of RuntimeExceptions that are non-retryable and should be handled differently.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(final Throwable cause) {
        super(cause);
    }
}
