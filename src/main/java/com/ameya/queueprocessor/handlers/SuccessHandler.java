package com.ameya.queueprocessor.handlers;

import com.amazonaws.services.sqs.model.Message;

/**
 * Interface for handling successful message processing.
 */
public interface SuccessHandler {

    public void handleSuccess(Message m);
}
