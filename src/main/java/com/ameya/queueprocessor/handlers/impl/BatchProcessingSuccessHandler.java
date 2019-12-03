package com.ameya.queueprocessor.handlers.impl;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.daos.SqsAccessor;
import com.ameya.queueprocessor.handlers.SuccessHandler;
import lombok.RequiredArgsConstructor;

/**
 * Handles the actions to be done whenever a message is successfully processed. This can be extended by other classes.
 */
@RequiredArgsConstructor
public class BatchProcessingSuccessHandler implements SuccessHandler {

    private final SqsAccessor queueAccessor;

    @Override
    public void handleSuccess(final Message m) {
        // Typically if everything went well, all you need to do is delete the message from the queue, publish
        // metrics, etc
        queueAccessor.deleteMessage(m.getReceiptHandle());
    }
}
