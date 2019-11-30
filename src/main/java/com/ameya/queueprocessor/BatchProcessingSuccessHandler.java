package com.ameya.queueprocessor;

import com.amazonaws.services.sqs.model.Message;
import com.ameya.queueprocessor.daos.SqsAccessor;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchProcessingSuccessHandler {

    private SqsAccessor queueAccessor;

    public void handleSuccess(final Message m) {
        // Typically if everything went well, all you need to do is delete the message from the queue, publish
        // metrics, etc
        queueAccessor.deleteMessage(m.getReceiptHandle());
    }
}
