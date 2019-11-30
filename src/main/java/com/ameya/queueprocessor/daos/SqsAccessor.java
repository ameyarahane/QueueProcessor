package com.ameya.queueprocessor.daos;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import lombok.NonNull;

import java.util.Collection;
import java.util.List;

/**
 * A minimal SQS queue accessor that operates on a single queue.
 */
public class SqsAccessor {

    private static final int MAX_ALLOWED_BATCH_SIZE = 10;
    private final AmazonSQS sqs;
    private final String queueName;
    private final String queueUrl;

    public SqsAccessor(@NonNull final AmazonSQS sqs, @NonNull final String queueName) {
        this.sqs = sqs;
        this.queueName = queueName;
        this.queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
    }

    public void deleteMessage(final String receiptHandle) {
        sqs.deleteMessage(queueUrl, receiptHandle);
    }

    public void sendMessage(final String message) {
        sqs.sendMessage(queueUrl, message);
    }

    public Collection<Message> getMessages(final int batchSize) {
        if (batchSize > MAX_ALLOWED_BATCH_SIZE) {
            throw new IllegalArgumentException(String.format("Batch size %s exceeds max allowed size %s",
                    batchSize, MAX_ALLOWED_BATCH_SIZE));
        }
        return retrieveMessages(batchSize);
    }

    private List<Message> retrieveMessages(final int batchSize) {
        ReceiveMessageRequest request = new ReceiveMessageRequest()
                .withMaxNumberOfMessages(batchSize)
                .withQueueUrl(queueUrl);
        ReceiveMessageResult result = sqs.receiveMessage(request);
        return result.getMessages();
    }

    // As needed the accessor can be extended to provide more methods for adding message attributes, delay, etc.
}
