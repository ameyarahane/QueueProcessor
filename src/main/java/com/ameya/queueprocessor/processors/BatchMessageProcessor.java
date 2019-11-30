package com.ameya.queueprocessor.processors;

import com.amazonaws.services.sqs.model.Message;

import java.util.Collection;

public interface BatchMessageProcessor {

    void processBatch(Collection<Message> messages);
}
