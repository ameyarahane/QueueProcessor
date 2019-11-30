package com.ameya.queueprocessor.configuration;


import com.amazonaws.regions.Regions;

/**
 * Replacement for a configuration file,
 */
public final class Configuration {

    private Configuration() {
    }

    public static final int BATCH_SIZE = 10;
    public static final int BUFFER_CAPACITY = 300;
    public static final int NUM_POLLERS = 3; // Number of threads to poll SQS queue
    public static final int NUM_PROCESSORS = 5; // Number of message processor threads PER POLLER
    public static final int POLLING_DELAY_MS = 3000; // Poll for messages every 3 seconds
    public static final String SQS_QUEUE_NAME = "service-queue";
    public static final String SQS_DLQ_NAME = "service-queue-dlq";
    public static final Regions REGION = Regions.US_WEST_2;
}
