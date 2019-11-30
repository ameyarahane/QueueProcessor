package com.ameya.queueprocessor;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class MessagePollerRunnable implements Runnable {

    private final AtomicBoolean running;
    private final MessagePoller poller;

    public MessagePollerRunnable(@NonNull final MessagePoller poller) {
        this.poller = poller;
        this.running = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        while (!running.get()) {
            sleep();
        }
        log.info("Thread {} started polling for messages.", Thread.currentThread().getName());
        try {
            poller.poll();
        } finally {
            log.info("Thread {} stopped polling for messages.", Thread.currentThread().getName());
        }
    }

    public void start() {
        running.set(true);
    }

    public void stop() {
        running.set(false);
    }

    private void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format("Thread %s interrupted!", Thread.currentThread().getName(), e));
        }
    }

}
