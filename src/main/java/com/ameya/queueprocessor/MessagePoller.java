package com.ameya.queueprocessor;

public interface MessagePoller {

    void poll();

    void start();

    void stop();
}