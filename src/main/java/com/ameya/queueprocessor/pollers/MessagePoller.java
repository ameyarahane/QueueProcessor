package com.ameya.queueprocessor.pollers;

public interface MessagePoller {

    void poll();

    void stop();
}