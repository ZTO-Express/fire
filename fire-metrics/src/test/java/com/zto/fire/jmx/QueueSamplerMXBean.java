package com.zto.fire.jmx;

public interface QueueSamplerMXBean {
    public QueueSample getQueueSample();

    public void clearQueue();
}