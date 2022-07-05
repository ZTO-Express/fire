package com.zto.fire.jmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class JmxApp {
    public static void main(String[] args) throws Exception {
        // 最基本的MBean
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.zto.fire.jmx:type=Hello");
        Hello mbean = new Hello();
        mbs.registerMBean(mbean, name);

        // 复杂类型的MXBean
        ObjectName mxbeanName = new ObjectName("com.zto.fire.jmx:type=QueueSampler");
        Queue<String> queue = new ArrayBlockingQueue<String>(10);
        queue.add("Request-1");
        queue.add("Request-2");
        queue.add("Request-3");
        QueueSampler mxbean = new QueueSampler(queue);
        mbs.registerMBean(mxbean, mxbeanName);

        System.out.println("Waiting forever...");
        Thread.sleep(Long.MAX_VALUE);
    }
}
