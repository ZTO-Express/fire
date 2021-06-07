/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.common.bean.runtime;

import com.sun.management.ThreadMXBean;

import java.io.Serializable;
import java.lang.management.ManagementFactory;

/**
 * 用于包装运行时线程信息
 * @author ChengLong 2019-9-28 19:36:52
 */
public class ThreadInfo implements Serializable {
    private static final long serialVersionUID = 7950498675819426939L;
    // 当前线程的总 CPU 时间（以毫微秒为单位）
    private long cpuTime;
    // 当前线程的总用户cpu时间（以毫微秒为单位）
    private long userTime;
    // 当前守护线程的总数
    private int deamonCount;
    // 返回自从 Java 虚拟机启动或峰值重置以来峰值活动线程计数
    private int peakCount;
    // 返回当前线程的总数，包括守护线程和非守护线程
    private int totalCount;
    // 返回自从 Java 虚拟机启动以来创建和启动的线程总数目
    private long totalStartedCount;

    private ThreadInfo() {}

    public long getCpuTime() {
        return cpuTime;
    }

    public long getUserTime() {
        return userTime;
    }

    public int getDeamonCount() {
        return deamonCount;
    }

    public int getPeakCount() {
        return peakCount;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public long getTotalStartedCount() {
        return totalStartedCount;
    }

    /**
     * 获取线程相关信息
     */
    public static ThreadInfo getThreadInfo() {
        ThreadInfo threadInfo = new ThreadInfo();
        ThreadMXBean threadMBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        threadInfo.cpuTime = threadMBean.getCurrentThreadCpuTime();
        threadInfo.userTime = threadMBean.getCurrentThreadUserTime();
        threadInfo.deamonCount = threadMBean.getDaemonThreadCount();
        threadInfo.peakCount = threadMBean.getPeakThreadCount();
        threadInfo.totalCount = threadMBean.getThreadCount();
        threadInfo.totalStartedCount = threadMBean.getTotalStartedThreadCount();

        return threadInfo;
    }

}
