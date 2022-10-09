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

import java.io.Serializable;
import java.lang.management.*;
import java.util.List;

/**
 * Jvm信息包装类，可获取jvm相关信息
 * @author ChengLong 2019-9-28 19:38:36
 */
public class JvmInfo implements Serializable {
    private static final long serialVersionUID = 3857878519712626828L;

    /**
     * Java版本
     */
    private String javaVersion;

    private String javaHome;

    private String classVersion;

    /**
     * jvm可从操作系统申请的最大内存
     */
    private long memoryMax;

    /**
     * jvm已使用操作系统的总内存空间
     */
    private long memoryTotal;

    /**
     * jvm剩余内存空间
     */
    private long memoryFree;

    /**
     * jvm已使用内存空间
     */
    private long memoryUsed;

    /**
     * jvm启动时间，unix时间戳
     */
    private long startTime;

    /**
     * jvm运行时间
     */
    private long uptime;

    /**
     * jvm heap 初始内存大小
     */
    private long heapInitSize;

    /**
     * jvm heap 最大内存空间
     */
    private long heapMaxSize;

    /**
     * jvm heap 已使用空间大小
     */
    private long heapUseSize;

    /**
     * jvm heap 已提交的空间大小
     */
    private long heapCommitedSize;

    /**
     * jvm Non-Heap初始空间
     */
    private long nonHeapInitSize;

    /**
     * jvm Non-Heap最大空间
     */
    private long nonHeapMaxSize;

    /**
     * jvm Non-Heap已使用空间
     */
    private long nonHeapUseSize;

    /**
     * jvm Non-Heap已提交空间
     */
    private long nonHeapCommittedSize;

    /**
     * minor gc 次数
     */
    private long minorGCCount;

    /**
     * minor gc 总耗时
     */
    private long minorGCTime;

    /**
     * full gc 次数
     */
    private long fullGCCount;

    /**
     * full gc 总耗时
     */
    private long fullGCTime;

    /**
     * 虚拟机参数
     */
    private List<String> jvmOptions;

    private JvmInfo() {}

    public long getMemoryMax() {
        return memoryMax;
    }

    public long getMemoryTotal() {
        return memoryTotal;
    }

    public long getMemoryFree() {
        return memoryFree;
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getUptime() {
        return uptime;
    }

    public long getHeapInitSize() {
        return heapInitSize;
    }

    public long getHeapMaxSize() {
        return heapMaxSize;
    }

    public long getHeapUseSize() {
        return heapUseSize;
    }

    public long getHeapCommitedSize() {
        return heapCommitedSize;
    }

    public long getNonHeapInitSize() {
        return nonHeapInitSize;
    }

    public long getNonHeapMaxSize() {
        return nonHeapMaxSize;
    }

    public long getNonHeapUseSize() {
        return nonHeapUseSize;
    }

    public long getNonHeapCommittedSize() {
        return nonHeapCommittedSize;
    }

    public String getJavaVersion() {
        return javaVersion;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public String getClassVersion() {
        return classVersion;
    }

    public long getMinorGCCount() {
        return minorGCCount;
    }

    public long getMinorGCTime() {
        return minorGCTime;
    }

    public long getFullGCCount() {
        return fullGCCount;
    }

    public long getFullGCTime() {
        return fullGCTime;
    }

    public List<String> getJvmOptions() {
        return jvmOptions;
    }

    /**
     * 获取Jvm、类加载器与线程相关信息
     */
    public static JvmInfo getJvmInfo() {
        Runtime runtime = Runtime.getRuntime();
        JvmInfo jvmInfo = new JvmInfo();
        jvmInfo.memoryMax = runtime.maxMemory();
        jvmInfo.memoryTotal = runtime.totalMemory();
        jvmInfo.memoryFree = runtime.freeMemory();
        jvmInfo.memoryUsed = jvmInfo.memoryTotal - jvmInfo.memoryFree;
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        jvmInfo.startTime = runtimeMXBean.getStartTime();
        jvmInfo.uptime = runtimeMXBean.getUptime();

        // 获取jvm heap相关信息
        MemoryMXBean memoryMBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryMBean.getHeapMemoryUsage();
        jvmInfo.heapInitSize = heapUsage.getInit();
        jvmInfo.heapMaxSize = heapUsage.getMax();
        jvmInfo.heapUseSize = heapUsage.getUsed();
        jvmInfo.heapCommitedSize = heapUsage.getCommitted();

        // 获取jvm non-heap相关信息
        MemoryUsage nonHeapUsage = memoryMBean.getNonHeapMemoryUsage();
        jvmInfo.nonHeapInitSize = nonHeapUsage.getInit();
        jvmInfo.nonHeapMaxSize = nonHeapUsage.getMax();
        jvmInfo.nonHeapUseSize = nonHeapUsage.getUsed();
        jvmInfo.nonHeapCommittedSize = nonHeapUsage.getCommitted();

        // 获取jvm版本与安装信息
        jvmInfo.javaVersion = System.getProperty("java.version");
        jvmInfo.javaHome = System.getProperty("java.home");
        jvmInfo.classVersion = System.getProperty("java.class.version");

        // jvm 参数
        jvmInfo.jvmOptions = ManagementFactory.getRuntimeMXBean().getInputArguments();

        // 获取gc信息
        List<GarbageCollectorMXBean> gcs = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gc : gcs) {
            if (gc.getName().contains("Young") || gc.getName().contains("MarkSweep")) {
                jvmInfo.minorGCCount = gc.getCollectionCount();
                jvmInfo.minorGCTime = gc.getCollectionTime();
            }
            if (gc.getName().contains("Old") || gc.getName().contains("Scavenge")) {
                jvmInfo.fullGCCount = gc.getCollectionCount();
                jvmInfo.fullGCTime = gc.getCollectionTime();
            }
        }

        return jvmInfo;
    }
}
