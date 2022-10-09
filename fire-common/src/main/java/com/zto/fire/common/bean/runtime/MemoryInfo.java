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

import com.sun.management.OperatingSystemMXBean;

import java.io.Serializable;
import java.lang.management.ManagementFactory;

/**
 * 用于封装当前系统内存信息
 * @author ChengLong 2019-9-28 19:50:22
 */
public class MemoryInfo implements Serializable {
    private static final long serialVersionUID = 7803435486311085016L;

    /**
     * 操作系统总内存空间
     */
    private long total;

    /**
     * 操作系统内存剩余空间
     */
    private long free;

    /**
     * 操作系统内存使用空间
     */
    private long used;

    /**
     * 操作系统提交的虚拟内存大小
     */
    private long commitVirtual;

    /**
     * 操作系统交换内存总空间
     */
    private long swapTotal;

    /**
     * 操作系统交换内存剩余空间
     */
    private long swapFree;

    /**
     * 操作系统交换内存已使用空间
     */
    private long swapUsed;

    private MemoryInfo() {}

    public long getTotal() {
        return total;
    }

    public long getFree() {
        return free;
    }

    public long getUsed() {
        return used;
    }

    public long getCommitVirtual() {
        return commitVirtual;
    }

    public long getSwapTotal() {
        return swapTotal;
    }

    public long getSwapFree() {
        return swapFree;
    }

    public long getSwapUsed() {
        return swapUsed;
    }

    /**
     * 获取内存使用信息
     */
    public static MemoryInfo getMemoryInfo() {
        MemoryInfo memoryInfo = new MemoryInfo();
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        memoryInfo.total = osmxb.getTotalPhysicalMemorySize();
        memoryInfo.free = osmxb.getFreePhysicalMemorySize();
        memoryInfo.used = memoryInfo.total - memoryInfo.free;
        memoryInfo.swapTotal = osmxb.getTotalSwapSpaceSize();
        memoryInfo.swapFree = osmxb.getFreeSwapSpaceSize();
        memoryInfo.swapUsed = memoryInfo.swapTotal - memoryInfo.swapFree;
        memoryInfo.commitVirtual = osmxb.getCommittedVirtualMemorySize();

        return memoryInfo;
    }
}
