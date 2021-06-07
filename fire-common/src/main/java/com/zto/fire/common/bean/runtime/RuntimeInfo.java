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

import com.zto.fire.common.util.OSUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * 用于获取jvm、os、memory等运行时信息
 *
 * @author ChengLong 2019年9月28日 16:57:03
 */
public class RuntimeInfo implements Serializable {
    private static final long serialVersionUID = 1960438466835847330L;
    private static RuntimeInfo runtimeInfo = new RuntimeInfo();
    // jvm运行时信息
    private JvmInfo jvmInfo;
    // 线程运行时信息
    private ThreadInfo threadInfo;
    // cpu运行时信息
    private CpuInfo cpuInfo;
    // 内存运行时信息
    private MemoryInfo memoryInfo;
    // 类加载器运行时信息
    private ClassLoaderInfo classLoaderInfo;
    // executor所在ip
    private static String ip;
    // executor所在主机名
    private static String hostname;
    // 当前pid的进程号
    private static String pid;
    // executor启动时间（UNIX时间戳）
    private long startTime = System.currentTimeMillis();

    private RuntimeInfo() {
    }

    public JvmInfo getJvmInfo() {
        return jvmInfo;
    }

    public ThreadInfo getThreadInfo() {
        return threadInfo;
    }

    public CpuInfo getCpuInfo() {
        return cpuInfo;
    }

    public MemoryInfo getMemoryInfo() {
        return memoryInfo;
    }

    public ClassLoaderInfo getClassLoaderInfo() {
        return classLoaderInfo;
    }

    public String getIp() {
        return ip;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPid() {
        return pid;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getUptime() {
        // executor运行时间（毫秒）
        return System.currentTimeMillis() - this.startTime;
    }

    /**
     * 获取运行时信息
     *
     * @return 当前运行时信息
     */
    public static RuntimeInfo getRuntimeInfo() {
        if (StringUtils.isBlank(ip)) {
            ip = OSUtils.getIp();
        }
        if (StringUtils.isBlank(hostname)) {
            hostname = OSUtils.getHostName();
        }
        if (StringUtils.isBlank(pid)) {
            pid = OSUtils.getPid();
        }
        runtimeInfo.jvmInfo = JvmInfo.getJvmInfo();
        runtimeInfo.classLoaderInfo = ClassLoaderInfo.getClassLoaderInfo();
        runtimeInfo.threadInfo = ThreadInfo.getThreadInfo();
        runtimeInfo.cpuInfo = CpuInfo.getCpuInfo();
        runtimeInfo.memoryInfo = MemoryInfo.getMemoryInfo();

        return runtimeInfo;
    }
}
