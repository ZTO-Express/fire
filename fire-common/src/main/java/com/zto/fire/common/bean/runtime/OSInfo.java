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
import oshi.SystemInfo;
import oshi.software.os.OperatingSystem;
import oshi.util.FormatUtil;

/**
 * 用于封装操作系统信息
 *
 * @author ChengLong 2019-9-28 19:56:59
 */
public class OSInfo {
    private static OSInfo osInfo = new OSInfo();
    // 制造商
    private String manufacturer;
    // 操作系统名称
    private String name;
    // 操作系统架构
    private String arch;
    // 操作系统版本
    private String version;
    // 当前用户
    private String userName;
    // 当前用户家目录
    private String userHome;
    // 当前用户工作目录
    private String userDir;
    // 机器的ip
    private String ip;
    // 集群的主机名
    private String hostname;
    // 运行时间
    private String uptime;
    // 组织信息
    private String family;

    private OSInfo() {
    }

    public String getName() {
        return name;
    }

    public String getArch() {
        return arch;
    }

    public String getVersion() {
        return version;
    }

    public String getUserName() {
        return userName;
    }

    public String getUserHome() {
        return userHome;
    }

    public String getUserDir() {
        return userDir;
    }

    public String getIp() {
        return ip;
    }

    public String getHostname() {
        return hostname;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public String getUptime() {
        return uptime;
    }

    public String getFamily() {
        return family;
    }

    /**
     * 获取操作系统相关信息
     */
    public static OSInfo getOSInfo() {
        SystemInfo systemInfo = new SystemInfo();
        osInfo.name = System.getProperty("os.name");
        osInfo.arch = System.getProperty("os.arch");
        osInfo.version = System.getProperty("os.version");
        osInfo.userName = System.getProperty("user.name");
        osInfo.userHome = System.getProperty("user.home");
        osInfo.userDir = System.getProperty("user.dir");
        osInfo.ip = OSUtils.getIp();
        osInfo.hostname = OSUtils.getHostName();
        OperatingSystem os = systemInfo.getOperatingSystem();
        osInfo.manufacturer = os.getManufacturer();
        osInfo.family = os.getFamily();
        osInfo.uptime = FormatUtil.formatElapsedSecs(systemInfo.getHardware().getProcessor().getSystemUptime());
        return osInfo;
    }
}
