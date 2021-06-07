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
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.software.os.NetworkParams;

import java.util.LinkedList;
import java.util.List;

/**
 * 网卡信息封装类
 * @author ChengLong 2019年9月30日 10:39:08
 */
public class NetworkInfo {
    // 网卡名称
    private String name;
    // 网卡display名称
    private String displayName;
    // mac地址
    private String macAddress;
    // 最大传输单元
    private int mtu;
    // 网卡带宽
    private long speed;
    // ip v4 地址
    private String[] ipv4;
    // ip v6 地址
    private String[] ipv6;
    // ip 地址
    private String ip;
    // 接收到的数据报个数
    private long packetsRecv;
    // 发送的数据报个数
    private long packetsSent;
    // 接收到的数据大小
    private long bytesRecv;
    // 发送的数据大小
    private long bytesSent;
    // 主机名
    private String hostname;
    // 域名称
    private String domainName;
    // dns
    private String[] dns;
    // ip v4 网关
    private String ipv4Gateway;
    // ip v6 网关
    private String ipv6Gateway;

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public int getMtu() {
        return mtu;
    }

    public long getSpeed() {
        return speed;
    }

    public String[] getIpv4() {
        return ipv4;
    }

    public String[] getIpv6() {
        return ipv6;
    }

    public String getIp() {
        return ip;
    }

    public long getPacketsRecv() {
        return packetsRecv;
    }

    public long getPacketsSent() {
        return packetsSent;
    }

    public long getBytesRecv() {
        return bytesRecv;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public String getHostname() {
        return hostname;
    }

    public String getDomainName() {
        return domainName;
    }

    public String[] getDns() {
        return dns;
    }

    public String getIpv4Gateway() {
        return ipv4Gateway;
    }

    public String getIpv6Gateway() {
        return ipv6Gateway;
    }

    private NetworkInfo() {}

    public static List<NetworkInfo> getNetworkInfo() {
        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hal = systemInfo.getHardware();
        NetworkIF[] networkIFS = hal.getNetworkIFs();
        List<NetworkInfo> networkInfoList = new LinkedList<>();
        if (networkIFS != null && networkIFS.length > 0) {
            NetworkParams networkParams = systemInfo.getOperatingSystem().getNetworkParams();
            for (NetworkIF networkIF : networkIFS) {
                NetworkInfo networkInfo = new NetworkInfo();
                networkInfo.name = networkIF.getName();
                networkInfo.displayName = networkIF.getDisplayName();
                networkInfo.bytesRecv = networkIF.getBytesRecv();
                networkInfo.bytesSent = networkIF.getBytesSent();
                networkInfo.packetsRecv = networkIF.getPacketsRecv();
                networkInfo.packetsSent = networkIF.getPacketsSent();
                networkInfo.ip = OSUtils.getIp();
                networkInfo.ipv4 = networkIF.getIPv4addr();
                networkInfo.ipv6 = networkIF.getIPv6addr();
                networkInfo.mtu = networkIF.getMTU();
                networkInfo.speed = networkIF.getSpeed();
                networkInfo.macAddress = networkIF.getMacaddr();
                networkInfo.hostname = networkParams.getHostName();
                networkInfo.domainName = networkParams.getDomainName();
                networkInfo.ipv4Gateway = networkParams.getIpv4DefaultGateway();
                networkInfo.ipv6Gateway = networkParams.getIpv6DefaultGateway();
                networkInfo.dns = networkParams.getDnsServers();
                networkInfoList.add(networkInfo);
            }
        }
        return networkInfoList;
    }
}
