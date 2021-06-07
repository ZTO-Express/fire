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
import com.zto.fire.common.util.MathUtils;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.Sensors;
import oshi.util.FormatUtil;

import java.io.Serializable;
import java.lang.management.ManagementFactory;

/**
 * 用于封装cpu运行时信息
 *
 * @author ChengLong 2019-9-28 19:52:56
 */
public class CpuInfo implements Serializable {
    private static final long serialVersionUID = 7712733535989008368L;
    // 系统cpu的负载
    private double cpuLoad;
    // 当前jvm可用的处理器数量
    private int availableProcessors;
    // 当前jvm占用的cpu时长
    private long processCpuTime;
    // 当前jvm占用的cpu负载
    private double processCpuLoad;
    // cpu温度
    private double temperature;
    // cpu电压
    private double voltage;
    // 风扇转速
    private int[] fanSpeeds;
    // 物理cpu数
    private int physicalCpu;
    // 逻辑cpu数
    private int logicalCpu;
    // 运行时间
    private long uptime;
    // io等待
    private long ioWait;
    // 用户时长
    private long userTick;
    // nice时长
    private long niceTick;
    // 系统时长
    private long sysTick;
    // 空闲时长
    private long idleTick;
    // 中断时长
    private long irqTick;
    // 软中断时长
    private long softIrqTick;
    // cpu steal 时长
    private long stealTick;
    // cpu平均负载
    private double[] loadAverage;
    // 最近一次平均负载
    private double lastLoadAverage;

    public double[] getLoadAverage() {
        return this.loadAverage;
    }

    public double getLastLoadAverage() {
        return lastLoadAverage;
    }

    public double getCpuLoad() {
        return MathUtils.doubleScale(cpuLoad, 2);
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public long getProcessCpuTime() {
        return processCpuTime;
    }

    public double getProcessCpuLoad() {
        return MathUtils.doubleScale(processCpuLoad, 2);
    }

    public String getTemperature() {
        return temperature + "℃";
    }

    public String getVoltage() {
        return voltage + "v";
    }

    public int[] getFanSpeeds() {
        return fanSpeeds;
    }

    public int getPhysicalCpu() {
        return physicalCpu;
    }

    public int getLogicalCpu() {
        return logicalCpu;
    }

    public String getUptime() {
        return FormatUtil.formatElapsedSecs(uptime);
    }

    public long getIoWait() {
        return ioWait;
    }

    public long getUserTick() {
        return userTick;
    }

    public long getNiceTick() {
        return niceTick;
    }

    public long getSysTick() {
        return sysTick;
    }

    public long getIdleTick() {
        return idleTick;
    }

    public long getIrqTick() {
        return irqTick;
    }

    public long getSoftIrqTick() {
        return softIrqTick;
    }

    public long getStealTick() {
        return stealTick;
    }

    private CpuInfo() {
    }

    /**
     * 获取cpu使用信息
     */
    public static CpuInfo getCpuInfo() {
        CpuInfo cpuInfo = new CpuInfo();
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        cpuInfo.lastLoadAverage = osmxb.getSystemLoadAverage();
        cpuInfo.cpuLoad = osmxb.getSystemCpuLoad();
        cpuInfo.availableProcessors = osmxb.getAvailableProcessors();
        cpuInfo.processCpuTime = osmxb.getProcessCpuTime();
        cpuInfo.processCpuLoad = osmxb.getProcessCpuLoad();
        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hal = systemInfo.getHardware();
        Sensors sensors = hal.getSensors();
        cpuInfo.temperature = sensors.getCpuTemperature();
        cpuInfo.voltage = sensors.getCpuVoltage();
        cpuInfo.fanSpeeds = sensors.getFanSpeeds();
        CentralProcessor centralProcessor = hal.getProcessor();
        cpuInfo.physicalCpu = centralProcessor.getPhysicalProcessorCount();
        cpuInfo.logicalCpu = centralProcessor.getLogicalProcessorCount();

        CentralProcessor processor = hal.getProcessor();
        cpuInfo.uptime = processor.getSystemUptime();
        long[] ticks = processor.getSystemCpuLoadTicks();
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        cpuInfo.userTick = ticks[TickType.USER.getIndex()] - prevTicks[TickType.USER.getIndex()];
        cpuInfo.niceTick = ticks[TickType.NICE.getIndex()] - prevTicks[TickType.NICE.getIndex()];
        cpuInfo.sysTick = ticks[TickType.SYSTEM.getIndex()] - prevTicks[TickType.SYSTEM.getIndex()];
        cpuInfo.idleTick = ticks[TickType.IDLE.getIndex()] - prevTicks[TickType.IDLE.getIndex()];
        cpuInfo.ioWait = ticks[TickType.IOWAIT.getIndex()] - prevTicks[TickType.IOWAIT.getIndex()];
        cpuInfo.irqTick = ticks[TickType.IRQ.getIndex()] - prevTicks[TickType.IRQ.getIndex()];
        cpuInfo.softIrqTick = ticks[TickType.SOFTIRQ.getIndex()] - prevTicks[TickType.SOFTIRQ.getIndex()];
        cpuInfo.stealTick = ticks[TickType.STEAL.getIndex()] - prevTicks[TickType.STEAL.getIndex()];
        cpuInfo.loadAverage = processor.getSystemLoadAverage(3);

        return cpuInfo;
    }

}
