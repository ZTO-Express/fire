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

import com.zto.fire.common.util.MathUtils;
import oshi.SystemInfo;
import oshi.hardware.ComputerSystem;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.PowerSource;

/**
 * 硬件信息封装类
 *
 * @author ChengLong 2019年9月29日 15:52:50
 */
public class HardwareInfo {
    private static HardwareInfo hardwareInfo = new HardwareInfo();
    // 制造商
    private String manufacturer;
    // 型号
    private String model;
    // 序列号
    private String serialNumber;
    // 电源信息
    private String power;
    // 电池容量
    private String batteryCapacity;

    public String getManufacturer() {
        return manufacturer;
    }

    public String getModel() {
        return model;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public String getPower() {
        return power;
    }

    public String getBatteryCapacity() {
        return batteryCapacity;
    }

    private HardwareInfo() {
    }

    /**
     * 获取硬件设备信息
     */
    public static HardwareInfo getHardwareInfo() {
        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hardware = systemInfo.getHardware();
        ComputerSystem computerSystem = hardware.getComputerSystem();

        if (hardwareInfo.manufacturer == null) {
            hardwareInfo.manufacturer = computerSystem.getManufacturer();
        }

        if (hardwareInfo.model == null) {
            hardwareInfo.model = computerSystem.getModel();
        }

        if (hardwareInfo.serialNumber == null) {
            hardwareInfo.serialNumber = computerSystem.getSerialNumber().trim();
        }

        // 获取电源信息
        PowerSource[] powerSources = hardware.getPowerSources();
        if (powerSources == null || powerSources.length == 0) {
            hardwareInfo.power = "Unknown";
        } else {
            double timeRemaining = powerSources[0].getTimeRemaining();
            if (timeRemaining < -1d) {
                hardwareInfo.power = "充电中";
            } else if (timeRemaining < 0d) {
                hardwareInfo.power = "计算剩余时间";
            } else {
                hardwareInfo.power = String.format("%d:%02d remaining", (int) (timeRemaining / 3600),
                        (int) (timeRemaining / 60) % 60);
            }

            for (PowerSource pSource : powerSources) {
                hardwareInfo.batteryCapacity = MathUtils.doubleScale(pSource.getRemainingCapacity() * 100d, 2) + "";
            }
        }

        return hardwareInfo;
    }
}
