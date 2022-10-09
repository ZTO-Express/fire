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

import com.google.common.collect.ImmutableMap;
import com.zto.fire.common.util.MathUtils;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.FileSystem;
import oshi.software.os.OSFileStore;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 用于封装系统磁盘信息
 *
 * @author ChengLong 2019年9月29日 09:36:57
 */
public class DiskInfo {
    /**
     * 磁盘名称
     */
    private String name;

    /**
     * 磁盘制造商
     */
    private String model;

    /**
     * 磁盘总空间
     */
    private long total;

    /**
     * 磁盘读取总量
     */
    private long reads;

    /**
     * 磁盘写入总量
     */
    private long writes;

    /**
     * 磁盘读/写花费的毫秒数
     */
    private long transferTime;

    /**
     * 磁盘分区信息
     */
    private static class DiskPartitionInfo {
        // 分区名称
        private String name;
        // 文件系统类型
        private String fileSystem;
        // 挂载点
        private String mount;
        // 磁盘总空间
        private long total;
        // 磁盘可用空间
        private long free;
        // 磁盘已使用空间
        private long used;
        // 磁盘已用空间的百分比
        private double usedPer;
        // 总的inodes数
        private long totalInodes;
        // 可用的inodes数
        private long freeInodes;
        // 已用的inodes数
        private long usedInodes;
        // 已用的inode百分比
        private double usedInodesPer;

        public DiskPartitionInfo() {
        }

        public DiskPartitionInfo(String name, String fileSystem, String mount, long total, long free, long totalInodes, long freeInodes) {
            this.name = name;
            this.fileSystem = fileSystem;
            this.mount = mount;
            this.total = total;
            this.free = free;
            this.used = total - free;
            this.usedPer = MathUtils.percent(this.used, this.total, 2);
            this.totalInodes = totalInodes;
            this.freeInodes = freeInodes;
            this.usedInodes = totalInodes - freeInodes;
            this.usedInodesPer = MathUtils.percent(this.usedInodes, this.totalInodes, 2);
        }

        public String getName() {
            return name;
        }

        public String getFileSystem() {
            return fileSystem;
        }

        public String getMount() {
            return mount;
        }

        public long getTotal() {
            return total;
        }

        public long getFree() {
            return free;
        }

        public long getUsed() {
            return used;
        }

        public long getTotalInodes() {
            return totalInodes;
        }

        public long getFreeInodes() {
            return freeInodes;
        }

        public long getUsedInodes() {
            return usedInodes;
        }

        public String getUsedPer() {
            return usedPer + "%";
        }

        public String getUsedInodesPer() {
            return usedInodesPer + "%";
        }
    }

    public String getName() {
        return name;
    }

    public String getModel() {
        return model;
    }

    public long getTotal() {
        return total;
    }

    public long getReads() {
        return reads;
    }

    public long getWrites() {
        return writes;
    }

    public long getTransferTime() {
        return transferTime;
    }

    private DiskInfo() {
    }

    private DiskInfo(String name, String model, long total, long reads, long writes, long transferTime) {
        this.name = name;
        this.model = model;
        this.total = total;
        this.reads = reads;
        this.writes = writes;
        this.transferTime = transferTime;
    }

    /**
     * 获取磁盘与分区信息
     */
    public static Map<String, Object> getDiskInfo() {
        SystemInfo systemInfo = new SystemInfo();
        // 获取文件系统信息
        FileSystem fileSystem = systemInfo.getOperatingSystem().getFileSystem();
        OSFileStore[] fileStores = fileSystem.getFileStores();
        List<DiskPartitionInfo> partitionInfoList = new LinkedList<>();
        for (OSFileStore fileStore : fileStores) {
            if (fileStore != null) {
                partitionInfoList.add(new DiskPartitionInfo(fileStore.getName(), fileStore.getType(), fileStore.getMount(), fileStore.getTotalSpace(), fileStore.getUsableSpace(), fileStore.getTotalInodes(), fileStore.getFreeInodes()));
            }
        }

        // 获取磁盘信息
        HardwareAbstractionLayer hal = systemInfo.getHardware();
        List<DiskInfo> diskInfoList = new LinkedList<>();

        for (HWDiskStore disk : hal.getDiskStores()) {
            DiskInfo diskInfo = new DiskInfo(disk.getName(), disk.getModel(), disk.getSize(), disk.getReadBytes(), disk.getWriteBytes(), disk.getTransferTime());
            diskInfoList.add(diskInfo);
        }
        return ImmutableMap.<String, Object>builder().put("disks", diskInfoList).put("partitions", partitionInfoList).build();
    }
}
