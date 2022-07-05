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

package com.zto.fire.spark.bean;

import com.zto.fire.common.util.DateFormatUtils;

import java.util.Map;
import java.util.Properties;

/**
 * 用于封装spark运行时的信息
 * @author ChengLong 2019-5-13 10:27:33
 */
public class SparkInfo {
    // spark应用名称
    private String appName;
    // spark应用的类名
    private String className;
    // common包的版本号
    private String fireVersion;
    // spark conf信息
    private Map<String, String> conf;
    // 当前spark版本
    private String version;
    // spark 运行模式
    private String master;
    // spark 的 applicationId
    private String applicationId;
    // yarn 的 applicationAttemptId
    private String applicationAttemptId;
    // spark 的 webui地址
    private String ui;
    // driver的进程id
    private String pid;
    // spark的运行时间
    private String uptime;
    // 程序启动的起始时间
    private String startTime;
    // 申请的每个executor的内存大小
    private String executorMemory;
    // 申请的executor个数
    private String executorInstances;
    // 申请的每个executor的cpu数
    private String executorCores;
    // 申请的driver cpu数量
    private String driverCores;
    // 申请的driver内存大小
    private String driverMemory;
    // 申请的driver堆外内存大小
    private String driverMemoryOverhead;
    // driver所在服务器ip
    private String driverHost;
    // driver占用的端口号
    private String driverPort;
    // restful接口的端口号
    private String restPort;
    // 申请的executor堆外内存大小
    private String executorMemoryOverhead;
    // 当前spark应用申请的总内存大小（driver+executor+总的堆外内存）
    private String memory;
    // 当前spark应用申请的总的cpu数量（driver+executor）
    private String cpu;
    // streaming批次时间
    private String batchDuration;
    // 当前driver系统时间
    private String timestamp = DateFormatUtils.formatCurrentDateTime();
    // 配置信息
    private Map<String, String> properties;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getFireVersion() {
        return fireVersion;
    }

    public void setFireVersion(String fireVersion) {
        this.fireVersion = fireVersion;
    }

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getApplicationAttemptId() {
        return applicationAttemptId;
    }

    public void setApplicationAttemptId(String applicationAttemptId) {
        this.applicationAttemptId = applicationAttemptId;
    }

    public String getUi() {
        return ui;
    }

    public void setUi(String ui) {
        this.ui = ui;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String getExecutorInstances() {
        return executorInstances;
    }

    public void setExecutorInstances(String executorInstances) {
        this.executorInstances = executorInstances;
    }

    public String getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(String executorCores) {
        this.executorCores = executorCores;
    }

    public String getDriverCores() {
        return driverCores;
    }

    public void setDriverCores(String driverCores) {
        this.driverCores = driverCores;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }

    public String getDriverMemoryOverhead() {
        return driverMemoryOverhead;
    }

    public void setDriverMemoryOverhead(String driverMemoryOverhead) {
        this.driverMemoryOverhead = driverMemoryOverhead;
    }

    public String getDriverHost() {
        return driverHost;
    }

    public void setDriverHost(String driverHost) {
        this.driverHost = driverHost;
    }

    public String getDriverPort() {
        return driverPort;
    }

    public void setDriverPort(String driverPort) {
        this.driverPort = driverPort;
    }

    public String getExecutorMemoryOverhead() {
        return executorMemoryOverhead;
    }

    public void setExecutorMemoryOverhead(String executorMemoryOverhead) {
        this.executorMemoryOverhead = executorMemoryOverhead;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public String getCpu() {
        return cpu;
    }

    public void setCpu(String cpu) {
        this.cpu = cpu;
    }

    public String getBatchDuration() {
        return batchDuration;
    }

    public void setBatchDuration(String batchDuration) {
        this.batchDuration = batchDuration;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getRestPort() {
        return restPort;
    }

    public void setRestPort(String restPort) {
        this.restPort = restPort;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * 计算cpu和内存总数
     */
    public void computeCpuMemory() {
        this.memory = (Integer.parseInt(this.driverMemory.replace("g", "")) + Integer.parseInt(this.driverMemoryOverhead.replace("g", "")) + Integer.parseInt(this.executorInstances) * (Integer.parseInt(this.executorMemory.replace("g", "")) + Integer.parseInt(this.executorMemoryOverhead.replace("g", "")))) + "g";
        this.cpu = (Integer.parseInt(this.executorInstances) * Integer.parseInt(this.executorCores) + Integer.parseInt(this.driverCores)) + "";
    }
}
