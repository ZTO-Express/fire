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

package com.zto.fire.common.bean.rest.yarn;

/**
 * 用于解析调用yarn接口返回的json
 * @author ChengLong 2019-5-15 17:50:06
 */
public class App {
    // yarn applicationId
    private String id;
    // yarn程序的启动用户
    private String user;
    // yarn程序名称
    private String name;
    // yarn的队列名称
    private String queue;
    // 程序的状态
    private String state;
    // 程序的最终状态
    private String finalStatus;
    // 执行进度
    private Double progress;
    // 程序的ui
    private String trackingUI;
    // 程序ui的url地址
    private String trackingUrl;
    // 诊断
    private String diagnostics;
    // 集群id
    private Long clusterId;
    // 程序类型（spark、mr）
    private String applicationType;
    // 程序的标签
    private String applicationTags;
    // 程序启动时间
    private Long startedTime;
    // 程序结束时间
    private Long finishedTime;
    // 程序执行时间
    private Long elapsedTime;
    // master 的日志路径
    private String amContainerLogs;
    // master所在主机host名称
    private String amHostHttpAddress;
    // 已分配的内存大小
    private Long allocatedMB;
    // 已分配的cpu数量
    private Long allocatedVCores;
    // 运行的container数量
    private Long runningContainers;
    // 内存时间
    private Long memorySeconds;
    // cpu时间
    private Long vcoreSeconds;
    // 占用的内存大小
    private Long preemptedResourceMB;
    // 占用的cpu数量
    private Long preemptedResourceVCores;
    private Long numNonAMContainerPreempted;
    private Long numAMContainerPreempted;
    // yarn的日志聚合状态（NOT_START、SUCCEEDED）
    private String logAggregationStatus;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getFinalStatus() {
        return finalStatus;
    }

    public void setFinalStatus(String finalStatus) {
        this.finalStatus = finalStatus;
    }

    public Double getProgress() {
        return progress;
    }

    public void setProgress(Double progress) {
        this.progress = progress;
    }

    public String getTrackingUI() {
        return trackingUI;
    }

    public void setTrackingUI(String trackingUI) {
        this.trackingUI = trackingUI;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public Long getClusterId() {
        return clusterId;
    }

    public void setClusterId(Long clusterId) {
        this.clusterId = clusterId;
    }

    public String getApplicationType() {
        return applicationType;
    }

    public void setApplicationType(String applicationType) {
        this.applicationType = applicationType;
    }

    public String getApplicationTags() {
        return applicationTags;
    }

    public void setApplicationTags(String applicationTags) {
        this.applicationTags = applicationTags;
    }

    public Long getStartedTime() {
        return startedTime;
    }

    public void setStartedTime(Long startedTime) {
        this.startedTime = startedTime;
    }

    public Long getFinishedTime() {
        return finishedTime;
    }

    public void setFinishedTime(Long finishedTime) {
        this.finishedTime = finishedTime;
    }

    public Long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(Long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public String getAmContainerLogs() {
        return amContainerLogs;
    }

    public void setAmContainerLogs(String amContainerLogs) {
        this.amContainerLogs = amContainerLogs;
    }

    public String getAmHostHttpAddress() {
        return amHostHttpAddress;
    }

    public void setAmHostHttpAddress(String amHostHttpAddress) {
        this.amHostHttpAddress = amHostHttpAddress;
    }

    public Long getAllocatedMB() {
        return allocatedMB;
    }

    public void setAllocatedMB(Long allocatedMB) {
        this.allocatedMB = allocatedMB;
    }

    public Long getAllocatedVCores() {
        return allocatedVCores;
    }

    public void setAllocatedVCores(Long allocatedVCores) {
        this.allocatedVCores = allocatedVCores;
    }

    public Long getRunningContainers() {
        return runningContainers;
    }

    public void setRunningContainers(Long runningContainers) {
        this.runningContainers = runningContainers;
    }

    public Long getMemorySeconds() {
        return memorySeconds;
    }

    public void setMemorySeconds(Long memorySeconds) {
        this.memorySeconds = memorySeconds;
    }

    public Long getVcoreSeconds() {
        return vcoreSeconds;
    }

    public void setVcoreSeconds(Long vcoreSeconds) {
        this.vcoreSeconds = vcoreSeconds;
    }

    public Long getPreemptedResourceMB() {
        return preemptedResourceMB;
    }

    public void setPreemptedResourceMB(Long preemptedResourceMB) {
        this.preemptedResourceMB = preemptedResourceMB;
    }

    public Long getPreemptedResourceVCores() {
        return preemptedResourceVCores;
    }

    public void setPreemptedResourceVCores(Long preemptedResourceVCores) {
        this.preemptedResourceVCores = preemptedResourceVCores;
    }

    public Long getNumNonAMContainerPreempted() {
        return numNonAMContainerPreempted;
    }

    public void setNumNonAMContainerPreempted(Long numNonAMContainerPreempted) {
        this.numNonAMContainerPreempted = numNonAMContainerPreempted;
    }

    public Long getNumAMContainerPreempted() {
        return numAMContainerPreempted;
    }

    public void setNumAMContainerPreempted(Long numAMContainerPreempted) {
        this.numAMContainerPreempted = numAMContainerPreempted;
    }

    public String getLogAggregationStatus() {
        return logAggregationStatus;
    }

    public void setLogAggregationStatus(String logAggregationStatus) {
        this.logAggregationStatus = logAggregationStatus;
    }
}
