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

package com.zto.fire.common.bean.analysis;

import com.zto.fire.common.util.*;

/**
 * 用于封装异常堆栈信息
 *
 * @author ChengLong 2022-08-01 09:28:04
 * @since 2.3.2
 */
public class ExceptionMsg {

    /**
     * 触发异常的执行引擎：spark/flink
     */
    private String engine;

    /**
     * 异常堆栈类名
     */
    private String exceptionClass;

    /**
     * 异常堆栈的标题
     */
    private String stackTitle;

    /**
     * 异常堆栈详细信息
     */
    private String stackTrace;

    /**
     * 发送异常的sql语句
     */
    private String sql;

    /**
     * 异常所在jvm进程发送的主机ip
     */
    private String ip;

    /**
     * 异常所属jvm进程所在的主机名称
     */
    private String hostname;

    /**
     * 进程的pid
     */
    private String pid;

    /**
     * 任务的主类名：package+类名
     */
    private String mainClass;

    /**
     * 异常发生的时间戳
     */
    private String timestamp;

    public ExceptionMsg(String stackTitle, String stackTrace, String exceptionClass, String sql) {
        this.stackTitle = stackTitle;
        this.stackTrace = stackTrace;
        this.exceptionClass = exceptionClass;
        this.sql = sql;
        this.engine = FireUtils.engine();
        this.ip = OSUtils.getIp();
        this.timestamp = DateFormatUtils.formatCurrentDateTime();
        this.mainClass = FireUtils.mainClass();
        this.hostname = OSUtils.getHostName();
        this.pid = OSUtils.getPid();
    }

    public ExceptionMsg(Throwable e, String sql) {
        this(e.getMessage(), ExceptionBus.stackTrace(e), e.getClass().getName(), sql);
    }

    public ExceptionMsg(Throwable e) {
        this(e.getMessage(), ExceptionBus.stackTrace(e), e.getClass().getName(), "");
    }

    public ExceptionMsg(String stackTitle, String stackTrace, String exceptionClass) {
        this(stackTitle, stackTrace, exceptionClass, "");
    }

    public ExceptionMsg(String stackTrace) {
        this("", stackTrace, "", "");
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getStackTitle() {
        return stackTitle;
    }

    public void setStackTitle(String stackTitle) {
        this.stackTitle = stackTitle;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return JSONUtils.toJSONString(this);
    }
}
