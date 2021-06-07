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

package com.zto.fire.common.enu;

/**
 * Fire任务类型
 *
 * @author ChengLong 2019-7-26 11:06:38
 */
public enum JobType {
    SPARK_CORE("spark_core"), SPARK_STREAMING("spark_streaming"), SPARK_STRUCTURED_STREAMING("spark_structured_streaming"), SPARK_SQL("spark_sql"), FLINK_STREAMING("flink_streaming"), FLINK_BATCH("flink_batch"), UNDEFINED("undefined");

    // 任务类型
    private String jobTypeDesc;

    JobType(String jobType) {
        this.jobTypeDesc = jobType;
    }

    /**
     * 获取当前任务的类型
     *
     * @return
     */
    public String getJobTypeDesc() {
        return this.jobTypeDesc;
    }

    /**
     * 用于判断当前任务是否为spark任务
     *
     * @return true: spark任务  false：非spark任务
     */
    public boolean isSpark() {
        return this.jobTypeDesc.contains("spark");
    }

    /**
     * 用于判断当前任务是否为flink任务
     *
     * @return true: flink任务  false：非flink任务
     */
    public boolean isFlink() {
        return this.jobTypeDesc.contains("flink");
    }
}
