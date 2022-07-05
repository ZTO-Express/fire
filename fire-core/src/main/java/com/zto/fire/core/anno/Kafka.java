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

package com.zto.fire.core.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 基于注解进行Kafka connector配置，优先级低于配置文件，高于@Config注解
 *
 * @author ChengLong 2022-04-26 13:36:00
 * @since 2.2.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Kafka {

    /**
     * kafka集群连接信息，同value
     */
    String brokers();

    /**
     * kafka topics，多个使用逗号分隔
     */
    String topics();

    /**
     * 消费者标识
     */
    String groupId();

    /**
     * 指定从何处开始消费
     */
    String startingOffset() default "";

    /**
     * 指定消费到何处结束
     */
    String endingOffsets() default "";

    /**
     * 是否开启主动提交offset
     */
    boolean autoCommit() default false;

    /**
     * session超时时间（ms）
     */
    long sessionTimeout() default -1;

    /**
     * request超时时间（ms）
     */
    long requestTimeout() default -1;

    /**
     * poll的周期（ms）
     */
    long pollInterval() default -1;

    /**
     * 从指定的时间戳开始消费
     */
    long startFromTimestamp() default -1;

    /**
     * 指定从kafka中保持的offset开始继续消费
     */
    boolean startFromGroupOffsets() default false;

    /**
     * 是否强制覆盖checkpoint中保持的offset信息，从指定位置开始消费
     */
    boolean forceOverwriteStateOffset() default false;

    /**
     * 是否在开启checkpoint的情况下强制周期性提交offset到kafka
     */
    boolean forceAutoCommit() default false;

    /**
     * 强制提交的周期（ms）
     */
    long forceAutoCommitInterval() default -1;

    /**
     * kafka-client参数，以key=value形式注明
     */
    String[] config() default "";

}
