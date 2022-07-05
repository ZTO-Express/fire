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
 * 基于注解进行RocketMQ connector配置，优先级低于配置文件，高于@Config注解
 *
 * @author ChengLong 2022-04-26 15:18:34
 * @since 2.2.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQ {

    /**
     * rocketmq集群连接信息
     */
    String brokers();

    /**
     * rocketmq topics，多个使用逗号分隔
     */
    String topics();

    /**
     * 消费者标识
     */
    String groupId();

    /**
     * 指定消费的tag
     */
    String tag() default "*";

    /**
     * 指定从何处开始消费
     */
    String startingOffset() default "";

    /**
     * 是否开启主动提交offset
     */
    boolean autoCommit() default false;

    /**
     * RocketMQ-client参数，以key=value形式注明
     */
    String[] config() default "";

}
