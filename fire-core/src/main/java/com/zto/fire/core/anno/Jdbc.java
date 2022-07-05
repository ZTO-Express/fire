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
 * 基于注解进行Jdbc connector配置，优先级低于配置文件，高于@Config注解
 *
 * @author ChengLong 2022-04-26 13:56:00
 * @since 2.2.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Jdbc {

    /**
     * Jdbc的url，同value
     */
    String url();

    /**
     * jdbc 驱动类，不填可根据url自动推断
     */
    String driver() default "";

    /**
     * jdbc的用户名
     */
    String username();

    /**
     * jdbc的密码
     */
    String password() default "";

    /**
     * 事务的隔离级别
     */
    String isolationLevel() default "";

    /**
     * 连接池的最大连接数
     */
    int maxPoolSize() default -1;

    /**
     * 连接池最少连接数
     */
    int minPoolSize() default -1;

    /**
     * 连接池初始连接数
     */
    int initialPoolSize() default -1;

    /**
     * 连接池的增量
     */
    int acquireIncrement() default -1;

    /**
     * 连接的最大空闲时间
     */
    int maxIdleTime() default -1;

    /**
     * 多少条操作一次
     */
    int batchSize() default -1;

    /**
     * flink引擎：flush的间隔周期（ms）
     */
    long flushInterval() default -1;

    /**
     * flink引擎：失败最大重试次数
     */
    int maxRetries() default -1;

    /**
     * spark引擎：scan后的缓存级别：fire.jdbc.storage.level
     */
    String storageLevel() default "";

    /**
     * spark引擎：select后存放到rdd的多少个partition中：fire.jdbc.query.partitions
     */
    int queryPartitions() default -1;

    /**
     * 日志中打印的sql长度
     */
    int logSqlLength() default -1;

    /**
     * c3p0参数，以key=value形式注明
     */
    String[] config() default "";

}
