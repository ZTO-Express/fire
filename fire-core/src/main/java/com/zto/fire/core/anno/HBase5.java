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
 * 基于注解进行Hbase connector配置，优先级低于配置文件，高于@Config注解
 *
 * @author ChengLong 2022-06-16 14:36:00
 * @since 2.2.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBase5 {

    /**
     * HBase集群连接信息：hbase.cluster
     */
    String value() default "";

    /**
     * HBase集群连接信息：hbase.cluster，同value
     */
    String cluster() default "";

    /**
     * 列族名称：hbase.column.family
     */
    String family() default "";

    /**
     * 每个线程最多insert的记录数：fire.hbase.batch.size
     */
    int batchSize() default -1;

    /**
     * spark引擎：scan hbase后存放到rdd的多少个partition中：fire.hbase.scan.partitions
     */
    int scanPartitions() default -1;

    /**
     * spark引擎：scan后的缓存级别：fire.hbase.storage.level
     */
    String storageLevel() default "";

    /**
     * flink引擎：sink hbase失败最大重试次数：hbase.max.retry
     */
    int maxRetries() default -1;

    /**
     * WAL等级：hbase.durability
     */
    String durability() default "";

    /**
     * 是否启用表信息缓存，提高表是否存在判断的效率：fire.hbase.table.exists.cache.enable
     */
    boolean tableMetaCache() default true;

    /**
     * hbase-client参数，以key=value形式注明
     */
    String[] config() default "";

}
