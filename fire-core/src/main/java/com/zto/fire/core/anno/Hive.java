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
 * 基于注解进行Hive connector配置，优先级低于配置文件，高于@Config注解
 *
 * @author ChengLong
 * @Date 2022-04-26 13:46:00
 * @since 2.2.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Hive {

    /**
     * hive连接别名：hive.cluster
     */
    String value() default "";

    /**
     * hive连接别名：hive.cluster，同value
     */
    String cluster() default "";

    /**
     * hive的版本：hive.version
     */
    String version() default "";

    /**
     * 在flink中hive的catalog名称：hive.catalog.name
     */
    String catalog() default "";

    /**
     * 分区名称（dt、ds）：default.table.partition.name
     */
    String partition() default "";
}
