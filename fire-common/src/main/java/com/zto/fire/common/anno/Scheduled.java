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

package com.zto.fire.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 定时任务注解，放在方法上，要求方法不带参数，且无返回值
 * 优先级：cron > fixedInterval   startAt > initialDelay
 * @author ChengLong 2019年11月4日 21:12:06
 * @since 0.3.5
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Scheduled {

    /**
     * cron表达式
     */
    String cron() default "";

    /**
     * 指定是否允许并发执行同一个任务
     * 默认为true，表示同一时间范围内同一个任务可以有多个实例并行执行
     */
    boolean concurrent() default true;

    /**
     * 按照给定的时间间隔（毫秒）周期性执行
     */
    long fixedInterval() default -1;

    /**
     * 周期性执行的次数，-1表示无限重复执行
     */
    long repeatCount() default -1;

    /**
     * 第一次延迟多久（毫秒）执行，0表示立即执行
     */
    long initialDelay() default -1;

    /**
     * 用于指定首次开始执行的时间，优先级高于initialDelay
     * 日期的格式为：yyyy-MM-dd HH:mm:ss
     */
    String startAt() default "";

    /**
     * 定时任务的作用域，driver、executor、all
     * 默认仅driver端执行
     */
    String scope() default "driver";
}
