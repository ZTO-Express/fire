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

package com.zto.fire.flink.ext.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * 基于AssignerWithPeriodicWatermarks的封装
 * 有参的构造方法允许定义允许的最大乱序时间（单位ms）
 * maxTimestamp用于自定义水位线的时间，若不指定，则以系统当前时间为水位线值
 *
 * @author ChengLong 2020-4-17 17:18:33
 */
public abstract class FirePeriodicWatermarks<T> implements AssignerWithPeriodicWatermarks<T> {
    // 用于计算水位线值，若为0则取当前系统时间
    protected long maxTimestamp = 0;
    // 允许最大的乱序时间，默认10s
    protected long maxOutOfOrder = 10 * 1000L;
    // 当前水位线的引用
    protected transient Watermark watermark = new Watermark(System.currentTimeMillis());

    protected FirePeriodicWatermarks() {
    }

    /**
     * 用于自定义允许最大的乱序时间
     *
     * @param maxOutOfOrder 用户定义的最大乱序时间
     */
    protected FirePeriodicWatermarks(long maxOutOfOrder) {
        if (maxOutOfOrder > 0) {
            this.maxOutOfOrder = maxOutOfOrder;
        }
    }

    /**
     * 计算并返回当前的水位线
     * 如果未指定水位线的时间戳，则默认获取当前系统时间
     */
    @Override
    public Watermark getCurrentWatermark() {
        if (this.maxTimestamp == 0) {
            this.watermark = new Watermark(System.currentTimeMillis() - this.maxOutOfOrder);
        } else {
            this.watermark = new Watermark(this.maxTimestamp - this.maxOutOfOrder);
        }

        return this.watermark;
    }
}
