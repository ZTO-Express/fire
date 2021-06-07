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

package com.zto.fire.common.util;

import java.math.*;

/**
 * 数据计算工具类
 *
 * @author ChengLong 2019年9月29日 13:50:31
 */
public class MathUtils {

    private MathUtils() {}

    /**
     * 计算百分比，并保留指定的小数位
     *
     * @param molecule    分子
     * @param denominator 分母
     * @param scale       精度
     * @return 百分比
     */
    public static double percent(long molecule, long denominator, int scale) {
        if (molecule == 0 || denominator == 0) {
            return 0.00;
        }
        return BigDecimal.valueOf(100.00 * molecule / denominator).setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }

    /**
     * 将指定double类型数据以四舍五入的方式保留指定的精度
     *
     * @param data  数据
     * @param scale 精度
     * @return 四舍五入后的数据
     */
    public static double doubleScale(double data, int scale) {
        return BigDecimal.valueOf(data).setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }
}
