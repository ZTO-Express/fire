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

import org.apache.commons.lang3.StringUtils;

/**
 * 数据源类型
 *
 * @author ChengLong
 * @create 2020-07-07 16:36
 * @since 1.0.0
 */
public enum Datasource {
    HIVE(1), HBASE(2), KAFKA(3), ROCKETMQ(4), REDIS(5),
    ES(6), MYSQL(7), TIDB(8), ORACLE(9), SQLSERVER(10),
    DB2(11), CLICKHOUSE(12), PRESTO(13), KYLIN(14), DERBY(15), UNKNOWN(20);

    Datasource(int type) {
    }

    /**
     * 将字符串解析成指定的枚举类型
     */
    public static Datasource parse(String dataSource) {
        if (StringUtils.isBlank(dataSource)) return UNKNOWN;
        try {
            return Enum.valueOf(Datasource.class, dataSource.trim().toUpperCase());
        } catch (Exception e) {
            return UNKNOWN;
        }
    }

}
