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
 * 用于枚举常见数据库的jdbc驱动类
 *
 * @author ChengLong 2022-04-26 14:58:17
 */
public enum JdbcDriver {
    mysql("com.mysql.jdbc.Driver"),
    tidb("com.mysql.jdbc.Driver"),
    sqlserver("com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    oracle("oracle.jdbc.driver.OracleDriver"),
    hive("org.apache.hive.jdbc.HiveDriver"),
    presto("com.facebook.presto.jdbc.PrestoDriver"),
    spark("org.apache.hive.jdbc.HiveDriver"),
    clickhouse("ru.yandex.clickhouse.ClickHouseDriver"),
    postgreSql("org.postgresql.Driver"),
    impala("com.cloudera.impala.jdbc41.Driver"),
    automatic("");

    private String driver;

    JdbcDriver(String driver) {
        this.driver = driver;
    }

    public String getDriver() {
        return driver;
    }
}
