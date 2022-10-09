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

package org.apache.flink.connector.clickhouse.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.clickhouse.internal.common.DistributedEngineFullSchema;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PROPERTIES_PREFIX;

/** clickhouse util. */
public class ClickHouseUtil {

    public static final String EMPTY = "";

    private static final LocalDate DATE_PREFIX_OF_TIME = LocalDate.ofEpochDay(1);

    // 匹配不带宏的分布式表cluster名称
    private static final Pattern DISTRIBUTED_TABLE_ENGINE_PATTERN =
            Pattern.compile(
                    "Distributed\\((?<cluster>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<database>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<table>[a-zA-Z_][0-9a-zA-Z_]*)");

    //匹配带宏的分布式表cluster名称
    private static final Pattern DISTRIBUTED_TABLE_ENGINE_MACROS_PATTERN =
            Pattern.compile(
                    "Distributed\\((?<cluster>\\{[a-zA-Z_][0-9a-zA-Z_]*\\}),\\s*(?<database>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<table>[a-zA-Z_][0-9a-zA-Z_]*)");

    private static final String QUERY_TABLE_ENGINE_SQL =
            "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?";

    // 查询宏对应实际的集群名称
    private static final String QUERY_MACRO_CLUSTER_SQL =
            "select substitution from system.macros where macro=?";

    public static String getJdbcUrl(String url, @Nullable String database) {
        try {
            database = database != null ? database : "";
            return url + "/" + database;
            //return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Cannot parse url: %s", url), e);
        }
    }

    public static DistributedEngineFullSchema getAndParseDistributedEngineSchema(
            ClickHouseConnection connection, String databaseName, String tableName)
            throws SQLException {

        String engineFull = "";
        // 匹配不带宏的分布式表信息
        try (PreparedStatement stmt = connection.prepareStatement(QUERY_TABLE_ENGINE_SQL)) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    engineFull = rs.getString("engine_full");
                    Matcher matcher =
                            DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull.replace("'", ""));
                    if (matcher.find()) {
                        String cluster = "cluster";
                        String database = matcher.group("database");
                        String table = matcher.group("table");
                        return DistributedEngineFullSchema.of(cluster, database, table);
                    }
                }
            }
        }

        // 匹配带宏的分布式表信息
        Matcher matcherMacro =
                DISTRIBUTED_TABLE_ENGINE_MACROS_PATTERN.matcher(engineFull.replace("'", ""));
        if (matcherMacro.find()) {
            String macroCluster = "cluster";
            String cluster = "";
            if (StringUtils.isNotBlank(macroCluster)) {
                // 根据宏变量查询system.macro表获取真正的虚拟集群名称
                try (PreparedStatement stmt = connection.prepareStatement(QUERY_MACRO_CLUSTER_SQL)) {
                    stmt.setString(1, macroCluster);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            cluster = rs.getString("substitution");
                        }
                    }
                }
            }
            if (StringUtils.isBlank(cluster)) {
                throw new IllegalStateException("没有获取到宏替换真正的集群名称");
            }
            String database = matcherMacro.group("database");
            String table = matcherMacro.group("table");
            return DistributedEngineFullSchema.of(cluster, database, table);
        } else {
            return null;
        }
    }

    public static Properties getClickHouseProperties(Map<String, String> tableOptions) {
        final Properties properties = new Properties();

        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring((PROPERTIES_PREFIX).length());
                            properties.setProperty(subKey, value);
                        });
        return properties;
    }

    public static Timestamp toFixedDateTimestamp(LocalTime localTime) {
        LocalDateTime localDateTime = localTime.atDate(DATE_PREFIX_OF_TIME);
        return Timestamp.valueOf(localDateTime);
    }

    public static String quoteIdentifier(String identifier) {
        return String.join(EMPTY, "`", identifier, "`");
    }
}
