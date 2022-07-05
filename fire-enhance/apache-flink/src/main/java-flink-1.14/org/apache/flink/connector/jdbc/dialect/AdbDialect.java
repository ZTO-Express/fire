/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.dialect;

import java.util.Optional;

/** JDBC dialect for ADB. */
public class AdbDialect extends MySQLDialect {
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    public boolean canHandle(String url, String dialectOption) {
        return (url.startsWith("jdbc:mysql:") && url.contains("aliyuncs.com"));
    }

    /**
     * @return the default driver class name, if user not configure the driver class name, then will
     *     use this one.
     */
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /**
     * Mysql upsert query use DUPLICATE KEY UPDATE.
     *
     * <p>NOTE: It requires Mysql's primary key to be consistent with pkFields.
     *
     * <p>We don't use REPLACE INTO, if there are other fields, we can keep their previous values.
     */
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.of(getReplaceIntoStatement(tableName, fieldNames));
    }
}
