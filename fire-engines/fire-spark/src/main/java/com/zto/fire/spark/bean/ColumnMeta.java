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

package com.zto.fire.spark.bean;

/**
 * 用于封装字段元数据
 *
 * @author ChengLong 2019-9-2 13:19:06
 */
public class ColumnMeta {
    // 所在数据库名称
    protected String database;
    // 表名
    protected String tableName;
    // 字段描述
    protected String description;
    // 字段名
    protected String columnName;
    // 字段类型
    protected String dataType;
    // 是否允许为空
    protected Boolean nullable;
    // 是否为分区字段
    protected Boolean isPartition;
    // 是否为bucket字段
    protected Boolean isBucket;

    public ColumnMeta() {
    }

    private ColumnMeta(Builder builder) {
        this.nullable = builder.nullable;
        this.tableName = builder.tableName;
        this.columnName = builder.columnName;
        this.database = builder.database;
        this.dataType = builder.dataType;
        this.description = builder.description;
        this.isBucket = builder.isBucket;
        this.isPartition = builder.isPartition;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDescription() {
        return description;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public Boolean getPartition() {
        return isPartition;
    }

    public Boolean getBucket() {
        return isBucket;
    }

    public static class Builder extends ColumnMeta {
        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder setDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder setNullable(Boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder setPartition(Boolean partition) {
            isPartition = partition;
            return this;
        }

        public Builder setBucket(Boolean bucket) {
            isBucket = bucket;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public ColumnMeta build() {
            return new ColumnMeta(this);
        }
    }
}