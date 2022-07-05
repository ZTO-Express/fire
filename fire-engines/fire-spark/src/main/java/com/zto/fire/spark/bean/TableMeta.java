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
 * 用于封装表的元数据
 * @author ChengLong 2019-9-2 13:11:56
 */
public class TableMeta {
    // 表的描述
    private String description;
    // 所在数据库名称
    private String database;
    // 表名
    private String tableName;
    // 表的类型
    private String tableType;
    // 是否为临时表
    private Boolean isTemporary;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public Boolean getTemporary() {
        return isTemporary;
    }

    public void setTemporary(Boolean temporary) {
        isTemporary = temporary;
    }

    public TableMeta() {
    }

    public TableMeta(String description, String database, String tableName, String tableType, Boolean isTemporary) {
        this.description = description;
        this.database = database;
        this.tableName = tableName;
        this.tableType = tableType;
        this.isTemporary = isTemporary;
    }
}
