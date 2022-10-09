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

package com.zto.fire.common.bean.lineage;

import java.util.*;

/**
 * 用于封装采集到SQL的实时血缘信息：SQL中所用到的表信息
 *
 * @author ChengLong 2022-09-01 13:32:03
 * @since 2.3.2
 */
public class SQLTable {

    /**
     * Hive、Kafka、JDBC等
     */
    private String catalog;

    /**
     * catalog集群信息url
     */
    private String cluster;

    /**
     * 物理表名
     */
    private String physicalTable;

    /**
     * 在spark或flink中注册成的临时表名
     */
    private String tmpView;

    /**
     * 表注释信息
     */
    private String comment;

    /**
     * sql中的属性信息，比如with字句的options
     */
    private Map<String, String> options;

    /**
     * 任务中对该表的操作：SELECT、DROP、CREATE等
     */
    private Set<String> operation;

    /**
     * 使用到的字段列表，包括字段的名称与类型
     */
    private Set<SQLTableColumns> columns;

    /**
     * 使用到的分区信息
     */
    private Set<SQLTablePartitions> partitions;

    public SQLTable() {
        this.operation = new HashSet<>();
        this.columns = new HashSet<>();
        this.options = new HashMap<>();
        this.partitions = new HashSet<>();
    }

    public SQLTable(String physicalTable) {
        this();
        this.physicalTable = physicalTable;
    }

    public SQLTable(String catalog, String cluster, String physicalTable, String tmpView, String comment,
                    HashMap<String, String> options, HashSet<String> operation, HashSet<SQLTableColumns> columns, HashSet<SQLTablePartitions> partitions) {
        this.catalog = catalog;
        this.cluster = cluster;
        this.physicalTable = physicalTable;
        this.tmpView = tmpView;
        this.options = options;
        this.operation = operation;
        this.columns = columns;
        this.partitions = partitions;
        this.comment = comment;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getCluster() {
        return cluster;
    }

    public void setPhysicalTable(String physicalTable) {
        this.physicalTable = physicalTable;
    }

    public String getPhysicalTable() {
        return physicalTable;
    }

    public void setTmpView(String tmpView) {
        this.tmpView = tmpView;
    }

    public String getTmpView() {
        return tmpView;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public void setOperation(Set<String> operation) {
        this.operation = operation;
    }

    public Set<String> getOperation() {
        return operation;
    }

    public void setColumns(HashSet<SQLTableColumns> columns) {
        this.columns = columns;
    }

    public Set<SQLTableColumns> getColumns() {
        return columns;
    }

    public Set<SQLTablePartitions> getPartitions() {
        return partitions;
    }

    public void setPartitions(HashSet<SQLTablePartitions> partitions) {
        this.partitions = partitions;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}