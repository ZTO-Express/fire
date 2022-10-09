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

import com.zto.fire.common.util.DatasourceDesc;

import java.util.LinkedList;
import java.util.List;

/**
 * 用于封装采集到SQL的实时血缘信息
 *
 * @author ChengLong 2022-09-01 13:30:22
 * @since 2.3.2
 */
public class SQLLineage implements DatasourceDesc {

    /**
     * 待解析的SQL语句
     */
    private List<String> statements;

    /**
     * 解析SQL中表的信息
     */
    private List<SQLTable> tables;

    /**
     * 描述表与表之前的数据血缘关系
     */
    private List<SQLTableRelations> relations;

    public SQLLineage() {
        this.statements = new LinkedList<>();
        this.tables = new LinkedList<>();
        this.relations = new LinkedList<>();
    }

    public List<String> getStatements() {
        return statements;
    }

    public void setStatements(List<String> statements) {
        this.statements = statements;
    }

    public void setTables(List<SQLTable> tables) {
        this.tables = tables;
    }

    public List<SQLTable> getTables() {
        return tables;
    }

    public void setRelations(List<SQLTableRelations> relations) {
        this.relations = relations;
    }

    public List<SQLTableRelations> getRelations() {
        return relations;
    }

}