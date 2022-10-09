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

import com.zto.fire.common.bean.FireTask;

/**
 * 用于封装采集到的实时血缘信息
 *
 * @author ChengLong 2022-08-30 15:31:32
 * @since 2.3.2
 */
public class Lineage extends FireTask {

    /**
     * 血缘信息
     */
    private Object datasource;

    /**
     * SQL血缘
     */
    private SQLLineage sql;

    public Lineage() {
        super();
    }

    public Lineage(Object lineage) {
        super();
        this.datasource = lineage;
    }

    public Lineage(Object lineage, SQLLineage sql) {
        this.datasource = lineage;
        this.sql = sql;
    }

    public Object getDatasource() {
        return datasource;
    }

    public void setDatasource(Object datasource) {
        this.datasource = datasource;
    }

    public SQLLineage getSql() {
        return sql;
    }

    public void setSql(SQLLineage sql) {
        this.sql = sql;
    }
}
