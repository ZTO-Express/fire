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

import java.util.Objects;

/**
 * 用于封装采集到SQL的实时血缘信息：描述表与表之间的关系，如：insert overwrite sinkTable select xxx from srcTable
 *
 * @author ChengLong 2022-09-01 13:31:23
 * @since 2.3.2
 */
public class SQLTableRelations {

    /**
     * 源表：SELECT
     */
    private String srcTable;

    /**
     * 目标表：INSERT、CREATE
     */
    private String sinkTable;

    public SQLTableRelations() {
    }

    public SQLTableRelations(String srcTable, String sinkTable) {
        this.srcTable = srcTable;
        this.sinkTable = sinkTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SQLTableRelations that = (SQLTableRelations) o;
        return Objects.equals(srcTable, that.srcTable) && Objects.equals(sinkTable, that.sinkTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcTable, sinkTable);
    }
}