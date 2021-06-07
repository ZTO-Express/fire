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

package com.zto.fire.hbase.bean;

import com.zto.fire.common.anno.FieldName;

import java.io.Serializable;

/**
 * HBase封装bean需实现该接口
 * Created by ChengLong on 2017-03-27.
 */
public abstract class HBaseBaseBean<T> implements Serializable {
    /**
     * rowKey字段
     */
    @FieldName(value = "rowKey", disuse = true)
    public String rowKey;

    /**
     * 子类包名+类名
     */
    @FieldName(value = "className", disuse = true)
    public final String className = this.getClass().getSimpleName();

    /**
     * 根据业务需要，构建rowkey
     */
    public abstract T buildRowKey();

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }
}
