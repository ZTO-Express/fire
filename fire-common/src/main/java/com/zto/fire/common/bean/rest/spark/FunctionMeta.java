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

package com.zto.fire.common.bean.rest.spark;

/**
 * 用于封装函数元数据信息
 * @author ChengLong 2019-9-2 16:50:50
 */
public class FunctionMeta {
    // 函数描述
    private String description;
    // 数据库
    private String database;
    // 函数名称
    private String name;
    // 函数定义的类
    private String className;
    // 是否为临时函数
    private Boolean isTemporary;

    public FunctionMeta() {
    }

    public FunctionMeta(String description, String database, String name, String className, Boolean isTemporary) {
        this.description = description;
        this.database = database;
        this.name = name;
        this.className = className;
        this.isTemporary = isTemporary;
    }

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Boolean getTemporary() {
        return isTemporary;
    }

    public void setTemporary(Boolean temporary) {
        isTemporary = temporary;
    }
}
