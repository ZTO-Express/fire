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

package com.zto.fire.examples.bean;

import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.JSONUtils;
import com.zto.fire.hbase.anno.HConfig;
import com.zto.fire.hbase.bean.HBaseBaseBean;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 对应HBase表的JavaBean
 *
 * @author ChengLong
 * @date 2022-05-11 13:41:42
 * @since 2.2.2
 */
@HConfig(nullable = true, multiVersion = true, versions = 3)
public class StudentMulti extends HBaseBaseBean<StudentMulti> {
    protected Long id;
    protected String name;
    protected Integer age;
    // 多列族情况下需使用family单独指定
    protected String createTime;
    // 若JavaBean的字段名称与HBase中的字段名称不一致，需使用value单独指定
    // 此时hbase中的列名为length1，而不是length
    //@FieldName(family = "data", value = "length1")
    protected BigDecimal length;
    protected Boolean sex;

    /**
     * rowkey的构建
     *
     * @return
     */
    @Override
    public StudentMulti buildRowKey() {
        this.rowKey = this.id.toString();
        return this;
    }

    public StudentMulti(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public StudentMulti(Long id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public StudentMulti(Long id, String name, Integer age, BigDecimal length, Boolean sex, String createTime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
        this.sex = sex;
        this.createTime = createTime;
    }

    public StudentMulti(Long id, String name, Integer age, BigDecimal length) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
    }

    public StudentMulti() {

    }

    public StudentMulti(Long id) {
        this.id = id;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public BigDecimal getLength() {
        return length;
    }

    public void setLength(BigDecimal length) {
        this.length = length;
    }

    public Boolean getSex() {
        return sex;
    }

    public void setSex(Boolean sex) {
        this.sex = sex;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return JSONUtils.toJSONString(this);
    }

    public List<StudentMulti> generate() {
        return newStudentMultiList();
    }

    public static List<StudentMulti> newStudentMultiList() {
        String dateTime = DateFormatUtils.formatCurrentDateTime();
        return Arrays.asList(
                new StudentMulti(1L, "admin", 12, BigDecimal.valueOf(12.1), true, dateTime),
                new StudentMulti(2L, "root", 22, BigDecimal.valueOf(22), true, dateTime),
                new StudentMulti(3L, "scala", 11, BigDecimal.valueOf(11), true, dateTime),
                new StudentMulti(4L, "spark", 15, BigDecimal.valueOf(15), true, dateTime),
                new StudentMulti(5L, "java", 16, BigDecimal.valueOf(16.1), true, dateTime),
                new StudentMulti(6L, "hive", 17, BigDecimal.valueOf(17.1), true, dateTime),
                new StudentMulti(7L, "presto", 18, BigDecimal.valueOf(18.1), true, dateTime),
                new StudentMulti(8L, "flink", 19, BigDecimal.valueOf(19.1), true, dateTime),
                new StudentMulti(9L, "streaming", 10, BigDecimal.valueOf(10.1), true, dateTime)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StudentMulti)) {
            return false;
        }
        StudentMulti StudentMulti = (StudentMulti) o;
        return Objects.equals(id, StudentMulti.id) &&
                Objects.equals(name, StudentMulti.name) &&
                Objects.equals(age, StudentMulti.age) &&
                Objects.equals(createTime, StudentMulti.createTime) &&
                Objects.equals(length, StudentMulti.length) &&
                Objects.equals(sex, StudentMulti.sex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, age, createTime, length, sex);
    }
}
