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

import com.zto.fire.common.anno.FieldName;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.JSONUtils;
import com.zto.fire.hbase.bean.HBaseBaseBean;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 对应HBase表的JavaBean
 *
 * @author ChengLong 2019-6-20 16:06:16
 */
// @HConfig(multiVersion = true)
public class Student extends HBaseBaseBean<Student> {
    private Long id;
    private String name;
    private Integer age;
    // 多列族情况下需使用family单独指定
    private String createTime;
    // 若JavaBean的字段名称与HBase中的字段名称不一致，需使用value单独指定
    // 此时hbase中的列名为length1，而不是length
    @FieldName(family = "data", value = "length1")
    private BigDecimal length;
    private Boolean sex;

    /**
     * rowkey的构建
     *
     * @return
     */
    @Override
    public Student buildRowKey() {
        this.rowKey = this.id.toString();
        return this;
    }

    public Student(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Student(Long id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Student(Long id, String name, Integer age, BigDecimal length, Boolean sex, String createTime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
        this.sex = sex;
        this.createTime = createTime;
    }

    public Student(Long id, String name, Integer age, BigDecimal length) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
    }

    public Student() {

    }

    public Student(Long id) {
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

    public static List<Student> newStudentList() {
        String dateTime = DateFormatUtils.formatCurrentDateTime();
        return Arrays.asList(
                new Student(1L, "admin", 12, BigDecimal.valueOf(12.1), true, dateTime),
                new Student(2L, "root", 22, BigDecimal.valueOf(22), true, dateTime),
                new Student(3L, "scala", 11, BigDecimal.valueOf(11), true, dateTime),
                new Student(4L, "spark", 15, BigDecimal.valueOf(15), true, dateTime),
                new Student(5L, "java", 16, BigDecimal.valueOf(16.1), true, dateTime),
                new Student(6L, "hive", 17, BigDecimal.valueOf(17.1), true, dateTime),
                new Student(7L, "presto", 18, BigDecimal.valueOf(18.1), true, dateTime),
                new Student(8L, "flink", 19, BigDecimal.valueOf(19.1), true, dateTime),
                new Student(9L, "streaming", 10, BigDecimal.valueOf(10.1), true, dateTime),
                new Student(10L, "sql", 12, BigDecimal.valueOf(12.1), true, dateTime)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Student)) return false;
        Student student = (Student) o;
        return Objects.equals(id, student.id) &&
                Objects.equals(name, student.name) &&
                Objects.equals(age, student.age) &&
                Objects.equals(createTime, student.createTime) &&
                Objects.equals(length, student.length) &&
                Objects.equals(sex, student.sex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, age, createTime, length, sex);
    }
}
