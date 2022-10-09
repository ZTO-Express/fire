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
import com.zto.fire.common.util.JSONUtils;
import com.zto.fire.hbase.bean.HBaseBaseBean;
import com.zto.fire.common.util.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * 对应HBase表的JavaBean
 *
 * @author ChengLong 2019-6-20 16:06:16
 */
public class Student extends HBaseBaseBean<Student> {
    @FieldName(value = "Student", disuse = true)
    protected static final transient Logger logger = LoggerFactory.getLogger(Student.class);
    private Long id;
    private String name;
    private Integer age;
    // 多列族情况下需使用family单独指定
    private String createTime;
    // 若JavaBean的字段名称与HBase中的字段名称不一致，需使用value单独指定
    // 此时hbase中的列名为length1，而不是length
    // @FieldName(family = "info", value = "length1")
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

    public void setClassName(String name) {}

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

    /**
     * 构建student集合
     *
     * @return
     */
    public static List<Student> buildStudentList() {
        List<Student> studentList = new LinkedList<>();
        try {
            for (int i = 1; i <= 1; i++) {
                Thread.sleep(500);
                Student stu = new Student(1L, "root", i + 1, BigDecimal.valueOf((long) 1 + i), true, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }

            for (int i = 1; i <= 2; i++) {
                Thread.sleep(500);
                Student stu = new Student(2L, "admin", i + 2, BigDecimal.valueOf(2019.05180919 + i), false, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }

            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(3L, "spark", i + 3, BigDecimal.valueOf(33.1415926 + i));
                studentList.add(stu);
            }

            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(4L, "flink", i + 4, BigDecimal.valueOf(4.2 + i), true, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }

            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(5L, "hadoop", i + 5, BigDecimal.valueOf(5.5 + i), false, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }
            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(6L, "hbase", i + 6, BigDecimal.valueOf(66.66 + i), true, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }
        } catch (Exception e) {
            logger.error("Sleep线程异常", e);
        }

        return studentList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Student)) {
            return false;
        }
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
