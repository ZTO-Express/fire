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

package com.zto.fire.common.db.bean;

import com.zto.fire.common.anno.FieldName;
import com.zto.fire.common.util.JSONUtils;
import com.zto.fire.hbase.anno.HConfig;
import com.zto.fire.hbase.bean.HBaseBaseBean;
import com.zto.fire.common.util.DateFormatUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ChengLong
 * @create 2020-11-13 17:46
 * @since 1.0.0
 */
@HConfig(nullable = true, multiVersion = true)
public class Student extends HBaseBaseBean<Student> {
    private Long id;
    private String name;
    private Integer age;
    private BigDecimal height;
    @FieldName(family = "data", value = "timestamp")
    private String createTime;
    private String nullField;

    public Student() {
    }

    public Student(Long id, String name, Integer age, BigDecimal height) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.height = height;
        this.createTime = DateFormatUtils.formatCurrentDateTime();
    }

    public static List<Student> build(int count) {
        List<Student> list = new ArrayList<>(count);
        try {
            for (int i = 1; i <= count; i++) {
                list.add(new Student(Long.parseLong(i + ""), "root_" + i, i, new BigDecimal(i + "" + i + "." + i)));
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public Student buildRowKey() {
        this.rowKey = this.id.toString();
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Student)) return false;
        Student student = (Student) o;
        return id.equals(student.id);
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

    public BigDecimal getHeight() {
        return height;
    }

    public void setHeight(BigDecimal height) {
        this.height = height;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getNullField() {
        return nullField;
    }

    public void setNullField(String nullField) {
        this.nullField = nullField;
    }

    @Override
    public String toString() {
        return JSONUtils.toJSONString(this);
    }

}
