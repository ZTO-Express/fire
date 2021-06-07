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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author ChengLong
 * @create 2021-02-07 16:45
 * @since 1.0.0
 */
public class Hudi {
    private Long id;
    private String name;
    private Integer age;
    private Boolean sex;
    private String createTime;
    private String ds;
    private static int num = 0;

    public Hudi(Long id, String name, Integer age, Boolean sex) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.createTime = DateFormatUtils.formatCurrentDateTime();
        if (num % 2 == 0) {
            this.ds = DateFormatUtils.formatBySchema(new Date(), "yyyyMMdd");
        } else {
            this.ds = "20200206";
        }
        num += 1;
    }

    public Hudi() {

    }

    public String getDs() {
        return ds;
    }

    public void setDs(String ds) {
        this.ds = ds;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public Hudi(Long id) {
        this.id = id;
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

    public static List<Hudi> newHudiList() {
        return Arrays.asList(
                new Hudi(1L, "admin", 12, true),
                new Hudi(2L, "root", 22, true),
                new Hudi(3L, "scala", 11, true),
                new Hudi(4L, "spark", 15, true),
                new Hudi(5L, "java", 16, true),
                new Hudi(6L, "hive", 17, true),
                new Hudi(7L, "presto", 18, true),
                new Hudi(8L, "flink", 19, true),
                new Hudi(9L, "streaming", 20, true),
                new Hudi(10L, "sql", 12, true)
        );
    }

    public static void main(String[] args) {
        LocalDateTime dateTime = LocalDateTime.of(2020, 2, 8, 15, 50, 30);
        dateTime.plusYears(1);
        System.out.println(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }
}
