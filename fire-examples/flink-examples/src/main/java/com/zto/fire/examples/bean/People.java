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

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

public class People {
    private Long id;
    private String name;
    private Integer age;
    private Double length;
    private BigDecimal data;

    public People() {
    }

    public People(Long id, String name, Integer age, Double length, BigDecimal data) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
        this.data = data;
    }

    public static List<People> createList() {
        List<People> list = new LinkedList<>();
        for (int i=0; i<10; i++) {
            list.add(new People((long) i, "admin_" + i, i, i * 0.1, new BigDecimal(i * 10.1012)));
        }
        return list;
    }
}
