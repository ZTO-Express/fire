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

package com.zto.fire.common.bean.config;

import com.zto.fire.common.enu.ConfigureLevel;

import java.util.Map;

/**
 * 用于解析配置中心返回的配置项：
 *
 * {"code":200,"content":{"FRAMEWORK":{"fire.thread.pool.size":"5","hive.cluster":"batch"},"TASK":{"fire.user.conf":"test","fire.conf.show.enable":"false"},"URGENT":{"hdfs.ha.conf.test.dfs.nameservices":"ns1","hdfs.ha.conf.test.fs.defaultFS":"hdfs://ns1"}}}
 *
 * @author ChengLong 2021-8-23 15:26:39
 * @since 2.2.0
 */
public class ConfigurationParam {
    private Integer code;
    private Map<ConfigureLevel, Map<String, String>> content;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public Map<ConfigureLevel, Map<String, String>> getContent() {
        return content;
    }

    public void setContent(Map<ConfigureLevel, Map<String, String>> content) {
        this.content = content;
    }

}