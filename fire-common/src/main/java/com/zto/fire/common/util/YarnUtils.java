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

package com.zto.fire.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Yarn相关工具类
 * @author ChengLong 2018年8月10日 16:03:29
 */
public class YarnUtils {

    private YarnUtils() {}
    /**
     * 使用正则提取日志中的applicationId
     * @param log
     * @return
     */
    public static String getAppId(String log) {
        // 正则表达式规则
        String regEx = "application_[0-9]+_[0-9]+";
        // 编译正则表达式
        Pattern pattern = Pattern.compile(regEx);
        // 忽略大小写的写法
        Matcher matcher = pattern.matcher(log);
        // 查找字符串中是否有匹配正则表达式的字符/字符串
        if(matcher.find()) {
            return matcher.group();
        } else {
            return "";
        }
    }
}
