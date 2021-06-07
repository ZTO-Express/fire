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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;

/**
 * 执行命令的工具
 *
 * @author ChengLong 2019-4-10 15:50:23
 */
public class ProcessUtil {
    private static final Logger logger = LoggerFactory.getLogger(ProcessUtil.class);

    private ProcessUtil() {}

    /**
     * 执行多条linux命令，不返回命令执行日志
     *
     * @param commands linux命令
     * @return 命令执行结果的一行数据
     */
    public static void executeCmds(String... commands) {
        Objects.requireNonNull(commands, "命令不能为空");
        for (String command : commands) {
            executeCmdForLine(command);
        }
    }

    /**
     * 执行一条linux命令，仅返回命令的一行
     *
     * @param cmd linux命令
     * @return 命令执行结果的一行数据
     */
    public static String executeCmdForLine(String cmd) {
        if (!OSUtils.isLinux() || StringUtils.isBlank(cmd)) {
            // 如果是windows环境
            return " <windows environment.> ";
        }
        Process process = null;
        BufferedReader reader = null;
        String result = "";
        try {
            process = Runtime.getRuntime().exec(cmd);
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                if (StringUtils.isNotBlank(line)) {
                    result = line;
                }
            }
        } catch (Exception e) {
            logger.error("执行命令报错", e);
        } finally {
            IOUtils.close(process);
            IOUtils.close(reader);
        }
        return result;
    }
}
