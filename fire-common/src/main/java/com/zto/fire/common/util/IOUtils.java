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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * io流工具类
 *
 * @author ChengLong 2019-3-27 11:17:56
 */
public class IOUtils {
    private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils() {}

    /**
     * 关闭多个流
     */
    public static void close(Closeable... closeables) {
        if (closeables != null && closeables.length > 0) {
            for (Closeable io : closeables) {
                try {
                    if (io != null) {
                        io.close();
                    }
                } catch (Exception e) {
                    logger.error("close 对象失败", e);
                }
            }
        }
    }

    /**
     * 关闭多个process对象
     */
    public static void close(Process... process) {
        if (process != null && process.length > 0) {
            for (Process pro : process) {
                try {
                    if (pro != null) {
                        pro.destroy();
                    }
                } catch (Exception e) {
                    logger.error("close process 对象失败", e);
                }
            }
        }
    }
}
