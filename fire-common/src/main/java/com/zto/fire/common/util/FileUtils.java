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

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * 文件操作工具类
 *
 * @author ChengLong 2018年8月22日 13:10:03
 */
public class FileUtils {
    private FileUtils() {}


    /**
     * 递归查找指定目录下的文件
     *
     * @param path 路径
     * @param fileName 文件名
     * @return 文件全路径
     */
    public static File findFile(String path, String fileName, List<File> fileList) {
        File searchFile = null;
        File dir = new File(path);
        if (dir.exists() && dir.isDirectory()) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                if (file.isDirectory()) {
                    searchFile = findFile(file.getPath(), fileName, fileList);
                } else {
                    if (file.getName().equals(fileName)) {
                        searchFile = file;
                        break;
                    }
                }
            }
        }
        if (searchFile != null) {
            fileList.add(searchFile);
        }
        return searchFile;
    }


    /**
     * 判断resource路径下的文件是否存在
     *
     * @param fileName 配置文件名称
     * @return null: 不存在，否则为存在
     */
    public static InputStream resourceFileExists(String fileName) {
        return FileUtils.class.getClassLoader().getResourceAsStream(fileName);
    }
}
