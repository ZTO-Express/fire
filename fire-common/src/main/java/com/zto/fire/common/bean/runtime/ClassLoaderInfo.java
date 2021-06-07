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

package com.zto.fire.common.bean.runtime;

import java.io.Serializable;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

/**
 * 获取运行时class loader信息
 * @author ChengLong 2019年9月28日 19:56:18
 */
public class ClassLoaderInfo implements Serializable {
    private static final long serialVersionUID = 4958598582046079565L;
    // 获取已加载的类数量
    private long loadedClassCount;
    // 获取总的类加载数
    private long totalLoadedClassCount;
    // 获取未被加载的类总数
    private long unloadedClassCount;

    private ClassLoaderInfo() {}

    public long getLoadedClassCount() {
        return loadedClassCount;
    }

    public long getTotalLoadedClassCount() {
        return totalLoadedClassCount;
    }

    public long getUnloadedClassCount() {
        return unloadedClassCount;
    }

    /**
     * 获取类加载器相关信息
     */
    public static ClassLoaderInfo getClassLoaderInfo() {
        ClassLoaderInfo classLoaderInfo = new ClassLoaderInfo();
        // 获取类加载器相关信息
        ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        classLoaderInfo.loadedClassCount = classLoadingMXBean.getLoadedClassCount();
        classLoaderInfo.totalLoadedClassCount = classLoadingMXBean.getTotalLoadedClassCount();
        classLoaderInfo.unloadedClassCount = classLoadingMXBean.getUnloadedClassCount();

        return classLoaderInfo;
    }

}