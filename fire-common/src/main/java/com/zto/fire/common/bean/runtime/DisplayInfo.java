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

import oshi.SystemInfo;
import oshi.hardware.Display;

/**
 * 用于封装显示器相关信息
 * @author ChengLong 2019年9月30日 13:36:16
 */
public class DisplayInfo {

    /**
     * 显示器描述信息
     */
    private String display;

    public String getDisplay() {
        return display;
    }

    private DisplayInfo() {
    }

    /**
     * 获取显示器信息
     */
    public static DisplayInfo getDisplayInfo() {
        SystemInfo systemInfo = new SystemInfo();
        Display[] displays = systemInfo.getHardware().getDisplays();

        StringBuilder sb = new StringBuilder();
        if (displays != null && displays.length > 0) {
            for (Display display : displays) {
                sb.append(display);
            }
        }
        DisplayInfo displayInfo = new DisplayInfo();
        displayInfo.display = sb.toString();

        return displayInfo;
    }
}
