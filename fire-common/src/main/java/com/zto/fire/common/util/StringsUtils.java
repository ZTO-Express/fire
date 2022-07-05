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

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字符串工具类
 *
 * @author ChengLong 2019-4-11 09:06:26
 */
public class StringsUtils {
    private StringsUtils() {
    }

    /**
     * 处理成超链接
     *
     * @param str
     * @return
     */
    public static String hrefTag(String str) {
        return append("<a href='", str, "'>", str, "</a>");
    }

    /**
     * 追加换行
     *
     * @param str
     * @return
     */
    public static String brTag(String str) {
        return append(str, "<br/>");
    }

    /**
     * 字符串拼接
     *
     * @param strs 多个字符串
     * @return 拼接结果
     */
    public static String append(String... strs) {
        StringBuilder sb = new StringBuilder();
        if (null != strs && strs.length > 0) {
            for (String str : strs) {
                sb.append(str);
            }
        }

        return sb.toString();
    }

    /**
     * replace多组字符串中的数据
     *
     * @param map
     * @return
     * @apiNote replace(str, ImmutableMap.of ( " # ", " ", ", ", " "))
     */
    public static String replace(String str, Map<String, String> map) {
        if (StringUtils.isNotBlank(str) && null != map && map.size() > 0) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                str = str.replace(entry.getKey(), entry.getValue());
            }
        }
        return str;
    }

    /**
     * 16进制的字符串表示转成字节数组
     *
     * @param hexString 16进制格式的字符串
     * @return 转换后的字节数组
     **/
    public static byte[] toByteArray(String hexString) {
        if (StringUtils.isEmpty(hexString))
            throw new IllegalArgumentException("this hexString must not be empty");

        hexString = hexString.toLowerCase();
        final byte[] byteArray = new byte[hexString.length() / 2];
        int k = 0;
        for (int i = 0; i < byteArray.length; i++) {//因为是16进制，最多只会占用4位，转换成字节需要两个16进制的字符，高位在先
            byte high = (byte) (Character.digit(hexString.charAt(k), 16) & 0xff);
            byte low = (byte) (Character.digit(hexString.charAt(k + 1), 16) & 0xff);
            byteArray[i] = (byte) (high << 4 | low);
            k += 2;
        }
        return byteArray;
    }

    /**
     * 字节数组转成16进制表示格式的字符串
     *
     * @param byteArray 需要转换的字节数组
     * @return 16进制表示格式的字符串
     **/
    public static String toHexString(byte[] byteArray) {
        if (byteArray == null || byteArray.length < 1)
            throw new IllegalArgumentException("this byteArray must not be null or empty");

        final StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < byteArray.length; i++) {
            if ((byteArray[i] & 0xff) < 0x10)//0~F前面不零
                hexString.append('0');
            hexString.append(Integer.toHexString(0xFF & byteArray[i]));
        }
        return hexString.toString().toLowerCase();
    }

    /**
     * 具有容错功能的substring，如果下标越界，则默认取到尾部
     *
     * @param str   原字符串
     * @param start 索引起始
     * @param end   索引结束
     * @return 截取后的子字符串
     */
    public static String substring(String str, int start, int end) {
        if (StringUtils.isBlank(str) || Math.abs(start) > Math.abs(end)) {
            return "";
        }
        int length = str.length();
        if (length >= Math.abs(end)) {
            return str.substring(Math.abs(start), Math.abs(end));
        } else {
            return str.substring(Math.abs(start), Math.abs(length));
        }
    }

    /**
     * 判断一个字符串是否为整型
     * 1. 包号空字符串的不能看作是整数
     * 2. 超过Int最大值的不能作为整数
     */
    public static boolean isInt(String str) {
        if (StringUtils.isBlank(str)) return false;
        try {
            Integer.parseInt(str);
            return true;
        } catch (Exception e) {
            // 如果超过精度，则不能看做是整型
            return false;
        }
    }

    /**
     * 判断字符串是否为整数（前面是数值类型，最后是L或l结尾，也认为是长整数）
     */
    public static boolean isLong(String str) {
        if (StringUtils.isBlank(str)) return false;
        str = str.toUpperCase();
        if (str.endsWith("L")) {
            try {
                Long.parseLong(str.replace("L", ""));
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        return false;
    }

    /**
     * 用于判断字符串是否为布尔类型
     */
    public static boolean isBoolean(String str) {
        if (StringUtils.isBlank(str)) return false;
        return "true".equalsIgnoreCase(str) || "false".equalsIgnoreCase(str);
    }

    /**
     * 用于判断字符串是否为float类型
     * 以字母F或f结尾的合法数值型字符串认为是float类型
     */
    public static boolean isFloat(String str) {
        if (StringUtils.isBlank(str)) return false;
        str = str.toUpperCase();
        if (str.endsWith("F")) {
            try {
                Float.parseFloat(str.replace("F", ""));
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    /**
     * 用于判断字符串是否为float类型
     * 以字母F或f结尾的合法数值型字符串认为是float类型
     */
    public static boolean isDouble(String str) {
        if (StringUtils.isBlank(str)) return false;
        str = str.toUpperCase();
        if (str.endsWith("D")) {
            try {
                Double.parseDouble(str.replace("D", ""));
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    /**
     * 根据字符串具体的类型进行转换，返回转换类型之后的数据
     */
    public static Object parseString(String str) {
        if (StringsUtils.isLong(str)) {
            String longStr = str.toUpperCase().replace("L", "");
            return Long.valueOf(longStr);
        } else if (StringsUtils.isInt(str)) {
            return Integer.valueOf(str);
        } else if (StringsUtils.isBoolean(str)) {
            return Boolean.valueOf(str);
        } else if (StringsUtils.isFloat(str)) {
            String floatStr = str.toUpperCase().replace("F", "");
            return Float.valueOf(floatStr);
        } else if (StringsUtils.isDouble(str)) {
            String doubleStr = str.toUpperCase().replace("D", "");
            return Double.valueOf(doubleStr);
        } else {
            return str;
        }
    }

    /**
     * 用于判断给定的字符串是否为数值类型，负数、小数均认为是数值类型
     * @param str
     * 字符串
     * @return
     * true：数值类型 false：非数值类型
     */
    public static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("(^\\-?[1-9]\\d*\\.?\\d*$)|(^\\-?0\\.\\d*[1-9]$)");
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }

    /**
     * 基于时间戳的随机算法从字符串列表中获取随机的字符串
     *
     * @param strs
     * 被随机分隔的一组字符串
     * @param delimiter
     * 分隔符
     * @return
     * 随机的字符串
     */
    public static String randomSplit(String strs, String delimiter) {
        if (StringUtils.isBlank(strs)) throw new IllegalArgumentException("Hive Thrift Server url不能为空!");
        if (StringUtils.isBlank(delimiter)) delimiter = ",";
        String[] metastores = strs.split(delimiter);
        if (metastores.length == 0) throw new IllegalArgumentException("未能根据指定的分隔符[" + delimiter + "]分隔字符串：" + strs);
        return StringUtils.trim(metastores[(int) (System.currentTimeMillis() % metastores.length)]);
    }
}
