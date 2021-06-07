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

import com.zto.fire.common.conf.FireFrameworkConf;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * 各种常用算法加密工具类
 *
 * @author ChengLong 2018年7月16日 09:53:59
 */
public class EncryptUtils {
    private static final String ERROR_MESSAGE = "参数不合法";
    private static final Logger logger = LoggerFactory.getLogger(EncryptUtils.class);

    private EncryptUtils() {}

    /**
     * BASE64解密
     */
    public static String base64Decrypt(String message) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        try {
            return new String((new BASE64Decoder()).decodeBuffer(message), StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("BASE64解密出错", e);
        }
        return "";
    }

    /**
     * BASE64加密
     */
    public static String base64Encrypt(String message) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        try {
            return new BASE64Encoder().encodeBuffer(message.getBytes());
        } catch (Exception e) {
            logger.error("BASE64加密出错", e);
        }
        return "";
    }

    /**
     * 生成32位md5码
     */
    public static String md5Encrypt(String message) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        try {
            // 得到一个信息摘要器
            MessageDigest digest = MessageDigest.getInstance("md5");
            byte[] result = digest.digest(message.getBytes(StandardCharsets.UTF_8));
            StringBuilder buffer = new StringBuilder();
            for (byte b : result) {
                int number = b & 0xff;// 加盐
                String str = Integer.toHexString(number);
                if (str.length() == 1) {
                    buffer.append('0');
                }
                buffer.append(str);
            }
            // 标准的md5加密后的结果
            return buffer.toString();
        } catch (NoSuchAlgorithmException e) {
            logger.error("生成32位md5码出错", e);
        }
        return "";
    }

    /**
     * SHA加密
     */
    public static String shaEncrypt(String message, String key) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        if(StringUtils.isBlank(key)) {
            key = "SHA";
        }
        try {
            MessageDigest sha = MessageDigest.getInstance(key);
            sha.update(message.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(sha.digest()).toString(32);
        } catch (Exception e) {
            logger.error("生成SHA加密出错", e);
        }
        return "";
    }

    /**
     * header权限校验
     * @param auth
     * 请求json
     * @return
     * true：身份合法  false：身份非法
     */
    public static boolean checkAuth(String auth, String privateKey) {
        if (StringUtils.isBlank(auth)) {
            return false;
        }
        String fireAuth = EncryptUtils.shaEncrypt(FireFrameworkConf.restServerSecret() + privateKey, "SHA");
        return fireAuth.equals(auth);
    }

}
