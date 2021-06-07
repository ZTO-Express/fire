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

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * HTTP接口调用，各模块继承自该类
 * Created by ChengLong on 2017-12-12.
 */
public class HttpClientUtils {
    private static final String CHARSET = "UTF-8";
    private static final String HEADER_JSON_VALUE = "application/json";
    private static final Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);

    private HttpClientUtils() {
    }

    /**
     * 添加header请求信息
     *
     * @param method  请求的方式
     * @param headers 请求头信息
     */
    private static void setHeaders(HttpMethodBase method, Header... headers) {
        if (method != null && headers != null && headers.length > 0) {
            for (Header header : headers) {
                if (header != null) method.setRequestHeader(header);
            }
        }
    }

    /**
     * 以流的方式获取返回的消息体
     */
    private static String responseBody(HttpMethodBase method) throws IOException {
        if (method == null) return "";

        StringBuilder stringBuffer = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(method.getResponseBodyAsStream()));
        String str = "";
        while ((str = reader.readLine()) != null) {
            stringBuffer.append(str);
        }
        return stringBuffer.toString();
    }

    /**
     * HTTP通用接口调用（Get请求）
     *
     * @param url 地址
     * @return 调用结果
     */
    public static String doGet(String url, Header... headers) throws IOException {
        String responseBody = "";
        GetMethod getMethod = new GetMethod();
        HttpClient httpClient = new HttpClient();
        // 设置 get 请求超时为 5 秒
        getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
        // 设置请求重试处理，用的是默认的重试处理：请求三次
        getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
        // 设置请求头
        setHeaders(getMethod, headers);

        getMethod.setURI(new URI(url, true, CHARSET));
        int statusCode = httpClient.executeMethod(getMethod);
        // 判断访问的状态码
        if (statusCode != HttpStatus.SC_OK) {
            logger.error("请求出错: {}", getMethod.getStatusLine());
        }
        // 读取 HTTP 响应内容，这里简单打印网页内容
        responseBody = responseBody(getMethod);
        getMethod.releaseConnection();
        httpClient.getHttpConnectionManager().closeIdleConnections(0);
        return responseBody;
    }

    /**
     * HTTP通用接口调用（Post请求）
     *
     * @param url 地址
     * @return 调用结果
     */
    public static String doPost(String url, String json, Header... headers) throws IOException {
        String responses = "";
        PostMethod postMethod = new PostMethod();
        HttpClient httpClient = new HttpClient();
        postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
        postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
        // 设置请求头
        setHeaders(postMethod, headers);
        postMethod.setURI(new URI(url, true, CHARSET));
        postMethod.addRequestHeader("Content-Type", HEADER_JSON_VALUE);
        if (json != null && StringUtils.isNotBlank(json.trim())) {
            RequestEntity requestEntity = new StringRequestEntity(json, HEADER_JSON_VALUE, CHARSET);
            postMethod.setRequestHeader("Content-Length", String.valueOf(requestEntity.getContentLength()));
            postMethod.setRequestEntity(requestEntity);
        }
        httpClient.executeMethod(postMethod);
        responses = responseBody(postMethod);
        postMethod.releaseConnection();
        httpClient.getHttpConnectionManager().closeIdleConnections(0);
        return responses;
    }

    /**
     * 发送一次post请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doPut(String url, String json, Header... headers) throws IOException {
        String responseBody = "";
        PutMethod putMethod = new PutMethod();
        HttpClient htpClient = new HttpClient();
        putMethod.setURI(new URI(url, true, CHARSET));
        putMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
        putMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
        // 设置请求头
        setHeaders(putMethod, headers);
        if (json != null && StringUtils.isNotBlank(json.trim())) {
            RequestEntity requestEntity = new StringRequestEntity(json, HEADER_JSON_VALUE, CHARSET);
            putMethod.setRequestHeader("Content-Length", String.valueOf(requestEntity.getContentLength()));
            putMethod.setRequestEntity(requestEntity);
        }
        int statusCode = htpClient.executeMethod(putMethod);
        if (statusCode != HttpStatus.SC_OK) {
            return "";
        }
        responseBody = responseBody(putMethod);
        putMethod.releaseConnection();
        htpClient.getHttpConnectionManager().closeIdleConnections(0);
        return responseBody;
    }

    /**
     * 发送一次get请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doGetIgnore(String url, Header... headers) {
        String response = "";
        try {
            response = doGet(url, headers);
        } catch (Exception e) {
            logger.error("HTTP通用接口调用（Get）失败", e);
        }
        return response;
    }

    /**
     * 发送一次post请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doPostIgnore(String url, String json, Header... headers) {
        String response = "";
        try {
            response = doPost(url, json, headers);
        } catch (Exception e) {
            logger.error("HTTP通用接口调用（Post）失败", e);
        }
        return response;
    }

    /**
     * 发送一次put请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doPutIgnore(String url, String json, Header... headers) {
        String response = "";
        try {
            response = doPut(url, json, headers);
        } catch (Exception e) {
            logger.error("HTTP通用接口调用（Put）失败", e);
        }

        return response;
    }

}
