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

package com.zto.fire.common.bean.rest;

import com.zto.fire.common.enu.ErrorCode;
import com.zto.fire.common.util.JSONUtils;

/**
 * 返回消息
 *
 * @author ChengLong 2018年6月12日 13:42:23
 */
public class ResultMsg {
    // 消息体
    private Object content;
    // 系统错误码
    private ErrorCode code;
    // 错误描述
    private String msg;

    /**
     * 验证是否成功
     *
     * @param resultMsg
     * @return true: 成功 false 失败
     */
    public static boolean isSuccess(ResultMsg resultMsg) {
        return resultMsg != null && resultMsg.getCode() == ErrorCode.SUCCESS;
    }

    /**
     * 获取描述信息
     *
     * @param resultMsg
     * @return 描述信息
     */
    public static String getMsg(ResultMsg resultMsg) {
        if (resultMsg != null) {
            return resultMsg.getMsg();
        } else {
            return "";
        }
    }

    /**
     * 获取状态码
     *
     * @return 状态码
     */
    public static ErrorCode getCode(ResultMsg resultMsg) {
        if (resultMsg != null) {
            return resultMsg.getCode();
        }
        return ErrorCode.ERROR;
    }

    public ResultMsg() {
    }

    public ResultMsg(String content, ErrorCode code, String msg) {
        this.content = content;
        this.code = code;
        this.msg = msg;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public ErrorCode getCode() {
        return code;
    }

    public void setCode(ErrorCode code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * 构建成功消息
     */
    public String buildSuccess(Object content, String msg) {
        this.content = content;
        this.code = ErrorCode.SUCCESS;
        this.msg = msg;
        return this.toString();
    }

    /**
     * 构建失败消息
     */
    public String buildError(String msg, ErrorCode errorCode) {
        this.content = "";
        this.code = errorCode;
        this.msg = msg;
        return this.toString();
    }

    @Override
    public String toString() {
        return JSONUtils.toJSONString(this);
    }
}
