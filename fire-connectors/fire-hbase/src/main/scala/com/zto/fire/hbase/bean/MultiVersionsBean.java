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

package com.zto.fire.hbase.bean;

import com.zto.fire.common.anno.FieldName;
import com.zto.fire.common.util.JSONUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.BigDecimalConverter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 多版本HBase实体Bean
 * Created by ChengLong on 2017-08-17.
 */
public class MultiVersionsBean extends HBaseBaseBean<MultiVersionsBean> {
    @FieldName(value = "logger", disuse = true)
    private static final transient Logger logger = LoggerFactory.getLogger(MultiVersionsBean.class);
    @FieldName("multiFields")
    private String multiFields;

    @FieldName(value = "HBaseBaseBean", disuse = true)
    private HBaseBaseBean<?> target;

    @FieldName(value = "BIGDECIMAL_ZERO", disuse = true)
    private static final BigDecimal BIGDECIMAL_ZERO = new BigDecimal("0");

    static {
        // 这里一定要注册默认值，使用null也可以
        BigDecimalConverter bd = new BigDecimalConverter(BIGDECIMAL_ZERO);
        ConvertUtils.register(bd, java.math.BigDecimal.class);
    }

    public String getMultiFields() {
        return multiFields;
    }

    public void setMultiFields(String multiFields) {
        this.multiFields = multiFields;
    }

    public HBaseBaseBean getTarget() {
        return target;
    }

    public void setTarget(HBaseBaseBean<?> target) {
        this.target = target;
    }

    public MultiVersionsBean(HBaseBaseBean<?> target) {
        this.target = (HBaseBaseBean) target.buildRowKey();
        this.multiFields = JSONUtils.toJSONString(this.target);
    }

    public MultiVersionsBean() {

    }

    @Override
    public MultiVersionsBean buildRowKey() {
        try {
            if (this.target == null && StringUtils.isNotBlank(this.multiFields)) {
                Map<Object, Object> map = JSONUtils.parseObject(this.multiFields, Map.class);
                Class<?> clazz = Class.forName(map.get("className").toString());
                HBaseBaseBean<?> bean = (HBaseBaseBean) clazz.newInstance();
                BeanUtils.populate(bean, map);
                this.target = (HBaseBaseBean) bean.buildRowKey();
            }

            if (this.target != null) {
                this.target = (HBaseBaseBean) this.target.buildRowKey();
                this.rowKey = this.target.rowKey;
            }
        } catch (Exception e) {
            logger.error("执行buildRowKey()方法失败", e);
        }

        return this;
    }
}
