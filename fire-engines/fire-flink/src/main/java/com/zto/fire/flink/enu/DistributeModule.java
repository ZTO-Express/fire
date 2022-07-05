package com.zto.fire.flink.enu;

import org.apache.commons.lang3.StringUtils;

/**
 * 模块类型，用于标识不同的模块
 *
 * @author ChengLong 2021-11-11 09:34:48
 * @since 2.2.0
 */
public enum DistributeModule {
    CONF("conf"), ARTHAS("arthas");

    DistributeModule(String type) {
    }

    /**
     * 将字符串解析成指定的枚举类型
     */
    public static DistributeModule parse(String type) {
        if (StringUtils.isBlank(type)) return CONF;
        try {
            return Enum.valueOf(DistributeModule.class, type.trim().toUpperCase());
        } catch (Exception e) {
            return CONF;
        }
    }
}
