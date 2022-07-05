package com.zto.fire.spark.bean;

import java.util.List;

/**
 * 自动生成数据的JavaBean父类
 *
 * @param <T> 子类具体的类型
 * @author ChengLong 2022-03-07 14:49:51
 * @since 2.2.1
 */
public interface GenerateBean<T extends GenerateBean<T>> {

    /**
     * 自动生成对象实例的具体逻辑
     * @return 对象实例
     */
    List<T> generate();
}
