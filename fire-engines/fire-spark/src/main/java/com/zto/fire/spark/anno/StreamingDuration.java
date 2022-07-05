package com.zto.fire.spark.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Spark Streaming任务的批次时间
 *
 * @author ChengLong 2021年8月3日19:39:28
 * @since 2.1.1
 */
@Deprecated
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface StreamingDuration {

    /**
     * 批次时间（s）
     */
    int value() default 10;

    /**
     * 批次时间（s），同value字段
     */
    int interval() default -1;

    /**
     * 是否开启spark streaming的checkpoint
     */
    boolean checkpoint() default false;

}
