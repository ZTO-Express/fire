package com.zto.fire.spark.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 基于注解的方式配置Spark Streaming任务
 *
 * @author ChengLong
 * @date 2022-04-30 21:44:19
 * @since 2.2.1
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Streaming {

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

    /**
     * 是否自动提交job：call startAwaitTermination()
     */
    boolean autoStart() default true;

    /**
     * 并行执行的streaming批次数
     */
    int concurrent() default -1;

    /**
     * 指定消费kafka或rocketmq每秒从每个分区获取的最大记录数
     */
    long maxRatePerPartition() default -1;

    /**
     * 是否启用反压机制
     */
    boolean backpressure() default true;

    /**
     * 启用反压机制时每个接收器接收第一批数据的初始最大速率
     */
    long backpressureInitialRate() default -1;

    /**
     * 是否优雅的停止streaming
     */
    boolean stopGracefullyOnShutdown() default true;

}
