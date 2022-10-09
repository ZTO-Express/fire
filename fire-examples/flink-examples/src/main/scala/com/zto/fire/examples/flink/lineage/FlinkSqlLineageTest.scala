package com.zto.fire.examples.flink.lineage

import com.zto.fire._
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.core.anno.lifecycle.{Step1, Step2, Step3, Step4}
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * 用于解析flink sql血缘依赖
 *
 * @author ChengLong 2022-09-13 14:20:13
 * @since 2.0.0
 */
@Hive("test")
@Streaming(interval = 60, parallelism = 2)
object FlinkSqlLineageTest extends FlinkStreaming {

  @Step1("血缘信息输出")
  def lineage: Unit = {
    // 定义hive表前先切换到hive catalog
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue) + "\n\n")
    }, 0, 10, TimeUnit.SECONDS)
  }

  @Step2("定义RocketMQ源表")
  def source: Unit = {
    sql("""
          |CREATE table source (
          |  id int,
          |  name string,
          |  age int,
          |  length double,
          |  data DECIMAL(10, 5)
          |) with (
          | 'connector'='fire-rocketmq',
          | 'format'='json',
          | 'rocket.brokers.name'='bigdata_test',
          | 'rocket.topics'='fire',
          | 'rocket.group.id'='fire',
          | 'rocket.consumer.tag'='*'
          |)
          |""".stripMargin)
  }

  @Step3("定义目标表")
  def sink: Unit = {
    sql(
      """
        |CREATE table sink (
        |  id int,
        |  name string,
        |  age int,
        |  length double,
        |  data DECIMAL(10, 5)
        |) with (
        | 'connector'='fire-rocketmq',
        | 'format'='json',
        | 'rocket.brokers.name'='bigdata_test',
        | 'rocket.topics'='fire2',
        | 'rocket.consumer.tag'='*',
        | 'rocket.sink.parallelism'='1'
        |)
        |""".stripMargin)
  }

  @Step4("数据sink")
  def insert: Unit = {
    sql("""
          |insert into sink select * from source
          |""".stripMargin)
  }
}
