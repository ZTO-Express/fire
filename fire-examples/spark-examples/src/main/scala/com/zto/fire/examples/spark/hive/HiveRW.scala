package com.zto.fire.examples.spark.hive

import com.zto.fire._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkStreaming
import org.apache.spark.sql.DataFrame


/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HiveRW extends SparkStreaming {

  // 消息格式
  // {"age":16,"className":"Student","createTime":"2020-08-03 17:23:05","id":6,"length":15.0,"name":"root","sex":true}
  // {"age":16,"className":"Student","createTime":"2020-08-03 17:23:05","id":6,"length":15.0,"name":"root","sex":true}
  override def process: Unit = {
    // this.ddl
    this.streaming
    // this.batch
  }

  /**
   * spark core模式
   */
  def batch: Unit = {
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    insert(df)
  }

  /**
   * streaming模式
   */
  def streaming: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.map(t => JSONUtils.parseObject[Student](t.value())).foreachRDD(rdd => {
      val df = this.fire.createDataFrame(rdd, classOf[Student])
      insert(df)
    })
  }

  /**
   * 创建表
   */
  def ddl: Unit = {
    sql(
      """
        |drop table if exists tmp.baseorganize_fire
        |""".stripMargin)

    sql(
      """
        |create table tmp.baseorganize_fire (
        |    id bigint,
        |    name string,
        |    age int
        |) partitioned by (ds string)
        |row format delimited fields terminated by '/t'
        |""".stripMargin)
  }

  /**
   * 动态分区写入
   */
  def insert(df: DataFrame): Unit = {
    sql("set hive.exec.dynamic.partition = true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    df.createOrReplaceTempView("t_student")

    sql(
      """
        |insert into table tmp.baseorganize_fire
        |select
        | id,
        | name,
        | age,
        | '20220221' as ds
        |from t_student
        |""".stripMargin)

    sql(
      """
        |select
        | *,
        | count(1) over()
        |from tmp.baseorganize_fire
        |""".stripMargin).show(3, false)
  }


  override def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
