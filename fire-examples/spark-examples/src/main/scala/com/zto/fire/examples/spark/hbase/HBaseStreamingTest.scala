package com.zto.fire.examples.spark.hbase

import com.zto.fire._
import com.zto.fire.core.anno.connector.{HBase, HBase2, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
  * 通过hbase相关api，将数据实时写入到hbase中
  * @author ChengLong 2019-5-26 13:21:59
  * @contact Fire框架技术交流群（钉钉）：35373471
  */
@HBase("test")
@Streaming(interval = 30, concurrent = 2)
@HBase2(cluster = "test", scanPartitions = 30, storageLevel = "DISK_ONLY")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HBaseStreamingTest extends SparkStreaming {
  private val tableName8 = "fire_test_8"
  private val tableName9 = "fire_test_9"

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    HBaseConnector.truncateTable(this.tableName8)
    HBaseConnector.truncateTable(this.tableName9, keyNum = 2)

    dstream.repartition(3).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        HBaseConnector.insert(this.tableName8, Student.newStudentList())
        val student = HBaseConnector.get(this.tableName9, classOf[Student], Seq("1", "2"))
        student.foreach(t => logger.error("HBase1 Get结果：" + t))

        HBaseConnector.insert(this.tableName9, Student.newStudentList())
        val student2 = HBaseConnector.get(this.tableName8, classOf[Student], Seq("2", "3"), keyNum = 2)
        student2.foreach(t => logger.error("HBase2 Get结果：" + t))
      })
    })
  }
}
