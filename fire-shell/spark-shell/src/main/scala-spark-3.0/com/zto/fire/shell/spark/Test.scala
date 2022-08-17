package com.zto.fire.shell.spark

import com.zto.fire.common.anno.Config
import com.zto.fire.spark.SparkStreaming
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

@Config(
      """
        |hive.cluster=test
        |kafka.brokers.name = bigdata_test
        |kafka.topics = fire
        |kafka.group.id=fire
        |spark.streaming.stopGracefullyOnShutdown=false
        |""")
object Test extends SparkStreaming {

      def getFire: SparkSession = this.fire

      def getSparkSession: SparkSession = this.fire

      def getSc: SparkContext = this.sc
}