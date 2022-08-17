package com.zto.fire.examples.spark.anno

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

import com.zto.fire.common.anno.Config
import com.zto.fire.common.conf.{FireFrameworkConf, FireHiveConf, FireKafkaConf, FireRocketMQConf}
import com.zto.fire.common.util.PropUtils
import com.zto.fire.core.anno.connector._
import com.zto.fire.hbase.conf.FireHBaseConf
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.{Streaming, StreamingDuration}
import com.zto.fire.spark.conf.FireSparkConf
import org.junit.Test

@Config(
  """
    |hive.cluster=test
    |spark.max.parallelism=11
    |""")
@Hive(value = "batch", catalog = "hive_catalog", version = "1.1.1", partition = "dt")
@HBase(value = "batch-new1", batchSize = 10, durability = "off", scanPartitions = 12, config = Array("hbase.zookeeper.property.clientPort=2181", "zookeeper.znode.parent = /hbase"))
@HBase2(value = "batch-new2", tableMetaCache = false, batchSize = 10, storageLevel = "memory_only", config = Array("hbase.zookeeper.property.clientPort=2182", "zookeeper.znode.parent = /hbase2"))
@HBase3(value = "batch-new3", scanPartitions = 11, family = "data", maxRetries = 5, config = Array("hbase.zookeeper.property.clientPort=2183", "zookeeper.znode.parent = /hbase3"))
@Kafka(brokers = "localhost:2181", topics = "fire", groupId = "fire", startingOffset = "start", endingOffsets = "end", autoCommit = true, sessionTimeout = 10, requestTimeout = 11, pollInterval = 12, forceOverwriteStateOffset = true, forceAutoCommit = true, forceAutoCommitInterval = 10)
@Kafka2(brokers = "127.0.0.1:2181", topics = "fire2", groupId = "fire2", startingOffset = "start2", endingOffsets = "end2", sessionTimeout = 100, requestTimeout = 110, pollInterval = 120)
@Kafka3(brokers = "127.0.0.1:2181", topics = "fire3", groupId = "fire3", startFromTimestamp = 100, startFromGroupOffsets = true, config = Array[String]("hello=world", "scala=flink"))
@RocketMQ(brokers = "rocketmq", topics = "fire", groupId = "fire", startingOffset = "new", tag = "a", autoCommit = true, config = Array[String]("hello=world", "scala=flink"))
@RocketMQ2(brokers = "rocketmq2", topics = "fire2", groupId = "fire2", startingOffset = "new2", tag = "b", autoCommit = true, config = Array[String]("hello=world2", "scala=flink2"))
@Jdbc(url = "jdbc:mysql://localhost:3306", username = "root1", password = "root1", maxPoolSize = 10, maxIdleTime = 10, batchSize = 51, flushInterval = 1000, logSqlLength = 20, storageLevel = "memory", queryPartitions = 12)
@Jdbc2(url = "jdbc:mysql://192.168.0.1:3306", driver = "com.fire", username = "root2", minPoolSize = 9, initialPoolSize = 8, password = "root2", maxRetries = 6, config = Array[String]("hello=world", "scala=flink"))
@Jdbc3(url = "jdbc:mysql://192.168.0.2:3306", username = "root3", isolationLevel = "read",  password = "root3", acquireIncrement = 2)
@StreamingDuration(value = 20, checkpoint = false)
@Streaming(value = 10, interval = 11, checkpoint = true, concurrent = 3, maxRatePerPartition = 10, backpressure = false, backpressureInitialRate = 26, stopGracefullyOnShutdown = false)
class AnnoConfTest extends SparkStreaming {

  /**
   * 测试@Streaming注解
   */
  @Test
  def testStreaming: Unit = {
    assert(FireSparkConf.confBathDuration == 11)
    assert(this.conf.getBoolean("spark.streaming.receiver.writeAheadLog.enable", false))
    assert(this.conf.getInt("spark.streaming.concurrentJobs", 1) == 3)
    assert(this.conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0) == 10)
    assert(!this.conf.getBoolean("spark.streaming.backpressure.enabled", false))
    assert(!this.conf.getBoolean("spark.streaming.stopGracefullyOnShutdown", true))
  }

  /**
   * 测试@Jdbc注解
   */
  @Test
  def testJdbc: Unit = {
    assert(FireJdbcConf.url().equals("jdbc:mysql://localhost:3306"))
    assert(FireJdbcConf.url(2).equals("jdbc:mysql://192.168.0.1:3306"))
    assert(FireJdbcConf.url(3).equals("jdbc:mysql://192.168.0.2:3306"))
    assert(FireJdbcConf.driverClass().equals("com.mysql.jdbc.Driver"))
    assert(FireJdbcConf.driverClass(2).equals("com.fire"))
    // TODO: 自动推断driver
    // assert(FireJdbcConf.driverClass(3).equals("com.mysql.jdbc.Driver"))
    assert(FireJdbcConf.user().equals("root1"))
    assert(FireJdbcConf.user(2).equals("root2"))
    assert(FireJdbcConf.user(3).equals("root3"))
    assert(FireJdbcConf.password().equals("root1"))
    assert(FireJdbcConf.password(2).equals("root2"))
    assert(FireJdbcConf.password(3).equals("root3"))
    assert(FireJdbcConf.maxPoolSize() == 10)
    assert(FireJdbcConf.initialPoolSize(2) == 8)
    assert(FireJdbcConf.isolationLevel(3).equals("read"))
    assert(FireJdbcConf.maxIdleTime() == 10)
    assert(FireJdbcConf.maxRetry(2) == 6)
    assert(FireJdbcConf.acquireIncrement(3) == 2)
    assert(FireJdbcConf.batchSize() == 51)
    assert(FireFrameworkConf.logSqlLength == 20)
    assert(FireJdbcConf.jdbcStorageLevel.equals("MEMORY"))
    assert(FireJdbcConf.jdbcFlushInterval() == 1000)
    assert(FireJdbcConf.jdbcQueryPartition == 12)
    // "hello=world", "scala=flink"
    PropUtils.sliceKeysByNum(FireJdbcConf.JDBC_C3P0_CONF_PREFIX, 2).foreach(kv => {
      if (kv._1.equals("hello")) assert(kv._2.equals("world"))
      if (kv._1.equals("scala")) assert(kv._2.equals("flink"))
    })
  }

  /**
   * 测试@RocketMQ注解
   */
  @Test
  def testRocketMQ: Unit = {
    assert(FireRocketMQConf.rocketNameServer().equals("rocketmq"))
    assert(FireRocketMQConf.rocketTopics().equals("fire"))
    assert(FireRocketMQConf.rocketGroupId().equals("fire"))
    assert(FireRocketMQConf.rocketStartingOffset().equals("new"))
    assert(FireRocketMQConf.rocketConsumerTag().equals("a"))
    assert(FireRocketMQConf.rocketEnableAutoCommit())
    // "hello=world", "scala=flink"
    PropUtils.sliceKeysByNum(FireRocketMQConf.rocketConfStart, 1).foreach(kv => {
      if (kv._1.equals("hello")) assert(kv._2.equals("world"))
      if (kv._1.equals("scala")) assert(kv._2.equals("flink"))
    })

    assert(FireRocketMQConf.rocketNameServer(2).equals("rocketmq2"))
    assert(FireRocketMQConf.rocketTopics(2).equals("fire2"))
    assert(FireRocketMQConf.rocketGroupId(2).equals("fire2"))
    assert(FireRocketMQConf.rocketStartingOffset(2).equals("new2"))
    assert(FireRocketMQConf.rocketConsumerTag(2).equals("b"))
    assert(FireRocketMQConf.rocketEnableAutoCommit(2))
    // "hello=world", "scala=flink"
    PropUtils.sliceKeysByNum(FireRocketMQConf.rocketConfStart, 2).foreach(kv => {
      if (kv._1.equals("hello")) assert(kv._2.equals("world2"))
      if (kv._1.equals("scala")) assert(kv._2.equals("flink2"))
    })
  }

  /**
   * 测试@Kafka注解
   */
  @Test
  def testKafka: Unit = {
    assert(FireKafkaConf.kafkaBrokers().equals("localhost:2181"))
    assert(FireKafkaConf.kafkaTopics().equals("fire"))
    assert(FireKafkaConf.kafkaGroupId().equals("fire"))
    assert(FireKafkaConf.kafkaStartingOffset().equals("start"))
    assert(FireKafkaConf.kafkaEndingOffsets().equals("end"))
    assert(FireKafkaConf.kafkaEnableAutoCommit())
    assert(FireKafkaConf.kafkaSessionTimeOut() == 10)
    assert(FireKafkaConf.kafkaRequestTimeOut() == 11)
    assert(FireKafkaConf.kafkaPollInterval() == 12)
    assert(FireKafkaConf.kafkaForceOverwriteStateOffset)
    assert(FireKafkaConf.kafkaForceCommit)
    assert(FireKafkaConf.kafkaForceCommitInterval == 10)

    assert(FireKafkaConf.kafkaBrokers(2).equals("127.0.0.1:2181"))
    assert(FireKafkaConf.kafkaTopics(2).equals("fire2"))
    assert(FireKafkaConf.kafkaGroupId(2).equals("fire2"))
    assert(FireKafkaConf.kafkaStartingOffset(2).equals("start2"))
    assert(FireKafkaConf.kafkaEndingOffsets(2).equals("end2"))
    assert(FireKafkaConf.kafkaSessionTimeOut(2) == 100)
    assert(FireKafkaConf.kafkaRequestTimeOut(2) == 110)
    assert(FireKafkaConf.kafkaPollInterval(2) == 120)

    assert(FireKafkaConf.kafkaStartFromTimeStamp(3) == 100)
    assert(FireKafkaConf.kafkaStartFromGroupOffsets(3))

    // "hello=world", "scala=flink"
    PropUtils.sliceKeysByNum(FireKafkaConf.kafkaConfStart, 3).foreach(kv => {
      if (kv._1.equals("hello")) assert(kv._2.equals("world"))
      if (kv._1.equals("scala")) assert(kv._2.equals("flink"))
    })
  }

  /**
   * 测试@Config注解
   */
  @Test
  def testConfig: Unit = {
    assert(this.conf.getInt("spark.max.parallelism", 10240) == 11)
  }

  /**
   * hive 注解断言
   */
  @Test
  def testHive: Unit = {
    // @Hive注解优先级低于@Config
    assert(FireHiveConf.hiveCluster.equals("batch"))
    assert(FireHiveConf.hiveVersion.equals("1.1.1"))
    assert(FireHiveConf.hiveCatalogName.equals("hive_catalog"))
    assert(FireHiveConf.partitionName.equals("dt"))
    this.logInfo("assert hive annotation success.")
  }

  /**
   * hbase 注解断言
   */
  @Test
  def tesHBase: Unit = {
    assert(FireHBaseConf.hbaseCluster().equals("batch-new1"))
    assert(FireHBaseConf.hbaseCluster(2).equals("batch-new2"))
    assert(FireHBaseConf.hbaseCluster(3).equals("batch-new3"))

    assert(FireHBaseConf.hbaseDurability(1).equals("off"))
    assert(!FireHBaseConf.tableExistsCache(2))
    assert(FireHBaseConf.familyName(3).equals("data"))

    assert(FireHBaseConf.hbaseHadoopScanPartitions() == 12)
    assert(FireHBaseConf.hbaseHadoopScanPartitions(2) == 1200)
    assert(FireHBaseConf.hbaseBatchSize() == 10)
    assert(FireHBaseConf.hbaseBatchSize(2) == 10)
    assert(FireHBaseConf.hbaseMaxRetry(3) == 5)
    assert(FireHBaseConf.hbaseMaxRetry(2) == 3)
    assert(FireHBaseConf.hbaseStorageLevel(2).equals("MEMORY_ONLY"))

    assert(FireHBaseConf.hbaseBatchSize() == 10)
    assert(FireHBaseConf.hbaseHadoopScanPartitions(3) == 11)

    assert(this.conf.getString("spark.fire.hbase.conf.hbase.zookeeper.property.clientPort").equals("2181"))
    assert(this.conf.getString("fire.hbase.conf.zookeeper.znode.parent2").equals("/hbase2"))
    assert(this.conf.getString("spark.fire.hbase.conf.hbase.zookeeper.property.clientPort3").equals("2183"))
  }
}
