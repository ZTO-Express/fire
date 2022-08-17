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

package com.zto.fire.examples.spark.hive

import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.spark.SparkCore

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |spark.sql.statistics.size.autoUpdate.enabled=true
    |""")
@Hive("test")
// 配置消费的kafka信息
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object HiveMetadataTest extends SparkCore {
  val sourceTable = "ods.mdb_md_dbs"
  val partitionTable = "dw.mdb_md_dbs_fire_orc"
  val multiPartitionTable = "tmp.mdb_md_dbs_fire_multi_partition_orc"
  val bucketTable = "tmp.mdb_md_dbs_fire_bucket"

  override def process: Unit = {
    this.testSinglePartitionTable
    // this.testMultiPartitionTable
    // this.testNoPartitionTable

    sql(
      s"""
        |INSERT INTO ${bucketTable}
        |SELECT * FROM VALUES(1,1,1)
        |""".stripMargin)
  }

  /**
   * 测试分区表更新hive元数据用例
   */
  def testMultiPartitionTable: Unit = {
    sql(
      s"""
         |insert into table ${multiPartitionTable} partition(ds, city) select *,'sh' as city from dw.mdb_md_dbs where ds='20211001' limit 100
         |""".stripMargin)
    (1 to 3).foreach(x => {
      sql(
        s"""
           |insert into table ${multiPartitionTable} partition(ds, city) select *,'sh' as city from dw.mdb_md_dbs where ds='20211001' limit 100
           |""".stripMargin)
    })

    (1 to 3).foreach(x => {
      sql(
        s"""
           |insert overwrite table ${multiPartitionTable} partition(ds, city) select *,'bj' as city from dw.mdb_md_dbs where ds='20211001' limit 100
           |""".stripMargin)
    })
  }

  /**
   * 测试非分区表更新hive元数据用例
   */
  def testNoPartitionTable: Unit = {
    (1 to 3).foreach(x => {
      // orc非分区表
      sql(
        s"""
          |insert into table dw.mdb_md_dbs_fire_orc_nopart select * from ${sourceTable} where ds='20190619' limit 10
          |""".stripMargin)
      sql(
        """
          |select count(1) from dw.mdb_md_dbs_fire_orc_nopart
          |""".stripMargin).show()

      // text非分区表
      sql(
        """
          |insert into table tmp.mdb_md_dbs_fire_txt partition(ds) select * from tmp.mdb_md_dbs_fire where ds='20190620' limit 10
          |""".stripMargin)
    })
  }

  /**
   * 测试分区表更新hive元数据用例
   */
  def testSinglePartitionTable: Unit = {
    (1 to 3).foreach(_ => {
      sql(
        """
          |insert into table tmp.mdb_md_dbs_fire_txt partition(ds) select * from tmp.mdb_md_dbs_fire where ds='20190619' limit 10
          |""".stripMargin)
    })

    // orc分区表
    sql(s"""drop table if exists ${partitionTable}2""")
    sql(
      s"""
         |create table if not exists ${partitionTable}2 like ${partitionTable}
         |""".stripMargin)
    sql(
      s"""
         |insert into table ${partitionTable}2 partition(ds) select * from dw.mdb_md_dbs where ds='20211001' limit 100
         |""".stripMargin)
    var partition = 20211002
    (1 to 3).foreach(x => {
      sql(s"""alter table ${partitionTable}2 PARTITION (ds='20211001') RENAME TO PARTITION (ds='${partition}')""")
      partition = partition + 1
      sql(
        s"""
           |insert into table ${partitionTable}2 partition(ds) select * from dw.mdb_md_dbs where ds='20211001' limit 100
           |""".stripMargin)
    })

    (1 to 3).foreach(x => {
      sql(
        s"""
           |insert into table ${partitionTable}2 partition(ds) select * from ${partitionTable}2
           |""".stripMargin)
    })
  }

  val jdbc =
    """
      |use hive;
      |-- orc分区表
      |select * from PARTITION_PARAMS where PART_ID in (
      |	select PART_ID from PARTITIONS p where TBL_ID = (SELECT TBL_ID FROM TBLS t where t.TBL_NAME = 'mdb_md_dbs_fire_orc2')
      |);
      |
      |SELECT * from TABLE_PARAMS t where t.TBL_ID = (SELECT TBL_ID FROM TBLS t where t.TBL_NAME = 'mdb_md_dbs_fire_orc')
      |
      |-- orc非分区表
      |SELECT * from TABLE_PARAMS t where t.TBL_ID = (SELECT TBL_ID FROM TBLS t where t.TBL_NAME = 'mdb_md_dbs_fire_orc_nopart')
      |
      |SELECT * from TABLE_PARAMS t where t.TBL_ID = (SELECT TBL_ID FROM TBLS t where t.TBL_NAME = 'mdb_md_dbs_fire_txt')
      |
      |
      |-- textfile分区表
      |select * from PARTITION_PARAMS where PART_ID = (
      |	select PART_ID from PARTITIONS p where TBL_ID = (SELECT TBL_ID FROM TBLS t where t.TBL_NAME = 'mdb_md_dbs_fire_txt')
      |);
      |""".stripMargin
}
