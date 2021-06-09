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

package com.zto.fire.examples.spark

import java.util.Date

import com.zto.fire._
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.examples.bean.Hudi
import com.zto.fire.spark.BaseSparkCore
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.SaveMode._

/**
 * hudi测试
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-02-07 13:50
 */
object HudiTest extends BaseSparkCore {
  val tableName = "t_hudi"
  val basePath = "J:\\hudi"
  val dataGen = new DataGenerator

  /**
   * 将DataFrame Overwrite到指定的路径下
   */
  def insert: Unit = {
    val df = this.spark.createDataFrame(Hudi.newHudiList(), classOf[Hudi])
    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD_OPT_KEY, "id")
      .option(RECORDKEY_FIELD_OPT_KEY, "name")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "ds")
      .option(TABLE_NAME, tableName)
      .mode(Overwrite)
      .save(basePath)
  }


  /**
   * 将DataFrame Overwrite到指定的路径下，并将表信息同步到hive元数据中
   */
  def insertHive: Unit = {
    val df = this.spark.createDataFrame(Hudi.newHudiList(), classOf[Hudi])
    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      // 设置主键列名
      .option(PRECOMBINE_FIELD_OPT_KEY, "id")
      // 设置数据更新时间的列名
      .option(RECORDKEY_FIELD_OPT_KEY, "name")
      // 分区列设置
      .option(PARTITIONPATH_FIELD_OPT_KEY, "ds")
      .option(TABLE_NAME, tableName)
      // 设置要同步的hive库名
      .option(HIVE_DATABASE_OPT_KEY, "tmp")
      // 设置要同步的hive表名
      .option(HIVE_TABLE_OPT_KEY, "t_hudi")
      // 设置数据集注册并同步到hive
      .option(HIVE_SYNC_ENABLED_OPT_KEY, "true")
      .option(META_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置要同步的分区列名
      .option(HIVE_PARTITION_FIELDS_OPT_KEY, "ds")
      // 设置jdbc 连接同步
      .option(HIVE_URL_OPT_KEY, this.conf.getString("hive.jdbc.url"))
      .option(HIVE_USER_OPT_KEY, "admin")
      .option(HIVE_PASS_OPT_KEY, this.conf.getString("hive.jdbc.password"))
      // hudi表名称设置
      // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
      .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(Overwrite)
      .save("hdfs://ns1/user/hive/warehouse/tmp.db/t_hudi")
  }

  /**
   * 根据给定的DataFrame进行更新操作
   */
  def update: Unit = {
    val df = this.spark.createDataFrame(Seq(new Hudi(1L, "admin", 122, true), new Hudi(2L, "root", 222, false)), classOf[Hudi])
    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD_OPT_KEY, "id")
      .option(RECORDKEY_FIELD_OPT_KEY, "name")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "ds")
      .option(TABLE_NAME, tableName)
      // 将mode从Overwrite改成Append就是更新操作
      .mode(Append)
      .save(basePath)
  }

  /**
   * 根据给定的DataFrame进行删除操作
   */
  def delete: Unit = {
    val df = this.spark.createDataFrame(Seq(new Hudi(1L, "admin", 122, true)), classOf[Hudi])
    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      // 执行delete操作
      .option(OPERATION_OPT_KEY,"delete")
      .option(PRECOMBINE_FIELD_OPT_KEY, "id")
      .option(RECORDKEY_FIELD_OPT_KEY, "name")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "ds")
      .option("hoodie.bulkinsert.shuffle.parallelism", 2)
      .option("insert_shuffle_parallelism", 2)
      .option("upsert_shuffle_parallelism", 2)
      .option(TABLE_NAME, tableName)
      // 将mode从Overwrite改成Append就是更新操作
      .mode(Append)
      .save(basePath)
  }

  /**
   * 从hudi中读取数据
   */
  def read: Unit = {
    val roViewDF = this.spark
      .read.format("org.apache.hudi")
      // /*的个数与PARTITIONPATH_FIELD_OPT_KEY指定的目录级数有关，如果分区路径是：region/country/city，则是四个/*
      // 如果分区路径是ds=20200208这种，则是两个/*。所以这个 /*数=PARTITIONPATH_FIELD_OPT_KEY+1
      .load(basePath + "/*/*")
    //load(basePath) 如果使用 "/partitionKey=partitionValue" 文件夹命名格式，Spark将自动识别分区信息

    roViewDF.createOrReplaceTempView(tableName)
    spark.sql(s"select * from $tableName order by id").show(false)
  }

  /**
   * 从hudi中读取数据
   */
  def readHDFS: Unit = {
    val roViewDF = this.spark
      .read.format("org.apache.hudi")
      // /*的个数与PARTITIONPATH_FIELD_OPT_KEY指定的目录级数有关，如果分区路径是：region/country/city，则是四个/*
      // 如果分区路径是ds=20200208这种，则是两个/*。所以这个 /*数=PARTITIONPATH_FIELD_OPT_KEY+1
      .load("hdfs://ns1/tmp/hudi2/*")
    //load(basePath) 如果使用 "/partitionKey=partitionValue" 文件夹命名格式，Spark将自动识别分区信息

    roViewDF.createOrReplaceTempView(tableName)
    spark.sql(s"select * from $tableName order by id").show(false)
  }

  /**
   * 增量查询
   */
  def readNew: Unit = {
    val newDF = this.spark.read.format("org.apache.hudi")
      .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL)
      // 指定beginTime，只查询从beginTime之后的最新数据
      .option(BEGIN_INSTANTTIME_OPT_KEY, DateFormatUtils.addSecs(new Date(), -20))
      .load(basePath + "/*/*")
    newDF.createOrReplaceTempView("new_table")
    this.spark.sql("select * from new_table").show(false)
  }

  override def process: Unit = {
    /*this.insert
    println("step 1.读取数据")
    this.read
    this.update
    println("step 2.读取更新后的数据")
    this.readNew
    this.delete
    println("step 2.读取删除后的数据")
    this.read*/
    this.insertHive
    // this.readHDFS
  }
}
