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

package com.zto

import com.zto.fire.core.ext.BaseFireExt
import com.zto.fire.spark.ext.core._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * 预定义fire框架中的扩展工具
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-22 13:41
 */
package object fire extends BaseFireExt {

  /**
   * SparkContext扩展
   *
   * @param spark
   * sparkSession对象
   */
  implicit class SparkSessionExtBridge(spark: SparkSession) extends SparkSessionExt(spark) {

  }

  /**
   * SparkContext扩展
   *
   * @param sc
   * SparkContext对象
   */
  implicit class SparkContextExtBridge(sc: SparkContext) extends SparkContextExt(sc) {

  }


  /**
   * RDD相关的扩展
   *
   * @param rdd
   * rdd
   */
  implicit class RDDExtBridge[T: ClassTag](rdd: RDD[T]) extends RDDExt[T](rdd) {

  }

  /**
   * SparkConf扩展
   *
   * @param sparkConf
   * sparkConf对象
   */
  implicit class SparkConfExtBridge(sparkConf: SparkConf) extends SparkConfExt(sparkConf) {

  }

  /**
   * SQLContext与HiveContext扩展
   *
   * @param sqlContext
   * sqlContext对象
   */
  implicit class SQLContextExtBridge(sqlContext: SQLContext) extends SQLContextExt(sqlContext) {

  }

  /**
   * DataFrame扩展
   *
   * @param dataFrame
   * dataFrame实例
   */
  implicit class DataFrameExtBridge(dataFrame: DataFrame) extends DataFrameExt(dataFrame) {

  }

  /**
   * Dataset扩展
   *
   * @param dataset
   * dataset对象
   */
  implicit class DatasetExtBridge[T: ClassTag](dataset: Dataset[T]) extends DatasetExt[T](dataset) {

  }

  /**
   * StreamingContext扩展
   *
   * @param ssc
   * StreamingContext对象
   */
  implicit class StreamingContextExtBridge(ssc: StreamingContext) extends StreamingContextExt(ssc){

  }


  /**
   * DStream扩展
   *
   * @param stream
   * stream对象
   */
  implicit class DStreamExtBridge[T: ClassTag](stream: DStream[T]) extends DStreamExt[T](stream) {

  }
}
