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

package com.zto.fire.spark.connector

import com.zto.fire.common.anno.Internal
import com.zto.fire.core.connector.{Connector, ConnectorFactory}
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.{HBaseBaseBean, MultiVersionsBean}
import com.zto.fire.hbase.conf.FireHBaseConf
import com.zto.fire.predef._
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * HBase直连工具类，基于HBase-Spark API开发
 * 具有更强大的性能和更低的资源开销，适用于
 * 与Spark相结合的大数据量操作，优点体现在并行
 * 和大数据量。如果数据量不大，仍推荐使用
 * HBaseConnector进行相关的操作
 *
 * @param sc
 * SparkContext实例
 * @param config
 * HBase相关配置参数
 * @author ChengLong 2018年4月10日 10:39:28
 */
class HBaseBulkConnector(@scala.transient sc: SparkContext, @scala.transient config: Configuration, batchSize: Int = 10000, keyNum: Int = 1)
  extends HBaseContext(sc, config) with Connector {
  private[fire] lazy val finalBatchSize = if (FireHBaseConf.hbaseBatchSize(this.keyNum) != -1) FireHBaseConf.hbaseBatchSize(this.keyNum) else this.batchSize
  private[this] lazy val sparkSession = SparkSingletonFactory.getSparkSession
  @transient
  private[this] lazy val tableConfMap = new JConcurrentHashMap[String, Configuration]()

  /**
   * 根据RDD[String]批量删除，rdd是rowkey的集合
   * 类型为String
   *
   * @param rdd
   * 类型为String的RDD数据集
   * @param tableName
   * HBase表名
   */
  def bulkDeleteRDD(tableName: String, rdd: RDD[String]): Unit = {
    requireNonEmpty(tableName, rdd)
    tryWithLog {
      val rowKeyRDD = rdd.filter(rowkey => StringUtils.isNotBlank(rowkey)).map(rowKey => Bytes.toBytes(rowKey))
      this.bulkDelete[Array[Byte]](rowKeyRDD, TableName.valueOf(tableName), rec => new Delete(rec), this.finalBatchSize)
    }(this.logger, s"execute bulkDeleteRDD(tableName: ${tableName}, batchSize: ${batchSize}) success. keyNum: ${keyNum}")
  }

  /**
   * 根据Dataset[String]批量删除，Dataset是rowkey的集合
   * 类型为String
   *
   * @param dataset
   * 类型为String的Dataset集合
   * @param tableName
   * HBase表名
   */
  def bulkDeleteDS(tableName: String, dataset: Dataset[String]): Unit = {
    requireNonEmpty(tableName, dataset)
    tryWithLog {
      this.bulkDeleteRDD(tableName, dataset.rdd)
    }(this.logger, s"execute bulkDeleteDS(tableName: ${tableName}, batchSize: ${finalBatchSize}) success. keyNum: ${keyNum}")
  }

  /**
   * 指定rowkey集合，进行批量删除操作内部会将这个集合转为RDD
   * 推荐在较大量数据时使用，小数据量的删除操作仍推荐使用HBaseConnector
   *
   * @param tableName
   * HBase表名
   * @param seq
   * 待删除的rowKey集合
   */
  def bulkDeleteList(tableName: String, seq: Seq[String]): Unit = {
    requireNonEmpty(tableName, seq)
    tryWithLog {
      val rdd = sc.parallelize(seq, math.max(1, math.min(seq.length / 2, FireSparkConf.parallelism)))
      this.bulkDeleteRDD(tableName, rdd)
    }(this.logger, s"execute bulkDeleteList(tableName: ${tableName}) success. keyNum: ${keyNum}")
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * rowKey集合，类型为RDD[String]
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def bulkGetRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E]): RDD[E] = {
    requireNonEmpty(tableName, rdd, clazz)

    tryWithReturn {
      val rowKeyRDD = rdd.filter(StringUtils.isNotBlank(_)).map(rowKey => Bytes.toBytes(rowKey))
      val getRDD = this.bulkGet[Array[Byte], E](TableName.valueOf(tableName), batchSize, rowKeyRDD, rowKey => new Get(rowKey), (result: Result) => {
        HBaseConnector(keyNum = this.keyNum).hbaseRow2Bean(result, clazz)
      }).filter(bean => bean != null).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
      getRDD
    }(this.logger, s"execute bulkGetRDD(tableName: ${tableName}, batchSize: ${finalBatchSize}) success. keyNum: ${keyNum}")
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * rowKey集合，类型为RDD[String]
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def bulkGetDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E]): DataFrame = {
    requireNonEmpty(tableName, rdd, clazz)
    tryWithReturn {
      val resultRdd = this.bulkGetRDD[E](tableName, rdd, clazz)
      this.sparkSession.createDataFrame(resultRdd, clazz)
    }(this.logger, s"execute bulkGetDF(tableName: ${tableName}, batchSize: ${finalBatchSize}) success. keyNum: ${keyNum}")
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * rowKey集合，类型为RDD[String]
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def bulkGetDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E]): Dataset[E] = {
    requireNonEmpty(tableName, rdd, clazz)
    tryWithReturn {
      val resultRdd = this.bulkGetRDD[E](tableName, rdd, clazz)
      this.sparkSession.createDataset(resultRdd)(Encoders.bean(clazz))
    }(this.logger, s"execute bulkGetDS(tableName: ${tableName}, batchSize: ${finalBatchSize}) success. keyNum: ${keyNum}")
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   * 内部实现是将rowkey集合转为RDD[String]，推荐在数据量较大
   * 时使用。数据量较小请优先使用HBaseConnector
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 具体类型
   * @param seq
   * rowKey集合
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def bulkGetSeq[E <: HBaseBaseBean[E] : ClassTag](tableName: String, seq: Seq[String], clazz: Class[E]): RDD[E] = {
    requireNonEmpty(tableName, seq, clazz)

    tryWithReturn {
      val rdd = sc.parallelize(seq, math.max(1, math.min(seq.length / 2, FireSparkConf.parallelism)))
      this.bulkGetRDD(tableName, rdd, clazz)
    }(this.logger, s"execute bulkGetSeq(tableName: ${tableName}, batchSize: ${finalBatchSize}) success. keyNum: ${keyNum}")
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * 数据集合，数类型需继承自HBaseBaseBean
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def bulkPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T]): Unit = {
    requireNonEmpty(tableName, rdd)

    tryWithLog {
      this.bulkPut[T](rdd,
        TableName.valueOf(tableName),
        (putRecord: T) => {
          HBaseConnector(keyNum = this.keyNum).convert2Put[T](if (HBaseConnector(keyNum = this.keyNum).getMultiVersion[T]) new MultiVersionsBean(putRecord).asInstanceOf[T] else putRecord, HBaseConnector(keyNum = this.keyNum).getNullable[T])
        })
    }(this.logger, s"execute bulkPutRDD(tableName: ${tableName}) success. keyNum: ${keyNum}")
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入。如果数据量
   * 较大，推荐使用。数据量过小则推荐使用HBaseConnector
   *
   * @param tableName
   * HBase表名
   * @param seq
   * 数据集，类型为HBaseBaseBean的子类
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   */
  def bulkPutSeq[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T]): Unit = {
    requireNonEmpty(tableName, seq)

    tryWithLog {
      val rdd = this.sc.parallelize(seq, math.max(1, math.min(seq.length / 2, FireSparkConf.parallelism)))
      this.bulkPutRDD(tableName, rdd)
    }(this.logger, s"execute bulkPutRDD(tableName: ${tableName}) success. keyNum: ${keyNum}")
  }

  /**
   * 定制化scan设置后从指定的表中scan数据
   * 并将scan到的结果集映射为自定义JavaBean对象
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 自定义JavaBean的Class对象
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   * @return
   * scan获取到的结果集，类型为RDD[T]
   */
  def bulkScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan)(implicit canOverload: Boolean = true): RDD[T] = {
    requireNonEmpty(tableName, scan, clazz)

    tryWithReturn {
      if (scan.getCaching == -1) {
        scan.setCaching(this.finalBatchSize)
      }
      this.hbaseRDD(TableName.valueOf(tableName), scan).mapPartitions(it => HBaseConnector(keyNum = this.keyNum).hbaseRow2BeanList(it, clazz)).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
    }(this.logger, s"execute bulkScanRDD(tableName: ${tableName}) success. keyNum: ${keyNum}")
  }

  /**
   * 指定startRow和stopRow后自动创建scan对象完成数据扫描
   * 并将scan到的结果集映射为自定义JavaBean对象
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowkey的起始
   * @param stopRow
   * rowkey的结束
   * @param clazz
   * 自定义JavaBean的Class对象
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   * @return
   * scan获取到的结果集，类型为RDD[T]
   */
  def bulkScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String): RDD[T] = {
    requireNonEmpty(tableName, clazz, startRow, stopRow)
    this.bulkScanRDD(tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param dataFrame
   * dataFrame实例，数类型需继承自HBaseBaseBean
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def bulkPutDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[T]): Unit = {
    requireNonEmpty(tableName, dataFrame, clazz)

    val rdd = dataFrame.rdd.mapPartitions(it => SparkUtils.sparkRowToBean(it, clazz))
    this.bulkPutRDD[T](tableName, rdd)
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param dataset
   * dataFrame实例，数类型需继承自HBaseBaseBean
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def bulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataset: Dataset[T]): Unit = {
    requireNonEmpty(tableName, dataset)

    this.bulkPutRDD[T](tableName, dataset.rdd)
  }

  /**
   * 用于已经映射为指定类型的DStream实时
   * 批量写入至HBase表中
   *
   * @param tableName
   * HBase表名
   * @param dstream
   * 类型为自定义JavaBean的DStream流
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   */
  def bulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dstream: DStream[T]): Unit = {
    requireNonEmpty(tableName, dstream)

    tryWithLog {
      this.streamBulkPut[T](dstream, TableName.valueOf(tableName), (putRecord: T) => {
        HBaseConnector(keyNum = this.keyNum).convert2Put[T](if (HBaseConnector(keyNum = this.keyNum).getMultiVersion[T]) new MultiVersionsBean(putRecord).asInstanceOf[T] else putRecord, HBaseConnector(keyNum = this.keyNum).getNullable[T])
      })
    }(this.logger, s"execute bulkPutStream(tableName: ${tableName}) success. keyNum: ${keyNum}")
  }

  /**
   * 以spark 方式批量将rdd数据写入到hbase中
   *
   * @param rdd
   * 类型为HBaseBaseBean子类的rdd
   * @param tableName
   * hbase表名
   * @tparam T
   * 数据类型
   */
  def hadoopPut[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T]): Unit = {
    requireNonEmpty(tableName, rdd)

    tryWithLog {
      rdd.mapPartitions(it => {
        val putList = ListBuffer[(ImmutableBytesWritable, Put)]()
        it.foreach(t => {
          putList += Tuple2(new ImmutableBytesWritable(), HBaseConnector(keyNum = this.keyNum).convert2Put[T](t, HBaseConnector(keyNum = this.keyNum).getNullable[T]))
        })
        putList.iterator
      }).saveAsNewAPIHadoopDataset(this.getConfiguration(tableName))
    }(this.logger, s"execute hadoopPut(tableName: ${tableName}) success. keyNum: ${keyNum}")
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hadoopPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[E]): Unit = {
    requireNonEmpty(tableName, dataFrame, clazz)

    val rdd = dataFrame.rdd.mapPartitions(it => SparkUtils.sparkRowToBean(it, clazz))
    this.hadoopPut[E](tableName, rdd)
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param dataset
   * JavaBean类型，待插入到hbase的数据集
   */
  def hadoopPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E]): Unit = {
    requireNonEmpty(tableName, dataset)("参数不合法：dataset不能为空")
    this.hadoopPut[E](tableName, dataset.rdd)
  }

  /**
   * 以spark 方式批量将DataFrame数据写入到hbase中
   * 注：此方法与hbaseHadoopPutDF不同之处在于，它不强制要求该DataFrame一定要与HBaseBaseBean的子类对应
   * 但需要指定rowKey的构建规则，相对与hbaseHadoopPutDF来说，少了中间的两次转换，性能会更高
   *
   * @param df
   * spark的DataFrame
   * @param tableName
   * hbase表名
   * @tparam T
   * JavaBean类型
   */
  def hadoopPutDFRow[T <: HBaseBaseBean[T] : ClassTag](tableName: String, df: DataFrame, buildRowKey: (Row) => String): Unit = {
    requireNonEmpty(tableName, df)
    val insertEmpty = HBaseConnector(keyNum = this.keyNum).getNullable[T]
    tryWithLog {
      val fields = df.schema.fields
      df.rdd.mapPartitions(it => {
        var count = 0
        val putList = ListBuffer[(ImmutableBytesWritable, Put)]()
        it.foreach(row => {
          val put = new Put(Bytes.toBytes(buildRowKey(row)))
          fields.foreach(field => {
            val fieldName = field.name
            val fieldIndex = row.fieldIndex(fieldName)
            val dataType = field.dataType.getClass.getSimpleName
            var fieldValue: Any = null
            if (!row.isNullAt(fieldIndex)) {
              fieldValue = row.get(fieldIndex)
              if (dataType.contains("StringType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.String]))
              } else if (dataType.contains("IntegerType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Integer]))
              } else if (dataType.contains("DoubleType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Double]))
              } else if (dataType.contains("LongType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Long]))
              } else if (dataType.contains("DecimalType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.math.BigDecimal]))
              } else if (dataType.contains("FloatType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Float]))
              } else if (dataType.contains("BooleanType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Boolean]))
              } else if (dataType.contains("ShortType")) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Short]))
              } else if (dataType.contains("NullType") && insertEmpty) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), null)
              }
            } else if (insertEmpty) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), null)
            }
          })
          putList += Tuple2(new ImmutableBytesWritable, put)
          count += putList.size
        })
        putList.iterator
      }).saveAsNewAPIHadoopDataset(this.getConfiguration(tableName))
    }(this.logger, s"execute hadoopPut(tableName: ${tableName}) success. keyNum: ${keyNum}")
  }

  /**
   * 根据表名构建hadoop configuration
   *
   * @param tableName
   * HBase表名
   * @return
   * hadoop configuration
   */
  @Internal
  private[this] def getConfiguration(tableName: String): Configuration = {
    requireNonEmpty(tableName)

    if (!this.tableConfMap.containsKey(tableName)) {
      val hadoopConfiguration = this.config
      hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      val job = Job.getInstance(hadoopConfiguration)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      this.tableConfMap.put(tableName, job.getConfiguration)
    }

    this.tableConfMap.get(tableName)
  }
}

/**
 * 用于单例构建伴生类HBaseContextExt的实例对象
 * 每个HBaseContextExt实例使用keyNum作为标识，并且与每个HBase集群一一对应
 */
private[fire] object HBaseBulkConnector extends ConnectorFactory[HBaseBulkConnector] with HBaseBulkFunctions {

  /**
   * 创建指定集群标识的HBaseContextExt对象实例
   */
  override protected def create(conf: Any = null, keyNum: Int = 1): HBaseBulkConnector = {
    val hadoopConf = if (conf != null) conf.asInstanceOf[Configuration] else HBaseConnector.getConfiguration(keyNum)
    val connector = new HBaseBulkConnector(SparkSingletonFactory.getSparkSession.sparkContext, hadoopConf, keyNum)
    connector
  }

}