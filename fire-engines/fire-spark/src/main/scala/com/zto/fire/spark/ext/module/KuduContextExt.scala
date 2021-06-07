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

package com.zto.fire.spark.ext.module

import java.lang.reflect.Field
import java.sql._
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.zto.fire._
import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.conf.FireKuduConf
import com.zto.fire.common.util.{ReflectionUtils, ValueUtils}
import com.zto.fire.jdbc.util.DBUtils
import com.zto.fire.spark.bean.KuduBaseBean
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.util.{KuduUtils, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.client.{CreateTableOptions, KuduTable}
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect._

/**
 * Kudu相关扩展API
 * Created by ChengLong on 2017-09-21.
 */
class KuduContextExt(val sqlContext: SQLContext, val kuduContext: KuduContext) extends Serializable {
  private[this] lazy val paramErrorMsg = "参数不能为空"

  /**
   * 用于维护kudu表与临时表之间的关系
   */
  private val tableMap = mutable.Map[String, String]()

  /**
   * 将表名包装为以impala::开头的表
   *
   * @param tableName
   * 库名.表名
   * @return
   * 包装后的表名
   */
  def packageKuduTableName(tableName: String): String = {
    require(StringUtils.isNotBlank(tableName), "表名不能为空")
    if (tableName.startsWith("impala::")) {
      tableName
    } else {
      s"impala::$tableName"
    }
  }

  /**
   * 插入多个实体bean到指定kudu表中
   *
   * @param tableName
   * 表名
   * @param beans
   * 多个与表结构对应的实体bean
   * @tparam T
   * 具体类型
   */
  def insertIgnoreBeans[T: ClassTag](tableName: String, beans: T*): Unit = {
    this.insertIgnoreList(tableName, beans)
  }

  /**
   * 插入多个实体bean到指定kudu表中
   *
   * @param tableName
   * 表名
   * @param beanSeq
   * 多个与表结构对应的实体bean
   * @tparam T
   * 具体类型
   */
  def insertIgnoreList[T: ClassTag](tableName: String, beanSeq: Seq[T]): Unit = {
    val beanRDD = sqlContext.sparkContext.parallelize(beanSeq, FireSparkConf.parallelism)
    this.insertIgnoreRDD(tableName, beanRDD)
  }

  /**
   * 插入RDD到指定kudu表中
   *
   * @param tableName
   * 表名
   * @param rdd
   * 多个与表结构对应的实体bean
   * @tparam T
   * 具体类型
   */
  def insertIgnoreRDD[T: ClassTag](tableName: String, rdd: RDD[T]): Unit = {
    require(rdd != null, paramErrorMsg)
    val df = sqlContext.createDataFrame(rdd, classTag[T].runtimeClass)
    this.insertIgnoreRows(tableName, df)
  }

  /**
   * 向指定表插入DataFrame
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * Spark DataFrame. 要与表结构对应
   */
  def insertIgnoreRows(tableName: String, dataFrame: DataFrame): Unit = {
    this.kuduContext.insertIgnoreRows(dataFrame, this.packageKuduTableName(tableName))
  }

  /**
   * 向指定表插入DataFrame
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * Spark DataFrame. 要与表结构对应
   */
  def insertIgnoreDataFrame(tableName: String, dataFrame: DataFrame): Unit = {
    this.insertIgnoreRows(tableName, dataFrame)
  }

  /**
   * 插入多个实体bean到指定kudu表中
   *
   * @param tableName
   * 表名
   * @param beans
   * 多个与表结构对应的实体bean
   * @tparam T
   * 具体类型
   */
  def insertBeans[T: ClassTag](tableName: String, beans: T*): Unit = {
    this.insertList(tableName, beans)
  }

  /**
   * 插入多个实体bean到指定kudu表中
   *
   * @param tableName
   * 表名
   * @param beanSeq
   * 多个与表结构对应的实体bean
   * @tparam T
   * 具体类型
   */
  def insertList[T: ClassTag](tableName: String, beanSeq: Seq[T]): Unit = {
    val beanRDD = sqlContext.sparkContext.parallelize(beanSeq, FireSparkConf.parallelism)
    this.insertRDD(tableName, beanRDD)
  }

  /**
   * 插入RDD到指定kudu表中
   *
   * @param tableName
   * 表名
   * @param rdd
   * 多个与表结构对应的实体bean
   * @tparam T
   * 具体类型
   */
  def insertRDD[T: ClassTag](tableName: String, rdd: RDD[T]): Unit = {
    require(rdd != null, paramErrorMsg)
    val df = sqlContext.createDataFrame(rdd, classTag[T].runtimeClass)
    this.insertRows(tableName, df)
  }

  /**
   * 向指定表插入DataFrame
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * Spark DataFrame. 要与表结构对应
   */
  def insertRows(tableName: String, dataFrame: DataFrame): Unit = {
    this.kuduContext.insertRows(dataFrame, this.packageKuduTableName(tableName))
  }

  /**
   * 向指定表插入DataFrame
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * Spark DataFrame. 要与表结构对应
   */
  def insertDataFrame(tableName: String, dataFrame: DataFrame): Unit = {
    this.insertRows(tableName, dataFrame)
  }

  /**
   * 使用DataFrame更新kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * 与表结构对应的DataFrame
   */
  def updateRows(tableName: String, dataFrame: DataFrame): Unit = {
    this.kuduContext.updateRows(dataFrame, this.packageKuduTableName(tableName))
  }

  /**
   * 更新插入数据到kudu表中
   *
   * @param tableName
   * kudu表名
   * @param dataFrame
   * 数据集
   */
  def upsertRows(tableName: String, dataFrame: DataFrame): Unit = {
    this.kuduContext.upsertRows(dataFrame, this.packageKuduTableName(tableName))
  }

  /**
   * 使用DataFrame更新插入kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * 与表结构对应的DataFrame
   */
  def upsertDataFrame(tableName: String, dataFrame: DataFrame): Unit = {
    this.upsertRows(tableName, dataFrame)
  }

  /**
   * 使用RDD更新插入kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param rdd
   * 与表结构对应的RDD
   */
  def upsertRDD[T: ClassTag](tableName: String, rdd: RDD[T]): Unit = {
    require(rdd != null, paramErrorMsg)
    val df = sqlContext.createDataFrame(rdd, classTag[T].runtimeClass)
    this.upsertRows(tableName, df)
  }

  /**
   * 使用多个实体bean更新插入kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param beans
   * 与表结构对应的多个实体bean
   */
  def upsertBeans[T: ClassTag](tableName: String, beans: T*): Unit = {
    val beanRDD = sqlContext.sparkContext.parallelize(beans, 1)
    this.upsertRDD(tableName, beanRDD)
  }

  /**
   * 使用多个实体bean集合更新插入kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param beanSeq
   * 与表结构对应的多个实体bean
   */
  def upsertList[T: ClassTag](tableName: String, beanSeq: Seq[T]): Unit = {
    val beanRDD = sqlContext.sparkContext.parallelize(beanSeq, FireSparkConf.parallelism)
    val df = sqlContext.createDataFrame(beanRDD, classTag[T].runtimeClass)
    this.upsertRows(tableName, df)
  }

  /**
   * 使用DataFrame更新kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * 与表结构对应的DataFrame
   */
  def updateDataFrame(tableName: String, dataFrame: DataFrame): Unit = {
    this.updateRows(tableName, dataFrame)
  }

  /**
   * 使用RDD更新kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param rdd
   * 与表结构对应的RDD
   */
  def updateRDD[T: ClassTag](tableName: String, rdd: RDD[T]): Unit = {
    require(rdd != null, paramErrorMsg)
    val df = sqlContext.createDataFrame(rdd, classTag[T].runtimeClass)
    this.updateRows(tableName, df)
  }

  /**
   * 使用多个实体bean更新kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param beans
   * 与表结构对应的多个实体bean
   */
  def updateBeans[T: ClassTag](tableName: String, beans: T*): Unit = {
    val beanRDD = sqlContext.sparkContext.parallelize(beans, 1)
    this.updateRDD(tableName, beanRDD)
  }

  /**
   * 使用多个实体bean集合更新kudu表中的数据
   *
   * @param tableName
   * 表名
   * @param beanSeq
   * 与表结构对应的多个实体bean
   */
  def updateList[T: ClassTag](tableName: String, beanSeq: Seq[T]): Unit = {
    val beanRDD = sqlContext.sparkContext.parallelize(beanSeq, FireSparkConf.parallelism)
    val df = sqlContext.createDataFrame(beanRDD, classTag[T].runtimeClass)
    this.updateRows(tableName, df)
  }

  /**
   * 使用多个实体bean集合删除kudu表中的多条数据
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * 与表结构对应的DataFrame
   */
  def deleteRows(tableName: String, dataFrame: DataFrame): Unit = {
    require(dataFrame != null && StringUtils.isNotBlank(tableName), paramErrorMsg)
    this.kuduContext.deleteRows(dataFrame, this.packageKuduTableName(tableName))
  }

  /**
   * 使用多个实体bean集合删除kudu表中的多条数据
   *
   * @param tableName
   * 表名
   * @param dataFrame
   * 与表结构对应的DataFrame
   */
  def deleteDataFrame(tableName: String, dataFrame: DataFrame): Unit = {
    this.deleteRows(tableName, dataFrame)
  }

  /**
   * 使用rdd集合删除kudu表中的多条数据
   *
   * @param tableName
   * 表名
   * 与表结构对应的rdd
   */
  def deleteKVRDD[T: ClassTag](tableName: String, kv: (String, RDD[_])*): Unit = {
    if (!this.tableExists(tableName)) throw new IllegalArgumentException(s"表名${tableName}不存在")
    val keys = mutable.LinkedHashMap[String, Class[_]]()
    val head = kv.head
    var rdd: RDD[_] = head._2
    keys += (head._1 -> rdd.first().getClass)
    kv.toList.tail.foreach(t => {
      keys += (t._1 -> t._2.first().getClass)
      rdd = rdd.zip(t._2).map(t => {
        if (t._1.isInstanceOf[(Any, Any)]) {
          val tmp = t._1.asInstanceOf[(Any, Any)]
          (tmp._1, tmp._2, t._2)
        } else if (t._1.isInstanceOf[(Any, Any, Any)]) {
          val tmp = t._1.asInstanceOf[(Any, Any, Any)]
          (tmp._1, tmp._2, tmp._3, t._2)
        } else if (t._1.isInstanceOf[(Any, Any, Any, Any)]) {
          val tmp = t._1.asInstanceOf[(Any, Any, Any, Any)]
          (tmp._1, tmp._2, tmp._3, tmp._4, t._2)
        } else if (t._1.isInstanceOf[(Any, Any, Any, Any, Any)]) {
          val tmp = t._1.asInstanceOf[(Any, Any, Any, Any, Any)]
          (tmp._1, tmp._2, tmp._3, tmp._4, tmp._5, t._2)
        } else {
          t
        }
      })
    })

    val tmpRDD = rdd.map(t => {
      if (t.isInstanceOf[(Any, Any)]) {
        val tmp = t.asInstanceOf[(Any, Any)]
        Row(tmp._1, tmp._2)
      } else if (t.isInstanceOf[(Any, Any, Any)]) {
        val tmp = t.asInstanceOf[(Any, Any, Any)]
        Row(tmp._1, tmp._2, tmp._3)
      } else if (t.isInstanceOf[(Any, Any, Any, Any)]) {
        val tmp = t.asInstanceOf[(Any, Any, Any, Any)]
        Row(tmp._1, tmp._2, tmp._3, tmp._4)
      } else if (t.isInstanceOf[(Any, Any, Any, Any, Any)]) {
        val tmp = t.asInstanceOf[(Any, Any, Any, Any, Any)]
        Row(tmp._1, tmp._2, tmp._3, tmp._4, tmp._5)
      } else {
        Row(t)
      }
    })

    val structType = ListBuffer[StructField]()
    keys.foreach(key => {
      val dataType: DataType = if (key._2 == classOf[java.lang.Integer]) {
        IntegerType
      } else if (key._2 == classOf[java.lang.Long]) {
        LongType
      } else {
        StringType
      }
      structType += StructField(key._1, dataType, nullable = false)
    })
    val df = sqlContext.createDataFrame(tmpRDD, StructType(structType.toArray))
    this.deleteRows(tableName, df)
  }

  /**
   * 使用实体bean集合删除kudu表中的多条数据
   *
   * @param tableName
   * 表名
   * 与表结构对应的实体bean集合
   */
  def deleteKVList[T: ClassTag](tableName: String, kv: (String, Seq[_])*): Unit = {
    val params = new ListBuffer[(String, RDD[_])]()
    kv.foreach(t => {
      val fieldName = t._1
      val rdd = sqlContext.sparkContext.parallelize(t._2, 1)
      params += (fieldName -> rdd)
    })
    this.deleteKVRDD(tableName, params: _*)
  }

  /**
   * 删除多条记录
   *
   * @param tableName
   * 表名
   * @param beans
   * 多个JavaBean实例
   * @tparam T
   * 类型
   */
  def deleteBeans[T: ClassTag](tableName: String, beans: T*): Unit = {
    this.deleteList(tableName, beans)
  }

  /**
   * 删除多条记录
   *
   * @param tableName
   * 表名
   * @param beans
   * 多个JavaBean实例
   * @tparam T
   * 类型
   */
  def deleteList[T: ClassTag](tableName: String, beans: Seq[T]): Unit = {
    val beanClazz = classTag[T].runtimeClass
    val rdd = this.sqlContext.sparkContext.parallelize(beans, 1)
    val rowRDD = rdd.map(bean => {
      KuduUtils.kuduBean2Row(bean)
    })
    val list = KuduUtils.buildSchemaFromKuduBean(beanClazz)
    val df = this.sqlContext.createDataFrame(rowRDD, StructType(list))
    df.show()
    this.deleteDataFrame(tableName, df)
  }

  /**
   * 判断指定表是否存在
   *
   * @param tableName
   * 表名
   * @return
   * 存在、不存在
   */
  def tableExists(tableName: String): Boolean = {
    if (StringUtils.isBlank(tableName)) {
      false
    } else {
      this.kuduContext.tableExists(this.packageKuduTableName(tableName))
    }
  }

  /**
   * 删除指定表
   *
   * @param tableName
   * 表名
   */
  def deleteTable(tableName: String): Unit = {
    if (this.tableExists(tableName)) {
      this.kuduContext.deleteTable(this.packageKuduTableName(tableName))
    }
  }

  /**
   * 删除指定表
   *
   * @param tableName
   * 表名
   */
  def dropTable(tableName: String): Unit = {
    this.deleteTable(tableName)
  }

  /**
   * 根据给定的主键值集合查询kudu数据，返回DataFrame
   *
   * 表名（与kudu表对应的Spark SQL临时表）
   *
   * @param kv
   * 联合主键键值对，如：("bill_code", "1", "2"), ("ds", "20171010","20171011")
   * @tparam T
   * 主键的类型（Int、Long、String）
   * @return
   * 查询结果，以DataFrame形式返回
   */
  def selectKVList[T: ClassTag](tableName: String, kv: (String, Seq[T])*): DataFrame = {
    require(StringUtils.isNotBlank(tableName) && kv != null && kv.nonEmpty, paramErrorMsg)
    val len = kv.head._2.length
    if (len < 1 || kv.filter(t => t._2.length != len).length != 0) throw new IllegalArgumentException("联合主键值的个数必须一致")

    val tmpTableName = getTmpTableName(tableName)
    val sqlStr = new mutable.StringBuilder("")
    for (i <- 0 until len) {
      val fragment = new mutable.StringBuilder(s"SELECT * FROM $tmpTableName WHERE ")
      kv.foreach(t => {
        val primaryKey = t._1
        if (StringUtils.isBlank(primaryKey)) throw new IllegalArgumentException("主键字段名称不能为空")
        val value = t._2(i)
        if (value.isInstanceOf[Int] || value.isInstanceOf[Long]) {
          fragment.append(s" $primaryKey=$value AND")
        } else {
          fragment.append(s" $primaryKey='$value' AND")
        }
      })
      sqlStr.append(s" ${fragment.substring(0, fragment.length - 3)}\n UNION ALL\n")
    }
    this.sqlContext.sql(
      s"""
         |${sqlStr.substring(0, sqlStr.length - 10)}
      """.stripMargin)
  }

  /**
   * 通过多个JavaBean查询
   *
   * @param tableName
   * 表名
   * @param beans
   * 多个JavaBean
   * @tparam T
   * JavaBean实体类型
   * @return
   * 数据集
   */
  def selectBeans[T: ClassTag](tableName: String, beans: KuduBaseBean*): DataFrame = {
    this.selectKVList(tableName, this.getParams(beans: _*): _*)
  }

  /**
   * 通过多个JavaBean查询
   *
   * @param tableName
   * 表名
   * @param beans
   * 多个JavaBean
   * @tparam T
   * JavaBean实体类型
   * @return
   * 数据集
   */
  def selectList[T: ClassTag](tableName: String, beans: Seq[KuduBaseBean]): DataFrame = {
    this.selectKVList(tableName, this.getParams(beans: _*): _*)
  }

  /**
   * 通过多个JavaBean查询
   *
   * @param tableName
   * 表名
   * @param rdd
   * JavaBean集合
   * @tparam T
   * JavaBean实体类型
   * @return
   * 数据集
   */
  def selectRDD[T: ClassTag](tableName: String, rdd: RDD[T]): DataFrame = {
    this.selectList(tableName, rdd.map(bean => bean.asInstanceOf[KuduBaseBean]).collect())
  }

  /**
   * 加载kudu表转为DataFrame
   *
   * @param tableName
   * @return
   */
  def loadKuduTable(tableName: String): DataFrame = {
    sqlContext.read.options(Map("kudu.master" -> FireKuduConf.kuduMaster, "kudu.table" -> KuduUtils.packageKuduTableName(tableName))).kudu
  }

  /**
   * 通过多个JavaBean查询
   *
   * @param tableName
   * 表名
   * @tparam T
   * JavaBean实体类型
   * @return
   * 数据集
   */
  def selectDataFrame[T: ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[_]): DataFrame = {
    val rdd = dataFrame.rdd.map(row => KuduUtils.kuduRowToBean(row, clazz))
    this.selectRDD(tableName, rdd)
  }

  /**
   * 根据自定义Javabean获取联合主键和值
   *
   * @param beans
   * 自定义JavaBean的多个实体
   * @tparam T
   * 类型
   * @return
   * kv参数
   */
  def getParams[T: ClassTag](beans: KuduBaseBean*): ListBuffer[(String, Seq[T])] = {
    val params = new ListBuffer[(String, Seq[T])]()
    val idField = new ListBuffer[Field]()
    val clazz = beans.head.getClass
    clazz.getDeclaredFields.foreach(field => {
      val anno = field.getAnnotation(classOf[FieldName])
      if (anno != null && anno.id) {
        idField += field
      }
    })
    val map = mutable.Map[String, ListBuffer[T]]()
    beans.foreach(bean => {
      idField.foreach(field => {
        ReflectionUtils.setAccessible(field)
        val fieldName = field.getName
        val seq = map.getOrElse(fieldName, null)
        if (seq == null) {
          val list = ListBuffer[T]()
          list += field.get(bean).asInstanceOf[T]
          map.put(fieldName, list)
        } else {
          seq += field.get(bean).asInstanceOf[T]
          map.put(fieldName, seq)
        }
      })
    })
    map.foreach(t => {
      params += (t._1 -> t._2)
    })
    params
  }

  /**
   * 执行sql语句
   * 注：sql中引用的表必须存在
   *
   * @param sql
   * Spark 支持的sql语句
   * @return
   * 结果集
   */
  def sql(sql: String): DataFrame = {
    sqlContext.sql(sql)
  }

  /**
   * 判断记录是否存在
   *
   * @param tableName
   * 临时表名
   * @param kv
   * 联合主键键值对，如：("bill_code", "1", "2"), ("ds", "20171010","20171011")
   * @tparam T
   * 主键的类型（Int、Long、String）
   * @return
   * 查询结果，以DataFrame形式返回
   */
  def rowExists[T: ClassTag](tableName: String, kv: (String, T)*): Boolean = {
    val idClazz = classTag[T].runtimeClass
    if (idClazz != classOf[Int] && idClazz != classOf[Long] && idClazz != classOf[String]) {
      throw new IllegalArgumentException("主键字段类型必须为Int、Long、String")
    }
    val params = new ListBuffer[(String, Seq[T])]()
    kv.foreach(t => {
      val fieldName = t._1
      params += (fieldName -> Seq(t._2))
    })
    val df = this.selectKVList[T](tableName, params: _*)
    !df.rdd.isEmpty()
  }

  /**
   * 判断记录是否存在
   *
   * @param tableName
   * 表名
   * @param bean
   * JavaBean实体
   * @return
   * 存在、不存在
   */
  def rowExists[T: ClassTag](tableName: String, bean: KuduBaseBean): Boolean = {
    !this.selectBeans(tableName, bean).rdd.isEmpty()
  }

  /**
   * 创建kudu表原始API
   *
   * @param tableName
   * 表名
   * @param schema
   * 表模式
   * @param keys
   * 主键集合
   * @param options
   * 选项
   * @return
   * kudu表对象
   */
  def createTable(tableName: String, schema: StructType, keys: Seq[String], options: CreateTableOptions): KuduTable = {
    this.kuduContext.createTable(this.packageKuduTableName(tableName), schema, keys, options)
  }

  /**
   * 使用实体bean自动生成schema的方式创建kudu表
   *
   * @param tableName
   * 表名
   * @param beanClazz
   * 实体bean的类型
   * @param keys
   * 主键集合
   * @param options
   * 选项
   * @return
   * kudu表对象
   */
  def createTable(tableName: String, beanClazz: Class[_], keys: Seq[String], options: CreateTableOptions): KuduTable = {
    val typeField = SparkUtils.buildSchemaFromBean(beanClazz)
    this.createTable(tableName, StructType(typeField), keys, options)
  }

  /**
   * 根据kudu表名获取临时表
   *
   * @param tableName
   * kudu表名
   * @return
   * Spark临时表名
   */
  private def getTmpTableName(tableName: String): String = {
    val pkTableName = this.packageKuduTableName(tableName)
    var tmpTableName = this.tableMap.getOrElse(pkTableName, "")
    if (StringUtils.isBlank(tmpTableName)) {
      tmpTableName = pkTableName.substring(pkTableName.indexOf(".") + 1, pkTableName.length)
      this.tableMap += (pkTableName -> tmpTableName)
      sqlContext.loadKuduTable(pkTableName).registerTempTable(tmpTableName)
    }
    tmpTableName
  }
}

object KuduContextExt {
  private lazy val impalaDaemons = FireKuduConf.impalaDaemons.split(",").toSet[String]
  private lazy val dataSource: util.LinkedList[Connection] = new util.LinkedList[Connection]()
  private lazy val isInit = new AtomicBoolean(false)

  /**
   * 初始化impala连接池
   */
  private[this] def initPool: Unit = {
    if (isInit.compareAndSet(false, true)) {
      if (ValueUtils.noEmpty(FireKuduConf.impalaJdbcDriverName)) Class.forName(FireKuduConf.impalaJdbcDriverName)

      impalaDaemons.filter(ValueUtils.noEmpty(_)).map(_.trim).foreach(ip => {
        val conn: Connection = DriverManager.getConnection(s"jdbc:hive2://$ip:21050/;auth=noSasl")
        println(s"已成功创建impala连接：$ip")
        this.dataSource.push(conn)
      })
    }
  }

  /**
   * 从数据库连接池中获取一个连接
   *
   * @return
   * impala连接
   */
  def getConnection: Connection = {
    this.synchronized {
      this.initPool
      // 如果当前连接池中没有连接，则一直等待，直到获取到连接
      while (this.dataSource.isEmpty)
        try {
          Thread.sleep(100)
        }
        catch {
          case e: InterruptedException => {
            e.printStackTrace()
          }
        }
      this.dataSource.poll
    }
  }

  /**
   * 回收Connection
   *
   * @param conn
   * 数据库连接
   */
  def closeConnection(conn: Connection): Unit = {
    if (conn != null)
      this.dataSource.push(conn)
  }

  /**
   * 执行kudu 的 sql语句
   *
   * @param sqls
   * 多条sql
   */
  def execute(sqls: String*): Unit = {
    if (ValueUtils.isEmpty(sqls)) return

    var con: Connection = null
    var stmt: Statement = null
    try {
      con = this.getConnection
      stmt = con.createStatement
      sqls.filter(ValueUtils.noEmpty(_)).foreach(sql => {
        stmt.execute(sql)
      })
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      this.closeConnection(con)
    }
  }

  /**
   * 为指定的kudu表添加分区
   *
   * @param tables
   * 多个kudu表名
   * @param start
   * 分区的起始时间（闭）
   * @param end
   * 分区的结束时间（开）
   */
  def addPartition(tables: Seq[String], start: String, end: String): Unit = {
    if (ValueUtils.isEmpty(tables, start, end)) return

    val sqls = tables.filter(ValueUtils.noEmpty(_)).map(table => s"""ALTER TABLE $table ADD IF NOT EXISTS RANGE PARTITION '$start' <= VALUES < '$end'""")
    if (ValueUtils.noEmpty(sqls)) this.execute(sqls: _*)
  }

  /**
   * 根据主键批量删除
   *
   * @param tableName
   * 表名
   * @param ids
   * 主键集合
   * @tparam T
   * 类型
   * @return
   * 影响的记录数
   */
  def deleteByIds[T: ClassTag](tableName: String, ids: Seq[T]): Long = {
    if (StringUtils.isBlank(tableName) || ids == null || ids.length < 1) {
      return 0L
    }
    val sqlStr = new StringBuilder(s"delete from $tableName where id in(")
    val clazz = classTag[T].runtimeClass
    ids.foreach(id => {
      if (classOf[Int] == clazz || classOf[Long] == clazz) {
        sqlStr.append(s"$id,")
      } else {
        sqlStr.append(s"'$id',")
      }
    })
    val sql = sqlStr.substring(0, sqlStr.length - 1) + ")"
    var con: Connection = null
    var stmt: Statement = null
    try {
      con = this.getConnection
      stmt = con.createStatement()
      stmt.executeUpdate(sql).toLong
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (stmt != null) {
        stmt.close()
      }
      this.closeConnection(con)
    }
    0L
  }

  /**
   * 执行删除操作
   *
   * @param sql
   * SQL语句
   * @return
   * 影响的记录数
   */
  def deleteBySQL[T: ClassTag](sql: String): Long = {
    if (StringUtils.isBlank(sql)) {
      return 0L
    }
    var con: Connection = null
    var stmt: Statement = null
    try {
      con = this.getConnection
      stmt = con.createStatement()
      val rs = stmt.executeUpdate(sql)
      return rs.toLong
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (stmt != null) {
        stmt.close()
      }
      this.closeConnection(con)
    }
    0L
  }

  def findBySQL[T](sql: String, clazz: Class[T]): ListBuffer[T] = {
    if (StringUtils.isBlank(sql) || clazz == null) {
      throw new IllegalArgumentException("参数不合法")
    }
    val list = ListBuffer[T]()
    var con: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      con = this.getConnection
      println(con.getCatalog)
      stmt = con.createStatement()
      rs = stmt.executeQuery(sql)
      list ++= DBUtils.dbResultSet2Bean(rs, clazz)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      try {
        if (rs != null) {
          rs.close
        }
        if (stmt != null) {
          stmt.close
        }
        this.closeConnection(con)
      } catch {
        case e1: Exception => e1.printStackTrace()
      }
    }
    list
  }
}