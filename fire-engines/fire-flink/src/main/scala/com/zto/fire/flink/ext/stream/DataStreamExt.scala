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

package com.zto.fire.flink.ext.stream

import java.lang.reflect.Field

import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.flink.sink.{HBaseSink, JdbcSink}
import com.zto.fire.flink.util.FlinkSingletonFactory
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire._
import com.zto.fire.hbase.HBaseConnector
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.accumulators.SimpleAccumulator
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * 用于对Flink DataStream的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class DataStreamExt[T](stream: DataStream[T]) {
  lazy val tableEnv = FlinkSingletonFactory.getTableEnv.asInstanceOf[StreamTableEnvironment]

  /**
   * 将流注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    val table = this.stream.toTable(this.tableEnv)
    this.tableEnv.createTemporaryView(tableName, table)
    table
  }

  /**
   * 为当前DataStream设定uid与name
   *
   * @param uid
   * uid
   * @param name
   * name
   * @return
   * 当前实例
   */
  def uname(uid: String, name: String = ""): DataStream[T] = {
    if (StringUtils.isNotBlank(uid)) stream.uid(uid)
    if (StringUtils.isNotBlank(name)) stream.name(name)
    this.stream
  }

  /**
   * 预先注册flink累加器
   *
   * @param acc
   * 累加器实例
   * @param name
   * 累加器名称
   * @return
   * 注册累加器之后的流
   */
  def registerAcc(acc: SimpleAccumulator[_], name: String): DataStream[String] = {
    this.stream.map(new RichMapFunction[T, String] {
      override def open(parameters: Configuration): Unit = {
        this.getRuntimeContext.addAccumulator(name, acc)
      }

      override def map(value: T): String = value.toString
    })
  }

  /**
   * 将流映射为批流
   *
   * @param count
   * 将指定数量的合并为一个集合
   */
  def countWindowSimple[T: ClassTag](count: Long): DataStream[List[T]] = {
    implicit val typeInfo = TypeInformation.of(classOf[List[T]])
    stream.asInstanceOf[DataStream[T]].countWindowAll(Math.abs(count)).apply(new AllWindowFunction[T, List[T], GlobalWindow]() {
      override def apply(window: GlobalWindow, input: Iterable[T], out: Collector[List[T]]): Unit = {
        out.collect(input.toList)
      }
    })(typeInfo)
  }

  /**
   * 设置并行度
   */
  def repartition(parallelism: Int): DataStream[T] = {
    this.stream.setParallelism(parallelism)
  }

  /**
   * 将DataStream转为Table
   */
  def toTable: Table = {
    this.tableEnv.fromDataStream(this.stream)
  }

  /**
   * jdbc批量sink操作，根据用户指定的DataStream中字段的顺序，依次填充到sql中的占位符所对应的位置
   * 若DataStream为DataStream[Row]类型，则fields可以为空，但此时row中每列的顺序要与sql占位符顺序一致，数量和类型也要一致
   * 注：
   *  1. fieldList指定DataStream中JavaBean的字段名称，非jdbc表中的字段名称
   *  2. fieldList多个字段使用逗号分隔
   *  3. fieldList中的字段顺序要与sql中占位符顺序保持一致，数量一致
   *
   * @param sql
   * 增删改sql
   * @param fields
   * DataStream中数据的每一列的列名（非数据库中的列名，需与sql中占位符的顺序一致）
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def jdbcBatchUpdate(sql: String,
                      fields: Seq[String],
                      batch: Int = 10,
                      flushInterval: Long = 1000,
                      keyNum: Int = 1): DataStreamSink[T] = {
    this.stream.addSink(new JdbcSink[T](sql, batch = batch, flushInterval = flushInterval, keyNum = keyNum) {
      var fieldMap: java.util.Map[String, Field] = _
      var clazz: Class[_] = _

      override def map(value: T): Seq[Any] = {
        requireNonEmpty(sql)("sql语句不能为空")

        val params = ListBuffer[Any]()
        if (value.isInstanceOf[Row] || value.isInstanceOf[Tuple2[Boolean, Row]]) {
          // 如果是Row类型的DataStream[Row]
          val row = if (value.isInstanceOf[Row]) value.asInstanceOf[Row] else value.asInstanceOf[Tuple2[Boolean, Row]]._2
          for (i <- 0 until row.getArity) {
            params += row.getField(i)
          }
        } else {
          requireNonEmpty(fields)("字段列表不能为空！需按照sql中的占位符顺序依次指定当前DataStream中数据字段的名称")

          if (clazz == null && value != null) {
            clazz = value.getClass
            fieldMap = ReflectionUtils.getAllFields(clazz)
          }

          fields.foreach(fieldName => {
            val field = this.fieldMap.get(StringUtils.trim(fieldName))
            requireNonEmpty(field)(s"当前DataStream中不存在该列名$fieldName，请检查！")
            params += field.get(value)
          })
        }
        params
      }
    }).name("fire jdbc stream sink")
  }

  /**
   * jdbc批量sink操作
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  def jdbcBatchUpdate2(sql: String,
                       batch: Int = 10,
                       flushInterval: Long = 1000,
                       keyNum: Int = 1)(fun: T => Seq[Any]): DataStreamSink[T] = {
    this.stream.addSink(new JdbcSink[T](sql, batch = batch, flushInterval = flushInterval, keyNum = keyNum) {
      override def map(value: T): Seq[Any] = {
        fun(value)
      }
    }).name("fire jdbc stream sink")
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def hbasePutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String,
                 batch: Int = 100,
                 flushInterval: Long = 3000,
                 keyNum: Int = 1): DataStreamSink[_] = {
    this.hbasePutDS2[E](tableName, batch, flushInterval, keyNum) {
      value => {
        value.asInstanceOf[E]
      }
    }
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  def hbasePutDS2[E <: HBaseBaseBean[E] : ClassTag](tableName: String,
                  batch: Int = 100,
                  flushInterval: Long = 3000,
                  keyNum: Int = 1)(fun: T => E): DataStreamSink[_] = {
    HBaseConnector.checkClass[E]()
    this.stream.addSink(new HBaseSink[T, E](tableName, batch, flushInterval, keyNum) {
      /**
       * 将数据构建成sink的格式
       */
      override def map(value: T): E = fun(value)
    }).name("fire hbase stream sink")
  }

}
