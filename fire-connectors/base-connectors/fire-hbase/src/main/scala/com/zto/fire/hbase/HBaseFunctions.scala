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

package com.zto.fire.hbase

import java.nio.charset.StandardCharsets

import com.zto.fire.predef._
import com.zto.fire.common.anno.Internal
import com.zto.fire.hbase.bean.HBaseBaseBean
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, Get, Put, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter.{Filter, FilterList}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * HBase API库
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 15:44
 */
private[hbase] trait HBaseFunctions {

  /**
   * 构建Get对象
   *
   * @param rowKey    rowKey
   * @param family    列族名称
   * @param qualifier 表的qualifier名称
   */
  def buildGet(rowKey: String,
               family: String = null,
               qualifier: String = "",
               maxVersions: Int = 1,
               filter: Filter = null): Get = {
    require(StringUtils.isNotBlank(rowKey), "buildGet执行失败，rowKey不能为空！")
    val get = new Get(rowKey.getBytes(StandardCharsets.UTF_8))
    if (StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier)) {
      get.addColumn(family.getBytes(StandardCharsets.UTF_8), qualifier.getBytes(StandardCharsets.UTF_8))
    } else if (StringUtils.isNotBlank(family)) {
      get.addFamily(family.getBytes(StandardCharsets.UTF_8))
    }
    if (filter != null) get.setFilter(filter)
    if (maxVersions > 0) get.setMaxVersions(maxVersions)
    get
  }

  /**
   * 构建Scan对象
   *
   * @param startRow   指定起始rowkey
   * @param endRow     指定结束rowkey
   * @param filterList 过滤器
   * @return scan实例
   */
  def buildScan(startRow: String, endRow: String,
                family: String = null,
                qualifier: String = "",
                maxVersions: Int = 1,
                filterList: FilterList = null,
                batch: Int = -1): Scan = {
    val scan = new Scan
    if (StringUtils.isNotBlank(startRow)) scan.setStartRow(startRow.getBytes(StandardCharsets.UTF_8))
    if (StringUtils.isNotBlank(endRow)) scan.setStopRow(endRow.getBytes(StandardCharsets.UTF_8))
    if (StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier)) {
      scan.addColumn(family.getBytes(StandardCharsets.UTF_8), qualifier.getBytes(StandardCharsets.UTF_8))
    } else if (StringUtils.isNotBlank(family)) {
      scan.addFamily(family.getBytes(StandardCharsets.UTF_8))
    }
    if (filterList != null) scan.setFilter(filterList)
    if (maxVersions > 0) scan.setMaxVersions(maxVersions)
    if (batch > 0) scan.setBatch(batch)
    scan
  }

  /**
   * 批量插入多行多列，自动将HBaseBaseBean子类转为Put集合
   *
   * @param tableName 表名
   * @param beans     HBaseBaseBean子类集合
   */
  def insert[T <: HBaseBaseBean[T] : ClassTag](tableName: String, beans: Seq[T], keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).insert[T](tableName, beans: _*)
  }

  /**
   * 批量插入多行多列
   *
   * @param tableName 表名
   * @param puts      Put集合
   */
  def insert(tableName: String, puts: Seq[Put], keyNum: Int): Unit = {
    HBaseConnector(keyNum = keyNum).insert(tableName, puts: _*)
  }

  /**
   * 从HBase批量Get数据，并将结果封装到JavaBean中
   *
   * @param tableName 表名
   * @param rowKeys   指定的多个rowKey
   * @param clazz     目标类类型，必须是HBaseBaseBean的子类
   * @return 目标对象实例
   */
  def get[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], rowKeys: Seq[String], keyNum: Int = 1): ListBuffer[T] = {
    HBaseConnector(keyNum = keyNum).get[T](tableName, clazz, rowKeys: _*)
  }

  /**
   * 从HBase批量Get数据，并将结果封装到JavaBean中
   *
   * @param tableName 表名
   * @param clazz     目标类类型，必须是HBaseBaseBean的子类
   * @param gets      指定的多个get对象
   * @return 目标对象实例
   */
  def get[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], gets: ListBuffer[Get], keyNum: Int): ListBuffer[T] = {
    HBaseConnector(keyNum = keyNum).get[T](tableName, clazz, gets: _*)
  }

  /**
   * 通过HBase Seq[Get]获取多条数据
   *
   * @param tableName 表名
   * @param getList   HBase的get对象实例
   * @return
   * HBase Result
   */
  def getResult(tableName: String, getList: Seq[Get], keyNum: Int): ListBuffer[Result] = {
    HBaseConnector(keyNum = keyNum).getResult(tableName, getList: _*)
  }

  /**
   * 通过HBase Get对象获取一条数据
   *
   * @param tableName 表名
   * @return
   * HBase Result
   */
  def getResult[T: ClassTag](tableName: String, rowKeyList: Seq[String], keyNum: Int = 1): ListBuffer[Result] = {
    HBaseConnector(keyNum = keyNum).getResult[T](tableName, rowKeyList: _*)
  }

  /**
   * 表扫描，将scan后得到的ResultScanner对象直接返回
   * 注：调用者需手动关闭ResultScanner对象实例
   *
   * @param tableName 表名
   * @param scan      HBase scan对象
   * @return 指定类型的List
   */
  def scanResultScanner(tableName: String, scan: Scan, keyNum: Int): ResultScanner = {
    HBaseConnector(keyNum = keyNum).scanResultScanner(tableName, scan)
  }

  /**
   * 表扫描，将scan后得到的ResultScanner对象直接返回
   * 注：调用者需手动关闭ResultScanner对象实例
   *
   * @param tableName 表名
   * @param startRow  开始行
   * @param endRow    结束行
   * @return 指定类型的List
   */
  def scanResultScanner(tableName: String, startRow: String, endRow: String, keyNum: Int = 1): ResultScanner = {
    HBaseConnector(keyNum = keyNum).scanResultScanner(tableName, startRow, endRow)
  }

  /**
   * 表扫描，将查询后的数据转为JavaBean并放到List中
   *
   * @param tableName 表名
   * @param startRow  开始行
   * @param endRow    结束行
   * @param clazz     类型
   * @return 指定类型的List
   */
  def scan[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, endRow: String, keyNum: Int = 1): ListBuffer[T] = {
    HBaseConnector(keyNum = keyNum).scan[T](tableName, clazz, startRow, endRow)
  }

  /**
   * 表扫描，将查询后的数据转为JavaBean并放到List中
   *
   * @param tableName 表名
   * @param scan      HBase scan对象
   * @param clazz     类型
   * @return 指定类型的List
   */
  def scan[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int): ListBuffer[T] = {
    HBaseConnector(keyNum = keyNum).scan[T](tableName, clazz, scan)
  }

  /**
   * 根据keyNum获取指定HBase集群的connection
   */
  def getConnection(keyNum: Int = 1): Connection = HBaseConnector(keyNum = keyNum).getConnection

  /**
   * 创建HBase表
   *
   * @param tableName
   * 表名
   * @param families
   * 列族
   */
  private[fire] def createTable(tableName: String, families: Seq[String], keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).createTable(tableName, families: _*)
  }

  /**
   * 删除指定的HBase表
   *
   * @param tableName 表名
   */
  private[fire] def dropTable(tableName: String, keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).dropTable(tableName)
  }

  /**
   * 启用指定的HBase表
   *
   * @param tableName 表名
   */
  private[fire] def enableTable(tableName: String, keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).enableTable(tableName)
  }

  /**
   * disable指定的HBase表
   *
   * @param tableName 表名
   */
  private[fire] def disableTable(tableName: String, keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).disableTable(tableName)
  }

  /**
   * 清空指定的HBase表
   *
   * @param tableName
   *                       表名
   * @param preserveSplits 是否保留所有的split信息
   */
  private[fire] def truncateTable(tableName: String, preserveSplits: Boolean = true, keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).truncateTable(tableName, preserveSplits)
  }

  /**
   * 用于判断HBase表是否存在
   */
  def tableExists(tableName: String, keyNum: Int = 1): Boolean = {
    HBaseConnector(keyNum = keyNum).tableExists(tableName)
  }

  /**
   * 用于判断HBase表是否存在（走缓存）
   */
  def isExists(tableName: String, keyNum: Int = 1): Boolean = {
    HBaseConnector(keyNum = keyNum).isExists(tableName)
  }

  /**
   * 根据多个rowKey删除对应的整行记录
   *
   * @param tableName 表名
   * @param rowKeys   待删除的rowKey集合
   */
  def deleteRows(tableName: String, rowKeys: Seq[String], keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).deleteRows(tableName, rowKeys: _*)
  }

  /**
   * 批量删除指定RowKey的多个列族
   *
   * @param tableName 表名
   * @param rowKey    rowKey
   * @param families  多个列族
   */
  @Internal
  private[fire] def deleteFamilies(tableName: String, rowKey: String, families: Seq[String], keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).deleteFamilies(tableName, rowKey, families: _*)
  }

  /**
   * 批量删除指定列族下的多个字段
   *
   * @param tableName  表名
   * @param rowKey     rowKey字段
   * @param family     列族
   * @param qualifiers 列名
   */
  @Internal
  private[fire] def deleteQualifiers(tableName: String, rowKey: String, family: String, qualifiers: Seq[String], keyNum: Int = 1): Unit = {
    HBaseConnector(keyNum = keyNum).deleteQualifiers(tableName, rowKey, family, qualifiers: _*)
  }

  /**
   * 获取Configuration实例
   *
   * @return HBase Configuration对象
   */
  def getConfiguration(keyNum: Int = 1): Configuration = HBaseConnector(keyNum = keyNum).getConfiguration

  /**
   * 校验类型合法性，class必须是HBaseBaseBean的子类
   */
  def checkClass[T: ClassTag](clazz: Class[_] = null): Unit = {
    val finalClazz = if (clazz != null) clazz else getParamType[T]
    if (finalClazz == null || finalClazz.getSuperclass != classOf[HBaseBaseBean[_]]) throw new IllegalArgumentException("请指定泛型类型，该泛型必须是HBaseBaseBean的子类，如：this.fire.hbasePutTable[JavaBean]")
  }
}
