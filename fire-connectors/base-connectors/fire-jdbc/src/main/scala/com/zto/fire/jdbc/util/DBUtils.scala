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

package com.zto.fire.jdbc.util

import com.google.common.collect.Maps
import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.{Logging, ReflectionUtils}
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

import java.sql.{ResultSet, Types}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Try

/**
 * 关系型数据库操作工具类
 *
 * @author ChengLong 2019-6-23 11:16:18
 */
object DBUtils extends Logging {
  private lazy val driverFile = "driver.properties"
  // 读取配置文件，获取jdbc url与driver的映射关系
  private lazy val driverMap = {
    tryWithReturn {
      val properties = new Properties()
      properties.load(this.getClass.getClassLoader.getResourceAsStream(this.driverFile))
      Maps.fromProperties(properties)
    } (this.logger, s"加载${this.driverFile}成功", s"加载${this.driverFile}失败，请确认该配置文件是否存在！")
  }

  /**
   * 将ResultSet结果转为JavaBean集合
   *
   * @param rs    数据库中的查询结果集
   * @param clazz 目标JavaBean类型
   * @return 将ResultSet转换为JavaBean集合返回
   */
  def resultSet2BeanList[T](rs: ResultSet, clazz: Class[T]): ListBuffer[T] = {
    val list = ListBuffer[T]()
    val fields = clazz.getDeclaredFields
    try {
      val columnMap = this.columns(rs)
      while (rs.next()) {
        val obj = clazz.newInstance()
        fields.foreach(field => {
          ReflectionUtils.setAccessible(field)
          val anno = field.getAnnotation(classOf[FieldName])
          if (!(anno != null && anno.disuse())) {
            val fieldName = if (anno != null && StringUtils.isNotBlank(anno.value())) anno.value() else field.getName
            if (columnMap.containsKey(fieldName)) {
              val fieldType = columnMap.get(fieldName)
              fieldType match {
                case Types.INTEGER | Types.SMALLINT=> field.set(obj, rs.getInt(fieldName))
                case Types.VARCHAR | Types.CHAR | Types.LONGVARCHAR => field.set(obj, rs.getString(fieldName))
                case Types.BIGINT => field.set(obj, rs.getLong(fieldName))
                case Types.FLOAT => field.set(obj, rs.getFloat(fieldName))
                case Types.DOUBLE => field.set(obj, rs.getDouble(fieldName))
                case Types.DECIMAL => field.set(obj, rs.getBigDecimal(fieldName))
                case Types.BOOLEAN | Types.TINYINT | Types.BIT => field.set(obj, rs.getBoolean(fieldName))
                case Types.DATE => field.set(obj, rs.getDate(fieldName))
                case Types.TIME => field.set(obj, rs.getTime(fieldName))
                case Types.TIMESTAMP => field.set(obj, rs.getTimestamp(fieldName))
                case _ => logger.error(s"ResultSet转换成JavaBean过程中遇到不支持的类型，字段名称：${fieldName}，字段类型：${fieldType}")
              }
            }
          }
        })
        list += obj
      }
    } catch {
      case e: Exception =>
        logger.error("ResultSet转换成JavaBean过程中出现异常.", e)
        throw e
    }
    list
  }

  /**
   * 判断指定的结果集中是否包含指定的列名
   *
   * @param rs
   * 关系型数据库查询结果集
   * @param columnName
   * 列名
   * @return
   * true: 存在 false：不存在
   */
  def containsColumn(rs: ResultSet, columnName: String): Boolean = {
    val start = currentTime
    val retVal = Try {
      try {
        rs.findColumn(columnName)
      }
    }
    if (retVal.isFailure) this.logger.warn(s"ResultSet结果集中未找到列名：${columnName}，请保证ResultSet与JavaBean中的字段一一对应，耗时：${elapsed(start)}")
    rs.getMetaData
    retVal.isSuccess
  }

  /**
   * 根据查询结果集获取字段名称与类型的映射关系
   * @param rs
   * jdbc query结果集
   * @return
   * Map[FieldName, FieldType]
   */
  def columns(rs: ResultSet): JHashMap[String, Int] = {
    val metaData = rs.getMetaData
    val fieldMap = new JHashMap[String, Int]()
    for (i <- 1 until metaData.getColumnCount) {
      val fieldName = metaData.getColumnName(i)
      val fieldType = metaData.getColumnType(i)
      fieldMap.put(fieldName, fieldType)
    }
    fieldMap
  }

  /**
   * 获取ResultSet返回的记录数
   *
   * @param rs
   * 查询结果集
   * @return
   * 结果集行数
   */
  def rowCount(rs: ResultSet): Int = {
    if (rs == null) return 0
    rs.last()
    val count = rs.getRow
    rs.beforeFirst()
    count
  }

  /**
   * 获取jdbc连接信息，若调用者指定，以调用者为准，否则读取配置文件
   *
   * @param jdbcProps
   * 调用者传入的jdbc配置信息
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * jdbc配置信息
   */
  def getJdbcProps(jdbcProps: Properties = null, keyNum: Int = 1): Properties = {
    if (jdbcProps == null || jdbcProps.size() == 0) {
      val defaultProps = new Properties()
      defaultProps.setProperty("user", FireJdbcConf.user(keyNum))
      defaultProps.setProperty("password", FireJdbcConf.password(keyNum))
      defaultProps.setProperty("driver", FireJdbcConf.driverClass(keyNum))
      defaultProps.setProperty("batchsize", FireJdbcConf.batchSize(keyNum).toString)
      defaultProps.setProperty("isolationLevel", FireJdbcConf.isolationLevel(keyNum).toUpperCase)
      defaultProps
    } else {
      jdbcProps
    }
  }

  /**
   * 根据jdbc驱动包名或数据库url区分连接的不同的数据库厂商标识
   */
  def dbTypeParser(driverClass: String, url: String): String = {
    var dbType = "unknown"
    Datasource.values().map(_.toString).foreach(datasource => {
      if (driverClass.toUpperCase.contains(datasource)) dbType = datasource
    })

    // 尝试从url中的端口号解析，对结果进行校正，因为有些数据库使用的是mysql驱动，可以通过url中的端口号区分
    if (StringUtils.isNotBlank(url)) {
      FireFrameworkConf.lineageDatasourceMap.foreach(kv => {
        if (url.contains(kv._2)) dbType = kv._1.toUpperCase
      })
    }
    dbType
  }

  /**
   * 通过解析jdbc url，返回url对应的已知的driver class
   *
   * @param url
   * jdbc url
   * @return
   * driver class
   */
  def parseDriverByUrl(url: String): String = {
    var driver = ""
    // 尝试从url中的端口号解析，对结果进行校正，因为有些数据库使用的是mysql驱动，可以通过url中的端口号区分
    if (StringUtils.isNotBlank(url)) {
      this.driverMap.foreach(kv => {
        if (url.toLowerCase.contains(kv._1)) driver = kv._2
      })
    }
    driver
  }

}
