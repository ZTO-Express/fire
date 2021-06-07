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

import java.sql.ResultSet
import java.util.{Date, Properties}

import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.jdbc.conf.FireJdbcConf
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * 关系型数据库操作工具类
 *
 * @author ChengLong 2019-6-23 11:16:18
 */
object DBUtils {

  /**
   * 将row结果转为javabean
   *
   * @param row 数据库中的一条记录
   * @param clazz
   * @tparam T
   * @return
   */
  def dbRow2Bean[T](row: ResultSet, clazz: Class[T]): T = {
    val obj = clazz.newInstance()
    clazz.getDeclaredFields.foreach(field => {
      ReflectionUtils.setAccessible(field)
      val fieldType = field.getType
      val anno = field.getAnnotation(classOf[FieldName])
      val fieldName = if (anno != null && StringUtils.isNotBlank(anno.value())) anno.value() else field.getName
      if (this.containsColumn(row, fieldName)) {
        if (fieldType eq classOf[String]) field.set(obj, row.getString(fieldName))
        else if (fieldType eq classOf[java.lang.Integer]) field.set(obj, row.getInt(fieldName))
        else if (fieldType eq classOf[java.lang.Double]) field.set(obj, row.getDouble(fieldName))
        else if (fieldType eq classOf[java.lang.Long]) field.set(obj, row.getLong(fieldName))
        else if (fieldType eq classOf[java.math.BigDecimal]) field.set(obj, row.getBigDecimal(fieldName))
        else if (fieldType eq classOf[java.lang.Float]) field.set(obj, row.getFloat(fieldName))
        else if (fieldType eq classOf[java.lang.Boolean]) field.set(obj, row.getBoolean(fieldName))
        else if (fieldType eq classOf[java.lang.Short]) field.set(obj, row.getShort(fieldName))
        else if (fieldType eq classOf[java.util.Date]) field.set(obj, row.getDate(fieldName))
      }
    })
    obj
  }

  /**
   * 将ResultSet结果转为javabean
   *
   * @param rs 数据库中的查询结果集
   * @param clazz
   * @tparam T
   * @return
   */
  def dbResultSet2Bean[T](rs: ResultSet, clazz: Class[T]): ListBuffer[T] = {
    val list = ListBuffer[T]()
    val fields = clazz.getDeclaredFields
    try {
      while (rs.next()) {
        var obj = clazz.newInstance()
        fields.foreach(field => {
          ReflectionUtils.setAccessible(field)
          val fieldType = field.getType
          val anno = field.getAnnotation(classOf[FieldName])
          if (!(anno != null && anno.disuse())) {
            val fieldName = if (anno != null && StringUtils.isNotBlank(anno.value())) anno.value() else field.getName
            if (this.containsColumn(rs, fieldName)) {
              if (fieldType eq classOf[String]) field.set(obj, rs.getString(fieldName))
              else if (fieldType eq classOf[java.lang.Integer]) field.set(obj, rs.getInt(fieldName))
              else if (fieldType eq classOf[java.lang.Double]) field.set(obj, rs.getDouble(fieldName))
              else if (fieldType eq classOf[java.lang.Long]) field.set(obj, rs.getLong(fieldName))
              else if (fieldType eq classOf[java.math.BigDecimal]) field.set(obj, rs.getBigDecimal(fieldName))
              else if (fieldType eq classOf[java.lang.Float]) field.set(obj, rs.getFloat(fieldName))
              else if (fieldType eq classOf[java.lang.Boolean]) field.set(obj, rs.getBoolean(fieldName))
              else if (fieldType eq classOf[java.lang.Short]) field.set(obj, rs.getShort(fieldName))
              else if (fieldType eq classOf[Date]) field.set(obj, rs.getDate(fieldName))
            }
          }
        })
        list += obj
      }
    } catch {
      case e: Exception => e.printStackTrace()
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
    Try {
      try {
        rs.findColumn(columnName)
      }
    }.isSuccess
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
      FireFrameworkConf.buriedPointDatasourceMap.foreach(kv => {
        if (url.contains(kv._2)) dbType = kv._1.toUpperCase
      })
    }
    dbType
  }

}
