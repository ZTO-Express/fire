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

package com.zto.fire.spark.util

import java.lang.reflect.Field

import com.zto.fire._
import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.spark.ext.module.KuduContextExt
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}

/**
  * kudu工具类
  *
  * @author ChengLong 2019-6-23 13:32:15
  */
object KuduUtils {

  /**
    * 将kudu的JavaBean转为Row
    * 实体Class类型
    *
    * @return
    * Spark SQL Row对象
    */
  def kuduBean2Row[T: ClassTag](bean: T): Row = {
    val beanClazz = classTag[T].runtimeClass
    val values = ListBuffer[AnyRef]()
    beanClazz.getDeclaredFields.foreach(field => {
      ReflectionUtils.setAccessible(field)
      val anno = field.getAnnotation(classOf[FieldName])
      if (anno != null && anno.id()) {
        values += field.get(bean)
      }
    })
    Row(values: _*)
  }

  /**
    * 将kudu的JavaBean转为Row
    *
    * @param beanClazz
    * 实体Class类型
    * @return
    * Spark SQL Row对象
    */
  def bean2Row(beanClazz: Class[_]): Row = {
    val fieldList = ListBuffer[Field]()
    beanClazz.getDeclaredFields.foreach(field => {
      ReflectionUtils.setAccessible(field)
      val anno = field.getAnnotation(classOf[FieldName])
      val begin = if (anno == null) true else !anno.disuse()
      if (begin) {
        fieldList += field
      }
    })
    Row(fieldList)
  }

  /**
    * 将Row转为自定义bean，以Row中的Field为基准
    * bean中的field名称要与DataFrame中的field名称保持一致
    */
  def kuduRowToBean[T](row: Row, clazz: Class[T]): T = {
    val obj = clazz.newInstance()
    if (row != null && clazz != null) {
      try {
        row.schema.fieldNames.foreach(fieldName => {
          clazz.getDeclaredFields.foreach(field => {
            ReflectionUtils.setAccessible(field)
            if (field.getName.equalsIgnoreCase(fieldName)) {
              val index = row.fieldIndex(fieldName)
              val fieldType = field.getType
              if (fieldType eq classOf[String]) field.set(obj, row.getString(index))
              else if (fieldType eq classOf[java.lang.Integer]) field.set(obj, row.getAs[IntegerType](index))
              else if (fieldType eq classOf[java.lang.Double]) field.set(obj, row.getAs[DoubleType](index))
              else if (fieldType eq classOf[java.lang.Long]) field.set(obj, row.getAs[LongType](index))
              else if (fieldType eq classOf[java.math.BigDecimal]) field.set(obj, row.getAs[DecimalType](index))
              else if (fieldType eq classOf[java.lang.Float]) field.set(obj, row.getAs[FloatType](index))
              else if (fieldType eq classOf[java.lang.Boolean]) field.set(obj, row.getAs[BooleanType](index))
              else if (fieldType eq classOf[java.lang.Short]) field.set(obj, row.getAs[ShortType](index))
              else if (fieldType eq classOf[java.util.Date]) field.set(obj, row.getAs[DateType](index))
            }
          })
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    obj
  }

  /**
    * 根据实体bean构建kudu表schema（只构建主键字段）
    *
    * @return StructField集合
    */
  def buildSchemaFromKuduBean(beanClazz: Class[_]): List[StructField] = {
    val fieldMap = ReflectionUtils.getAllFields(beanClazz)
    val strutFields = new ListBuffer[StructField]()
    for (map <- fieldMap.entrySet) {
      val field: Field = map.getValue
      val fieldType: Class[_] = field.getType
      val anno: FieldName = field.getAnnotation(classOf[FieldName])
      var fieldName: String = map.getKey
      var nullable: Boolean = true
      val begin = if (anno == null) {
        false
      } else {
        if (StringUtils.isNotBlank(anno.value)) {
          fieldName = anno.value
        }
        nullable = anno.nullable()
        !anno.disuse
      }
      if (begin && anno.id) {
        if (fieldType eq classOf[String]) strutFields += DataTypes.createStructField(fieldName, DataTypes.StringType, nullable)
        else if (fieldType eq classOf[java.lang.Integer]) strutFields += DataTypes.createStructField(fieldName, DataTypes.IntegerType, nullable)
        else if (fieldType eq classOf[java.lang.Double]) strutFields += DataTypes.createStructField(fieldName, DataTypes.DoubleType, nullable)
        else if (fieldType eq classOf[java.lang.Long]) strutFields += DataTypes.createStructField(fieldName, DataTypes.LongType, nullable)
        else if (fieldType eq classOf[java.math.BigDecimal]) strutFields += DataTypes.createStructField(fieldName, DataTypes.DoubleType, nullable)
        else if (fieldType eq classOf[java.lang.Float]) strutFields += DataTypes.createStructField(fieldName, DataTypes.FloatType, nullable)
        else if (fieldType eq classOf[java.lang.Boolean]) strutFields += DataTypes.createStructField(fieldName, DataTypes.BooleanType, nullable)
        else if (fieldType eq classOf[java.lang.Short]) strutFields += DataTypes.createStructField(fieldName, DataTypes.ShortType, nullable)
        else if (fieldType eq classOf[java.util.Date]) strutFields += DataTypes.createStructField(fieldName, DataTypes.DateType, nullable)
      }
    }
    strutFields.toList
  }

  /**
    * 将表名包装为以impala::开头的表
    *
    * @param tableName
    * 库名.表名
    * @return
    * 包装后的表名
    */
  def packageKuduTableName(tableName: String): String = {
    if (StringUtils.isBlank(tableName)) throw new IllegalArgumentException("表名不能为空")
    if (tableName.startsWith("impala::")) {
      tableName
    } else {
      s"impala::$tableName"
    }
  }

  /**
   * 以Map的方式获取Hive表的字段名称和类型
   *
   * @param tableName
   *                  db.hiveTable
   * @return
   * Map[FieldName, FieldType]
   */
  def getTableSchemaAsMap(hiveContext: HiveContext, kuduContext: KuduContextExt, tableName: String): Map[String, String] = {
    val dataFrame = if (tableName.startsWith("impala")) {
      kuduContext.loadKuduTable(tableName)
    } else {
      hiveContext.table(tableName)
    }

    dataFrame.schema.map(s => {
      (s.name, s.dataType.simpleString)
    }).toMap
  }
}
