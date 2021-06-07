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

package com.zto.fire.spark.udf

import java.util.Date

import com.zto.fire.common.util.{DateFormatUtils, NumberFormatUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
 * 通用的自定义UDF工具函数集合
 * Created by ChengLong on 2017-01-06.
 */
object UDFs extends Serializable {

  /**
   * 批量注册系统内置的udf函数
   */
  def registerSysUDF(spark: SparkSession): Unit = {
    // ==================== 日期相关 ====================
    spark.udf.register("addTimer", Timer.addTimer _)
    spark.udf.register("addYears", Timer.addYears _)
    spark.udf.register("addMons", Timer.addMons _)
    spark.udf.register("addDays", Timer.addDays _)
    spark.udf.register("addHours", Timer.addHours _)
    spark.udf.register("addMins", Timer.addMins _)
    spark.udf.register("addSecs", Timer.addSecs _)
    spark.udf.register("dateSchemaFormat", Timer.dateSchemaFormat _)
    spark.udf.register("dateStrSchemaFormat", Timer.dateStrSchemaFormat _)
    spark.udf.register("isSameDay", Timer.isSameDay _)
    spark.udf.register("isBig", Timer.isBig _)
    spark.udf.register("isSmall", Timer.isSmall _)
    spark.udf.register("isBetween", Timer.isBetween _)
    spark.udf.register("date", Timer.date _)
    spark.udf.register("interval", Timer.interval _)
    spark.udf.register("runTime", Timer.runTime _)
    spark.udf.register("truncateMinute", Timer.truncateMinute _)
    spark.udf.register("truncateHour", Timer.truncateHour _)

    // ==================== 字符串相关 ====================
    spark.udf.register("isNull", Str.isNull _)
    spark.udf.register("isNotNull", Str.isNotNull _)
    spark.udf.register("len", Str.len _)
    spark.udf.register("reverse", Str.reverse _)
    spark.udf.register("contains", Str.contains _)

    // ==================== 数字相关 ====================
    spark.udf.register("floor", Num.floor _)
    spark.udf.register("long2Int", Num.long2Int _)
    spark.udf.register("bigDecimal2Long", Num.bigDecimal2Long _)
    spark.udf.register("ifnull", Num.ifnull _)
    spark.udf.register("truncate", Num.truncate _)
    spark.udf.register("truncate_decimal", Num.truncateDecimal _)
  }

  /**
   * 时间相关的udf函数
   * 时间戳格式为：yyyy-MM-dd hh:mm:ss
   */
  object Timer {

    /**
     * 指定时间字段，对日期进行加减
     *
     * @param field
     * 'year'、'month'、'day'、'hour'、'minute'、'second'
     * @param dateTimeStr
     * 格式：yyyy-MM-dd hh:mm:ss
     * @param count
     * 正负数
     * @return
     * 计算后的日期
     */
    def addTimer(field: String, dateTimeStr: String, count: Int): String = {
      DateFormatUtils.addTimer(field, dateTimeStr, count)
    }

    /**
     * 对指定的时间字段进行年度加减
     */
    def addYears(dateTimeStr: String, years: Int): String = {
      DateFormatUtils.addYears(dateTimeStr, years)
    }

    /**
     * 对指定的时间字段进行月份加减
     */
    def addMons(dateTimeStr: String, mons: Int): String = {
      DateFormatUtils.addMons(dateTimeStr, mons)
    }

    /**
     * 对指定的时间字段进行天加减
     */
    def addDays(dateTimeStr: String, days: Int): String = {
      DateFormatUtils.addDays(dateTimeStr, days)
    }

    /**
     * 对指定的时间字段进行天加减
     */
    def addWeeks(dateTimeStr: String, weeks: Int): String = {
      DateFormatUtils.addWeeks(dateTimeStr, weeks)
    }

    /**
     * 对指定的时间字段进行小时加减
     */
    def addHours(dateTimeStr: String, hours: Int): String = {
      DateFormatUtils.addHours(dateTimeStr, hours)
    }

    /**
     * 对指定的时间字段进行分钟加减
     */
    def addMins(dateTimeStr: String, minutes: Int): String = {
      DateFormatUtils.addMins(dateTimeStr, minutes)
    }

    /**
     * 对指定的时间字段进行秒钟加减
     */
    def addSecs(dateTimeStr: String, seconds: Int): String = {
      DateFormatUtils.addSecs(dateTimeStr, seconds)
    }

    /**
     * 对字段进行格式转换
     */
    def dateStrSchemaFormat(dateTimeStr: String, srcSchema: String, destSchema: String): String = {
      if (StringUtils.isBlank(dateTimeStr)) "" else DateFormatUtils.dateSchemaFormat(dateTimeStr, srcSchema, destSchema)
    }

    /**
     * 获取两个时间间隔的毫秒数
     *
     * @param before
     * 开始时间（小）
     * @param after
     * 结束时间（大）
     * @return
     */
    def interval(before: String, after: String): Long = {
      DateFormatUtils.interval(before, after)
    }

    /**
     * 计算运行时长
     *
     * @param time
     * 形如：3日11时21分15秒
     */
    def runTime(time: Long): String = {
      DateFormatUtils.runTime(time)
    }

    /**
     * 判断两个字段是否为同一天
     */
    def isSameDay(day1: String, day2: String): Boolean = {
      DateFormatUtils.isSameDay(day1, day2)
    }

    /**
     * day1是否大于day2
     */
    def isBig(day1: String, day2: String): Boolean = {
      DateFormatUtils.isBig(day1, day2)
    }

    /**
     * day1是否小于day2
     */
    def isSmall(day1: String, day2: String): Boolean = {
      DateFormatUtils.isSmall(day1, day2)
    }

    /**
     * 指定字段日期是否介于day1与day2之间
     */
    def isBetween(day: String, day1: String, day2: String) = {
      DateFormatUtils.isBetween(day, day1, day2)
    }

    /**
     * 截取到年月日
     */
    def date(dateTime: String): String = {
      if (StringUtils.isNotBlank(dateTime) && dateTime.length > 10) dateTime.substring(0, 10) else dateTime
    }

    /**
     * 对字段进行格式转换
     */
    def dateSchemaFormat(dateTime: Date, srcSchema: String, destSchema: String): String = {
      this.dateStrSchemaFormat(DateFormatUtils.formatDateTime(dateTime), srcSchema, destSchema)
    }

    /**
     * 将yyyy-MM-dd hh:mm:ss类型日期truncate为分钟
     */
    def truncateMinute(dateTime: String): String = {
      DateFormatUtils.truncateMinute(dateTime)
    }

    /**
     * 获取整点小时
     */
    def truncateHour(dateStr: String): String = {
      DateFormatUtils.truncateHour(dateStr)
    }
  }

  /**
   * 对字段进行字符串相关操作
   */
  object Str {

    /**
     * 如果字段为空，则返回true，否则返回false
     */
    def isNull(field: String): Boolean = {
      if (StringUtils.isBlank(field) || field.trim.length() == 0 || "null".equalsIgnoreCase(field.trim) || """\N""".equalsIgnoreCase(field.trim)) {
        true
      } else {
        false
      }
    }

    /**
     * 如果字段为空，则返回false，否则返回true
     */
    def isNotNull(field: String): Boolean = {
      !isNull(field)
    }

    /**
     * 计算长度
     */
    def len(field: String): Int = {
      if (this.isNull(field)) 0 else field.length
    }

    /**
     * 字符串反转
     */
    def reverse(str: String): String = {
      StringUtils.reverse(str)
    }

    /**
     * 是否包含
     *
     * @param field
     * 字段名称
     * @param str
     * 包含的字符串
     * @return
     */
    def contains(field: String, str: String): Boolean = {
      if (StringUtils.isBlank(field) || StringUtils.isBlank(str)) {
        false
      } else {
        field.contains(str)
      }
    }
  }

  /**
   * 数值相关
   */
  object Num {

    /**
     * floor操作
     */
    def floor(field: Double): Int = {
      NumberFormatUtils.floor(field)
    }

    /**
     * 将Long转为Integer
     */
    def long2Int(field: java.lang.Long): java.lang.Integer = {
      NumberFormatUtils.long2Int(field)
    }

    /**
     * 将BigDecimal转为Long类型
     */
    def bigDecimal2Long(field: java.math.BigDecimal): java.lang.Long = {
      NumberFormatUtils.bigDecimal2Long(field)
    }

    /**
     * 判断是否为空
     */
    def ifnull(decimal: java.math.BigDecimal, defaultVal: java.math.BigDecimal): java.math.BigDecimal = {
      NumberFormatUtils.ifnull(decimal, defaultVal)
    }

    /**
     * 类似于round，但不会四舍五入
     *
     * @param value
     * 目标值
     * @param scale
     * 精度
     * @return
     */
    def truncate(value: Double, scale: Int): Double = {
      NumberFormatUtils.truncate(value, scale)
    }

    /**
     * 截取精度
     *
     * @param scale
     * 精度
     * @return
     */
    def truncateDecimal(bigDecimal: java.math.BigDecimal, scale: Int): java.math.BigDecimal = {
      NumberFormatUtils.truncateDecimal(bigDecimal, scale)
    }
  }

}
