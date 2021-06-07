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

package com.zto.fire.common.util

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}
import scala.collection.mutable.ArrayBuffer

/**
  * 日期格式化工具类
  * Created by ChengLong on 2016-11-24.
  */
object DateFormatUtils {
  lazy val yyyyMMdd = "yyyyMMdd"
  lazy val yyyy_MM_dd = "yyyy-MM-dd"
  lazy val yyyyMMddHH = "yyyyMMddHH"
  lazy val yyyy_MM_ddHHmmss = "yyyy-MM-dd HH:mm:ss"
  lazy val TRUNCATE_MIN = "yyyy-MM-dd HH:mm:00"
  private val timeZoneShangHai = "Asia/Shanghai"
  private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  lazy val HOUR = "hour"
  lazy val DAY = "day"
  lazy val WEEK = "week"
  lazy val MONTH = "month"
  lazy val YEAR = "year"
  lazy val MINUTE = "minute"
  lazy val SECOND = "second"
  lazy val enumSet = Set(HOUR, DAY, WEEK, MONTH, YEAR, MINUTE, SECOND)

  /**
    * 将日期格式化为 yyyy-MM-dd HH:mm:ss
    */
  def getTimeFormat(): SimpleDateFormat = {
    val timeFormat: SimpleDateFormat = new SimpleDateFormat(DateFormatUtils.yyyy_MM_ddHHmmss)
    timeFormat.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
    timeFormat
  }

  /**
    * 给定yyyy-MM-dd HH:mm:ss 格式数据，返回yyyy-MM-dd
    */
  def getDateFromDateTimeStr(dateTime: String) = {
    if (StringUtils.isNotBlank(dateTime) && dateTime.length() > 10) {
      dateTime.substring(0, 10)
    } else {
      dateTime
    }
  }

  /**
    * 给定yyyy-MM-dd HH:mm:ss 格式数据，返回yyyyMMdd格式的时间分区
    */
  def getPartitionDate(dateTime: String): String = {
    this.getDateFromDateTimeStr(dateTime).replace("-", "")
  }

  /**
    * 将日期格式化为 yyyy-MM-dd
    */
  def getDateFormat(): SimpleDateFormat = {
    this.getSchemaFormat()
  }

  /**
    * 将日期格式化为 yyyy-MM-dd
    */
  def getSchemaFormat(schema: String = DateFormatUtils.yyyy_MM_dd): SimpleDateFormat = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(schema)
    dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
    dateFormat
  }

  /**
    * 格式化Date为yyyy-MM-dd格式的字符串
    */
  def formatDate(date: Date): String = {
    this.getDateFormat().format(date)
  }

  /**
    * 将日期格式化为 yyyy-MM-dd hh:mm:ss 格式的字符串
    */
  def formatDateTime(dateTime: Date): String = {
    if (dateTime != null) this.getTimeFormat().format(dateTime) else ""
  }

  /**
    * 将指定时间转为指定schema的格式
    *
    * @param dateTime
    * 指定时间
    * @return
    */
  def formatBySchema(dateTime: Date, schema: String): String = {
    if (dateTime != null) this.getSchemaFormat(schema).format(dateTime) else ""
  }


  /**
    * 将字符串格式化为yyyy-MM-dd的日期
    */
  def formatDate(date: String): Date = {
    this.getDateFormat().parse(date)
  }

  /**
    * 将字符串格式化为yyyy-MM-dd hh:mm:ss的日期
    */
  def formatDateTime(dateTime: String): Date = {
    this.getTimeFormat().parse(dateTime)
  }

  /**
    * 将当期系统时间格式化为yyyy-MM-dd 并返回字符串
    */
  def formatCurrentDate(): String = {
    this.formatDate(new Date)
  }

  /**
    * 将当期系统时间格式化为yyyy-MM-dd hh:mm:ss并返回字符串
    */
  def formatCurrentDateTime(): String = {
    this.formatDateTime(new Date)
  }

  /**
    * 转换当前时间为指定的时间格式
    *
    * @param schema
    * 指定的schema
    */
  def formatCurrentBySchema(schema: String): String = {
    this.formatBySchema(new Date, schema)
  }

  /**
    * 将指定的unix元年时间转为yyyy-MM-dd 的字符串
    */
  def formatUnixDate(date: Long): String = {
    this.formatDate(new Date(date))
  }

  /**
    * 将指定的unix元年时间转为yyyy-MM-dd hh:mm:ss 的字符串
    */
  def formatUnixDateTime(dateTime: Long): String = {
    this.formatDateTime(new Date(dateTime))
  }

  /**
    * 对日期进行格式转换
    */
  def dateSchemaFormat(dateTimeStr: String, srcSchema: String, destSchema: String): String = {
    if (StringUtils.isBlank(dateTimeStr)) {
      return dateTimeStr
    }
    val timeFormat: SimpleDateFormat = new SimpleDateFormat(srcSchema)
    timeFormat.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
    val datetime = timeFormat.parse(dateTimeStr)
    timeFormat.applyPattern(destSchema)
    timeFormat.format(datetime)
  }

  /**
    * 对日期进行格式转换
    */
  def dateSchemaFormat(dateTime: Date, srcSchema: String, destSchema: String): Date = {
    val timeFormat: SimpleDateFormat = new SimpleDateFormat(srcSchema)
    timeFormat.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
    val dateTimeStr = timeFormat.format(dateTime)
    timeFormat.applyPattern(destSchema)
    timeFormat.parse(dateTimeStr)
  }

  /**
    * 判断两个日期是否为同一天
    */
  def isSameDay(day1: String, day2: String): Boolean = {
    if (StringUtils.isNotBlank(day1) && StringUtils.isNotBlank(day2)) {
      val format = this.getTimeFormat()
      DateUtils.isSameDay(format.parse(day1), format.parse(day2))
    } else {
      false
    }
  }

  /**
    * 判断两个日期是否为同一天
    */
  def isSameDay(day1: Date, day2: Date): Boolean = {
    DateUtils.isSameDay(day1, day2)
  }

  /**
    * 用于判断给定的时间是否和系统时间处于同一天
    */
  def isSameDay(date: String): Boolean = {
    try {
      DateUtils.isSameDay(new Date(), this.getTimeFormat().parse(date))
    } catch {
      case e: Exception => {
        logger.error("isSameDay判断失败", e)
        false
      }
    }
  }

  /**
    * 判断两个日期是否为同一小时（前提是同一天）
    */
  def isSameHour(day1: String, day2: String): Boolean = {
    if (StringUtils.isNotBlank(day1) && StringUtils.isNotBlank(day2)) {
      val format = this.getTimeFormat()
      val d1 = format.parse(day1)
      val d2 = format.parse(day2)
      if (this.isSameDay(d1, d2)) {
        d1.getHours == d2.getHours
      } else {
        false
      }
    } else {
      false
    }
  }

  /**
    * 判断两个日期是否为同一小时（前提是同一天）
    */
  def isSameHour(day1: Date, day2: Date): Boolean = {
    if (this.isSameDay(day1, day2)) {
      day1.getHours == day2.getHours
    } else {
      false
    }
  }

  /**
    * 判断两个日期是否为同一星期（必须是同年同月）
    */
  def isSameWeek(day1: Date, day2: Date): Boolean = {
    if (this.isSameYear(day1, day2) && this.isSameMonth(day1, day2)) {
      val cal = Calendar.getInstance()
      cal.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
      cal.setTime(day1)
      val week1 = cal.get(Calendar.DAY_OF_WEEK_IN_MONTH)
      cal.setTime(day2)
      week1 == cal.get(Calendar.DAY_OF_WEEK_IN_MONTH)
    } else {
      false
    }
  }

  /**
    * 判断两个日期是否为同一星期（必须是同年同月）
    */
  def isSameWeek(day1: String, day2: String): Boolean = {
    if (StringUtils.isNotBlank(day1) && StringUtils.isNotBlank(day2)) {
      val format = this.getTimeFormat()
      val d1 = format.parse(day1)
      val d2 = format.parse(day2)
      this.isSameWeek(d1, d2)
    } else {
      false
    }
  }

  /**
    * 判断两个日期是否为同一月份
    */
  def isSameMonth(day1: Date, day2: Date): Boolean = {
    day1.getMonth == day2.getMonth
  }

  /**
    * 判断两个日期是否为同一月份
    */
  def isSameMonth(day1: String, day2: String): Boolean = {
    val format = this.getTimeFormat()
    val d1 = format.parse(day1)
    val d2 = format.parse(day2)
    this.isSameMonth(d1, d2)
  }

  /**
    * 判断两个日期是否为同一年
    */
  def isSameYear(day1: Date, day2: Date): Boolean = {
    day1.getYear == day2.getYear
  }

  /**
    * 判断两个日期是否为同一年
    */
  def isSameYear(day1: String, day2: String): Boolean = {
    val format = this.getTimeFormat()
    val d1 = format.parse(day1)
    val d2 = format.parse(day2)
    this.isSameYear(d1, d2)
  }

  /**
    * day1是否大于day2
    */
  def isBig(day1: String, day2: String): Boolean = {
    if (StringUtils.isNotBlank(day1) && StringUtils.isNotBlank(day2)) {
      DateFormatUtils.formatDateTime(day1).after(DateFormatUtils.formatDateTime(day2))
    } else if (StringUtils.isNotBlank(day1) && StringUtils.isBlank(day2)) {
      true
    } else if (StringUtils.isBlank(day1) && StringUtils.isNotBlank(day2)) {
      false
    } else {
      true
    }
  }

  /**
    * day1是否小于day2
    */
  def isSmall(day1: String, day2: String): Boolean = {
    !this.isBig(day1, day2)
  }

  /**
    * day 是否介于day1与day2之间
    */
  def isBetween(day: String, day1: String, day2: String) = {
    this.isSmall(day, day2) && this.isBig(day, day1)
  }

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
    if (this.YEAR.equalsIgnoreCase(field)) {
      this.addYears(dateTimeStr, count)
    } else if (this.MONTH.equalsIgnoreCase(field)) {
      this.addMons(dateTimeStr, count)
    } else if (this.DAY.equalsIgnoreCase(field)) {
      this.addDays(dateTimeStr, count)
    } else if (this.HOUR.equalsIgnoreCase(field)) {
      this.addHours(dateTimeStr, count)
    } else if (this.MINUTE.equalsIgnoreCase(field)) {
      this.addMins(dateTimeStr, count)
    } else if (this.SECOND.equalsIgnoreCase(field)) {
      this.addSecs(dateTimeStr, count)
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行年度加减
    */
  def addYears(dateTimeStr: String, years: Int): String = {
    if (StringUtils.isNotBlank(dateTimeStr) && !"null".equals(dateTimeStr) && !"NULL".equals(dateTimeStr)) {
      val datetime = DateFormatUtils.formatDateTime(dateTimeStr)
      DateFormatUtils.formatDateTime(DateUtils.addYears(datetime, years))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行年度加减
    */
  def addYears(dateTime: Date, years: Int): String = {
    if (dateTime != null) {
      DateFormatUtils.formatDateTime(DateUtils.addYears(dateTime, years))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行月份加减
    */
  def addMons(dateTimeStr: String, mons: Int): String = {
    if (StringUtils.isNotBlank(dateTimeStr) && !"null".equals(dateTimeStr) && !"NULL".equals(dateTimeStr)) {
      val datetime = DateFormatUtils.formatDateTime(dateTimeStr)
      DateFormatUtils.formatDateTime(DateUtils.addMonths(datetime, mons))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行月份加减
    */
  def addMons(dateTime: Date, mons: Int): String = {
    if (dateTime != null) {
      DateFormatUtils.formatDateTime(DateUtils.addMonths(dateTime, mons))
    } else {
      ""
    }
  }

  /**
    * 对指定日期增加天
    */
  def addDays(dateTimeStr: String, days: Int): String = {
    if (StringUtils.isNotBlank(dateTimeStr) && !"null".equals(dateTimeStr) && !"NULL".equals(dateTimeStr)) {
      val datetime = DateFormatUtils.formatDateTime(dateTimeStr)
      DateFormatUtils.formatDateTime(DateUtils.addDays(datetime, days))
    } else {
      ""
    }
  }

  /**
    * 对指定日期增加天
    */
  def addDays(dateTime: Date, days: Int): String = {
    if (dateTime != null) {
      DateFormatUtils.formatDateTime(DateUtils.addDays(dateTime, days))
    } else {
      ""
    }
  }

  /**
    * 对指定日期增加天，并以指定的格式返回
    */
  def addPartitionDays(dateTime: Date, days: Int, schema: String = "yyyyMMdd"): String = {
    if (dateTime != null) {
      DateFormatUtils.formatBySchema(DateUtils.addDays(dateTime, days), schema)
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行天加减
    */
  def addWeeks(dateTimeStr: String, weeks: Int): String = {
    if (StringUtils.isNotBlank(dateTimeStr) && !"null".equals(dateTimeStr) && !"NULL".equals(dateTimeStr)) {
      val datetime = DateFormatUtils.formatDateTime(dateTimeStr)
      DateFormatUtils.formatDateTime(DateUtils.addWeeks(datetime, weeks))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行天加减
    */
  def addWeeks(dateTime: Date, weeks: Int): String = {
    if (dateTime != null) {
      DateFormatUtils.formatDateTime(DateUtils.addWeeks(dateTime, weeks))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行小时加减
    */
  def addHours(dateTimeStr: String, hours: Int): String = {
    if (StringUtils.isNotBlank(dateTimeStr) && !"null".equals(dateTimeStr) && !"NULL".equals(dateTimeStr)) {
      val datetime = DateFormatUtils.formatDateTime(dateTimeStr)
      DateFormatUtils.formatDateTime(DateUtils.addHours(datetime, hours))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行小时加减
    */
  def addHours(dateTime: Date, hours: Int): String = {
    if (dateTime != null) {
      DateFormatUtils.formatDateTime(DateUtils.addHours(dateTime, hours))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行分钟加减
    */
  def addMins(dateTimeStr: String, minutes: Int): String = {
    if (StringUtils.isNotBlank(dateTimeStr) && !"null".equals(dateTimeStr) && !"NULL".equals(dateTimeStr)) {
      val datetime = DateFormatUtils.formatDateTime(dateTimeStr)
      DateFormatUtils.formatDateTime(DateUtils.addMinutes(datetime, minutes))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行分钟加减
    */
  def addMins(dateTime: Date, minutes: Int): String = {
    if (dateTime != null) {
      DateFormatUtils.formatDateTime(DateUtils.addMinutes(dateTime, minutes))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行秒钟加减
    */
  def addSecs(dateTimeStr: String, seconds: Int): String = {
    if (StringUtils.isNotBlank(dateTimeStr) && !"null".equals(dateTimeStr) && !"NULL".equals(dateTimeStr)) {
      val datetime = DateFormatUtils.formatDateTime(dateTimeStr)
      DateFormatUtils.formatDateTime(DateUtils.addSeconds(datetime, seconds))
    } else {
      ""
    }
  }

  /**
    * 对指定的时间字段进行秒钟加减
    */
  def addSecs(dateTime: Date, seconds: Int): String = {
    if (dateTime != null) {
      DateFormatUtils.formatDateTime(DateUtils.addSeconds(dateTime, seconds))
    } else {
      ""
    }
  }

  /**
    * 获取day1到day2之间的所有日期
    *
    * @param prefix
    * 指定拼接前缀
    */
  def getBetweenDate(prefix: String, day1: String, day2: String): Array[String] = {
    val dates = ArrayBuffer[String]()
    var nextDay = this.addDays(day1, 1)
    if (this.isBetween(nextDay, day1, day2)) {
      dates += s"$prefix >= to_date('$day1','yyyy-mm-dd hh24:mi:ss') and $prefix < to_date('$nextDay','yyyy-mm-dd hh24:mi:ss')"
    }
    while (this.isBetween(nextDay, day1, day2)) {
      var tmpDay = ""
      tmpDay = this.addDays(nextDay, 1)
      dates += s"$prefix >= to_date('$nextDay','yyyy-mm-dd hh24:mi:ss') and $prefix < to_date('$tmpDay','yyyy-mm-dd hh24:mi:ss')"
      nextDay = tmpDay
    }
    dates.toArray
  }

  /**
    * 计算date1与date2之间相差的小时数
    * @return
    *         相差的小时数
    */
  def betweenHours(date1: Date, date2: Date): Double = {
    (date1.getTime - date2.getTime) / 3600000.0
  }

  /**
    * 将yyyy-MM-dd hh:mm:ss类型日期truncate为月初零点
    */
  def truncateMonth(dateTime: Date): String = {
    val cal = Calendar.getInstance()
    if (dateTime != null) cal.setTime(dateTime)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    if (month < 10)
      year + "-0" + month + "-01 00:00:00"
    else
      year + "-" + month + "-01 00:00:00"
  }

  /**
    * 取年月日
    */
  def getyyyyMMdd(dataTime: String): String = {
    if (StringUtils.isNotBlank(dataTime) && dataTime.length >= 10) {
      dataTime.substring(0, 10)
    } else {
      dataTime
    }
  }

  /**
    * 取年月日
    */
  def getyyyyMM(dataTime: String): String = {
    if (StringUtils.isNotBlank(dataTime) && dataTime.length >= 7) {
      dataTime.substring(0, 7)
    } else {
      dataTime
    }
  }

  /**
    * 取年月日
    */
  def getyyyy(dataTime: String): String = {
    if (StringUtils.isNotBlank(dataTime) && dataTime.length >= 4) {
      dataTime.substring(0, 4)
    } else {
      dataTime
    }
  }

  /**
    * 获取指定日期的月初时间，如为空则返回系统当前时间对应的月初
    */
  def truncateMonthStr(dateTime: String): String = {
    var dateTimeStr = dateTime
    if (StringUtils.isBlank(dateTimeStr)) {
      dateTimeStr = this.getTimeFormat().format(new Date)
    }
    this.truncateMonth(this.formatDate(dateTimeStr))
  }

  /**
    * 根据指定的时间和格式，将时间格式化为hive分区格式
    */
  def getPartitionTime(dateTime: String = this.formatCurrentDateTime(), schema: String = DateFormatUtils.yyyyMMdd): String = {
    this.dateSchemaFormat(dateTime, DateFormatUtils.yyyy_MM_ddHHmmss, schema)
  }

  /**
    * 将当前系统时间格式化为指定的格式作为分区
    */
  def getCurrentPartitionTime(schema: String = DateFormatUtils.yyyyMMdd): String = {
    getPartitionTime(this.formatCurrentDateTime(), schema)
  }

  /**
    * 获取两个时间间隔的毫秒数
    */
  def interval(before: Date, after: Date): Long = {
    after.getTime - before.getTime
  }

  /**
    * 获取两个时间间隔的毫秒数
    */
  def interval(before: String, after: String): Long = {
    this.formatDateTime(after).getTime - this.formatDateTime(before).getTime
  }

  /**
    * 将yyyy-MM-dd hh:mm:ss类型日期truncate为整点分钟
    */
  def truncateMinute(dateTime: String): String = {
    val date = this.formatDateTime(dateTime)
    val prefix = this.dateSchemaFormat(dateTime, DateFormatUtils.yyyy_MM_ddHHmmss, "yyyy-MM-dd HH")
    val minute = date.getMinutes
    if (minute >= 0 && minute < 10) {
      s"$prefix:00"
    } else if (minute >= 10 && minute < 20) {
      s"$prefix:10"
    } else if (minute >= 20 && minute < 30) {
      s"$prefix:20"
    } else if (minute >= 30 && minute < 40) {
      s"$prefix:30"
    } else if (minute >= 40 && minute < 50) {
      s"$prefix:40"
    } else {
      s"$prefix:50"
    }
  }

  /**
    * 将yyyy-MM-dd hh:mm:ss类型日期truncate为整点分钟
    */
  def truncateMinute(dateTime: Date): String = {
    this.truncateMinute(this.formatDateTime(dateTime))
  }

  /**
    * 获取整点小时
    */
  def truncateHour(dateStr: String): String = {
    this.dateSchemaFormat(dateStr, DateFormatUtils.yyyy_MM_ddHHmmss, DateFormatUtils.yyyyMMddHH)
  }

  /**
    * 截取指定时间指定的位数
    *
    * @param date
    * 日期
    * @param cron
    * 切分的范围
    * @param replace
    * 是否替换掉日期字符串中的特殊字符
    * @return
    */
  def truncate(date: String, cron: String = this.DAY, replace: Boolean = true): String = {
    if (StringUtils.isBlank(date) || StringUtils.isBlank(cron) || date.length != 19) {
      throw new IllegalArgumentException("日期不能为空，格式为yyyy-MM-dd HH:mm:ss")
    }
    if (!this.enumSet.contains(cron)) {
      throw new IllegalArgumentException("where参数必须是hour/day/week/month/year中的一个")
    }
    val index: Int = if (this.HOUR.equals(cron)) {
      13
    } else if (this.DAY.equals(cron)) {
      10
    } else if (this.MONTH.equals(cron)) {
      7
    } else if (this.MINUTE.equals(cron)) {
      15
    } else {
      4
    }
    if (replace) date.substring(0, index).replace("-", "").replace(":", "").replace(" ", "") else date.substring(0, index)
  }

  /**
    * 截取指定时间指定的位数
    *
    * @param date
    * 日期
    * @param cron
    * 切分的范围
    * @param replace
    * 是否替换掉日期字符串中的特殊字符
    * @return
    */
  def truncate(date: Date, cron: String, replace: Boolean): String = {
    this.truncate(this.formatDateTime(date), cron, replace)
  }

  /**
    * 截取系统时间指定的位数
    *
    * @param cron
    * 切分的范围
    * @param replace
    * 是否替换掉日期字符串中的特殊字符
    * @return
    */
  def truncate(cron: String, replace: Boolean): String = {
    this.truncate(this.formatCurrentDateTime(), cron, replace)
  }

  /**
    * 判断给定的时间的秒位的个位是否为0秒，如00/10/20/30/40/60/60
    */
  def isSecondDivisibleZero(date: Date = new Date): Boolean = {
    val cal = Calendar.getInstance()
    cal.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
    cal.setTime(date)
    cal.get(Calendar.SECOND) % 10 == 0
  }

  /**
    * 判断给定的时间的秒位的个位是否为0秒，如00/10/20/30/40/60/60
    */
  def isSecondDivisibleZero(dateTime: String): Boolean = {
    this.isSecondDivisibleZero(this.formatDateTime(dateTime))
  }

  /**
    * 判断给定的时间的秒位是否为00秒
    */
  def isZeroSecond(date: Date = new Date): Boolean = {
    val cal = Calendar.getInstance()
    cal.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
    cal.setTime(date)
    cal.get(Calendar.SECOND) == 0
  }

  /**
    * 判断给定的时间的秒位是否为00秒
    */
  def isZeroSecond(dateTime: String): Boolean = {
    this.isZeroSecond(this.formatDateTime(dateTime))
  }

  /**
    * 判断给定的时间的分钟位是否为00分
    */
  def isZeroMinute(date: Date = new Date): Boolean = {
    if (this.isZeroSecond(date)) {
      val cal = Calendar.getInstance()
      cal.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
      cal.setTime(date)
      cal.get(Calendar.MINUTE) == 0
    } else {
      false
    }
  }

  /**
    * 判断给定的时间的分钟位是否为00分
    */
  def isZeroMinute(dateTime: String): Boolean = {
    this.isZeroMinute(this.formatDateTime(dateTime))
  }

  /**
    * 判断给定的时间的小时位是否为00时
    */
  def isZeroHour(date: Date = new Date): Boolean = {
    if (this.isZeroMinute(date)) {
      val cal = Calendar.getInstance()
      cal.setTimeZone(TimeZone.getTimeZone(timeZoneShangHai))
      cal.setTime(date)
      cal.get(Calendar.HOUR_OF_DAY) == 0
    } else {
      false
    }
  }

  /**
    * 判断给定的时间的小时位是否为00时
    */
  def isZeroHour(dateTime: String): Boolean = {
    this.isZeroHour(this.formatDateTime(dateTime))
  }

  /**
    * 获取系统当前时间，精确到秒
    */
  def currentTime: Long = {
    System.currentTimeMillis() / 1000
  }

  /**
    * 计算运行时长
    */
  def runTime(startTime: Long): String = {
    val currentTime = this.currentTime
    val apartTime = currentTime - startTime
    val hours = apartTime / 3600
    val hoursStr = if (hours < 10) s"0${hours}" else s"${hours}"
    val minutes = apartTime / 60 - hours * 60
    val minutesStr = if (minutes < 10) s"0${minutes}" else s"${minutes}"
    val seconds = apartTime - minutes * 60 - hours * 60 * 60
    val secondsStr = if (seconds < 10) s"0${seconds}" else s"${seconds}"

    s"${hoursStr}时 ${minutesStr}分 ${secondsStr}秒"
  }
}
