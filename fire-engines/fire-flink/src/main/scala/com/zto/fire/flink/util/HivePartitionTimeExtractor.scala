package com.zto.fire.flink.util

import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.filesystem.PartitionTimeExtractor
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util

/**
 * hive分区时间提取器，分区格式为yyyyMMdd
 *
 * @author ChengLong 2021年7月30日13:56:16
 */
private[fire] class HivePartitionTimeExtractor(pattern: String = "$ds") extends PartitionTimeExtractor {
  private val DEFAULT_PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val HOUR_PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH")

  def this() {
    this("$ds")
  }

  override def extract(partitionKeys: util.List[String], partitionValues: util.List[String]): LocalDateTime = {
    var timestampString: String = null
    if (pattern == null) timestampString = partitionValues.get(0)
    else {
      timestampString = pattern
      for (i <- 0 until partitionKeys.size) {
        timestampString = timestampString.replaceAll("\\$" + partitionKeys.get(i), partitionValues.get(i))
      }
    }
    toLocalDateTime(timestampString).plusHours(-8)
  }

  def toLocalDateTime(timestampString: String): LocalDateTime = {
    try {
      LocalDateTime.of(
        LocalDate.parse(timestampString, DEFAULT_PARTITION_FORMATTER),
        LocalTime.MIDNIGHT)
    } catch {
      case e: Exception => {
        LocalDateTime.of(
          LocalDate.parse(timestampString, HOUR_PARTITION_FORMATTER),
          LocalTime.MIDNIGHT)
        throw e
      }
    }
  }

  def toMills(dateTime: LocalDateTime): Long = TimestampData.fromLocalDateTime(dateTime).getMillisecond

  def toMills(timestampString: String): Long = toMills(toLocalDateTime(timestampString))
}
