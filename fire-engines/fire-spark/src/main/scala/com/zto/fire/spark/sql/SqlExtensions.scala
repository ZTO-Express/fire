package com.zto.fire.spark.sql

import com.zto.fire.spark.conf.FireSparkConf
import org.apache.spark.sql.SparkSession

/**
 * spark sql语法扩展
 * @author ChengLong
 * @date 2022-05-09 14:45:15
 * @since 2.2.2
 */
private[fire] object SqlExtensions {

  /**
   * 启用自定义Sql解析器扩展
   */
  def sqlExtension(sessionBuilder: SparkSession.Builder): Unit = {
    if (FireSparkConf.sqlExtensionsEnable) SparkSqlExtensionsParser.sqlExtension(sessionBuilder)
  }

}
