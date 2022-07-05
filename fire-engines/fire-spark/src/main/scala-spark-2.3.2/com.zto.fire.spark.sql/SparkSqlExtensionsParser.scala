package com.zto.fire.spark.sql

import com.zto.fire.spark.conf.FireSparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}


/**
 * Spark Sql解析扩展，用于拦截执行的sql以及解析sql中的血缘
 *
 * @author ChengLong 2021-6-23 10:25:17
 * @since 2.0.0
 */
private[fire] class SparkSqlExtensionsParser(parser: ParserInterface) {

}

private[fire] object SparkSqlExtensionsParser {

  /**
   * 启用自定义Sql解析器扩展
   */
  def sqlExtension(sessionBuilder: SparkSession.Builder): SparkSession.Builder = {
    sessionBuilder
  }
}