package com.zto.fire.spark.sql

import com.zto.fire.core.sql.SqlExtensionsParser
import com.zto.fire.spark.conf.FireSparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface

/**
 * spark sql语法扩展
 * @author ChengLong
 * @date 2022-05-09 14:45:15
 * @since 2.2.2
 */
private[fire] object SqlExtensions extends SqlExtensionsParser {

  /**
   * 启用自定义Sql解析器扩展
   */
  def sqlExtension(sessionBuilder: SparkSession.Builder): Unit = {
    if (FireSparkConf.sqlExtensionsEnable) {
      type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
      type ExtensionsBuilder = SparkSessionExtensions => Unit
      val parserBuilder: ParserBuilder = (sparkSession, parser) => new SparkSqlExtensionsParser(sparkSession, parser)
      val extBuilder: ExtensionsBuilder = { e => e.injectParser(parserBuilder) }
      sessionBuilder.withExtensions(extBuilder)
    }
  }
}
