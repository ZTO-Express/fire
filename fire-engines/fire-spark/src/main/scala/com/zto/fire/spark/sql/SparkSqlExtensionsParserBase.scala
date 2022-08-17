package com.zto.fire.spark.sql

import com.zto.fire.common.util.{ExceptionBus, Logging}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * 提供通用的sql解析与校验类
 * @param sparkSession
 * @param parser
 */
private[fire] class SparkSqlExtensionsParserBase(sparkSession: SparkSession, parser: ParserInterface) extends Logging {

  /**
   * Parse a string to a [[LogicalPlan]].
   */
  def parsePlan(sqlText: String): LogicalPlan = {
    try {
      sparkSession.sessionState.sqlParser.parseExpression(sqlText)
      SparkSqlParser.sqlParse(sqlText)
    } catch {
      case e: Throwable =>
        ExceptionBus.post(e, sqlText)
    }
    parser.parsePlan(sqlText)
  }

  /**
   * Parse a string to an [[Expression]].
   */
  def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)
}
