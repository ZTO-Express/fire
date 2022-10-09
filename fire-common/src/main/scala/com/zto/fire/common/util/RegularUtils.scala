package com.zto.fire.common.util

/**
 * 常用的正则表达式
 *
 * @author ChengLong 2021-5-28 11:14:19
 * @since fire 2.0.0
 */
object RegularUtils {
  // 用于匹配纯数值的表达式
  lazy val numeric = "(^[1-9]\\d*\\.?\\d*$)|(^0\\.\\d*[1-9]$)".r
  // 用于匹配字符串中以数值开头的数值
  lazy val numericPrefix = "(^[1-9]\\d*\\.?\\d*)|(^0\\.\\d*[1-9])".r
  // 用于匹配字符串中以固定的字母+空白符结尾
  lazy val unitSuffix = "[a-zA-Z]+\\s*$".r
  // 用于匹配使用#号作为注释的所有结尾
  lazy val propAnnotation = "\\s+\\#.*".r
  // 用于匹配insert语句
  lazy val insertReg = "^\\s*INSERT.*".r
  // 用于匹配sql中的with表达式的value
  lazy val withValueReg = """=\s*'.+'""".r
  // 用于匹配sql中with表达式value具体的值
  lazy val valueReg = """'.+'""".r
  // 用于匹配flink sql with表达式中数据源别名
  lazy val withDatasourceReg = """'datasource'\s*=\s*'[A-Za-z0-9_]+'""".r
  // 多条sql语句的截取
  lazy val sqlSplit = """;\s"""
}
