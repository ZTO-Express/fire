package com.zto.fire.examples.flink.connector.sql

import com.zto.fire.core.anno.lifecycle.{Step1, Step2, Step3}
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * DataGen connector使用
 *
 * @author ChengLong
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(interval = 10)
object DataGenTest extends FlinkStreaming {
  private lazy val dataGenTable = "t_student"
  private lazy val sinkPrintTable = "t_print_table"

  @Step1("定义源表表结构")
  def sourceTable: Unit = {
    sql(
      s"""
         | CREATE TABLE ${this.dataGenTable} (
         |   id BIGINT,
         |   name STRING,
         |   age INT,
         |   createTime TIMESTAMP(13),
         |   sex Boolean
         |) WITH (
         |   'connector' = 'datagen',
         |   'rows-per-second'='100', -- 5000/s
         |   'fields.id.min'='1', -- id字段，1到1000之间
         |   'fields.id.max'='1000',
         |   'fields.name.length'='5', -- name字段，长度为5
         |   'fields.age.min'='1', -- age字段，1到120岁
         |   'fields.age.max'='120'
         |)
         |""".stripMargin)
  }

  @Step2("定义目标表结构")
  def destTable: Unit = {
    sql(
      s"""
         |CREATE TABLE ${this.sinkPrintTable} WITH ('connector' = 'print')
         |LIKE ${this.dataGenTable} (EXCLUDING ALL)
         |""".stripMargin)
  }

  @Step3("执行insert语句")
  def insertStatement: Unit = {
    sql(
      s"""
         |insert into ${this.sinkPrintTable}
         |select
         | id, name, age, createTime, sex
         |from ${this.dataGenTable}
         |group by id, name, age, createTime, sex
         |""".stripMargin)
  }
}
