package com.zto.fire.examples.flink.connector.sql

object DDL {

  /**
   * 创建自动生成数据的source table
   *
   * @param tableName 表名
   * @param rowsPerSec 每秒产生的记录数
   *
   * @return ddl
   */
  def createStudent(tableName: String = "t_student", rowsPerSec: Int = 5000): String = {
    s"""
      | CREATE TABLE ${tableName} (
      |   id BIGINT,
      |   name STRING,
      |   age INT,
      |   createTime TIMESTAMP(13),
      |   length DECIMAL(5, 2),
      |   sex Boolean
      |) WITH (
      |   'connector' = 'datagen',
      |
      |   'rows-per-second'='${rowsPerSec}', -- 5000/s
      |
      |   'fields.id.min'='1', -- id字段，1到1000之间
      |   'fields.id.max'='1000',
      |
      |   'fields.name.length'='5', -- name字段，长度为5
      |
      |   'fields.age.min'='1', -- age字段，1到120岁
      |   'fields.age.max'='120',
      |
      |   'fields.length.min'='50', -- length字段，最小1000，最大10000
      |   'fields.length.max'='220'
      |)
      |""".stripMargin
  }

  /**
   * 创建print connector
   *
   * @param printTableName sink print 表名
   * @param likeTableName like的source 表名
   * @return 建表语句
   */
  def createPrintLike(printTableName: String = "t_print_table", likeTableName: String): String = {
    s"""
      |CREATE TABLE ${printTableName} WITH ('connector' = 'print')
      |LIKE ${likeTableName} (EXCLUDING ALL)
      |""".stripMargin
  }
}
