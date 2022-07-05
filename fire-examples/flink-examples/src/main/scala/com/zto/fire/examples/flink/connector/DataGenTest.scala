package com.zto.fire.examples.flink.connector

import com.zto.fire._
import com.zto.fire.examples.flink.connector.sql.DDL
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * DataGen connector使用
 *
 * @author ChengLong
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
object DataGenTest extends BaseFlinkStreaming {
  private lazy val dataGenTable = "t_student"
  private lazy val sinkPrintTable = "t_print_table"

  override def process: Unit = {
    this.fire.sql(DDL.createStudent(this.dataGenTable, 10000))
    this.fire.sql(DDL.createPrintLike(this.sinkPrintTable, this.dataGenTable))

    this.fire.sql(
      s"""
         |insert into ${this.sinkPrintTable}
         |select
         | id, name, age, createTime, length, sex
         |from ${this.dataGenTable}
         |group by id, name, age, createTime, length, sex
         |""".stripMargin)
  }
}
