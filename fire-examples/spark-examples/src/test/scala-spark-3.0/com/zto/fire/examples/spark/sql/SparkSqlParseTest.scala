package com.zto.fire.examples.spark.sql

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

import com.zto.fire.core.anno.Hive
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkCore
import com.zto.fire.spark.sql.SparkSqlParser
import org.junit.{Before, Test}

import scala.util.Try

/**
 * Spark SQL血缘解析单元测试
 *
 * @author ChengLong
 * @Date 2022-04-29 13:37:30
 * @since 2.2.1
 */
@Hive("test")
class SparkSqlParseTest extends BaseSparkCore {

  @Before
  def before: Unit = {
    this.init()
  }

  /**
   * 用于批量断言sql解析
   */
  private def assertSqlParse(assertMsg: String, sqls: String*): Unit = {
    sqls.foreach(sql => {
      val retVal = Try {
        SparkSqlParser.sqlParser(sql)
      }
      assert(retVal.isSuccess, assertMsg)
    })
  }

  /**
   * 断言临时表与hive表
   */
  @Test
  def testTempView: Unit = {
    val ds = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    ds.createOrReplaceTempView("t_student")
    assert(SparkSqlParser.isTempView(null, "t_student"))
    assert(!SparkSqlParser.isTempView(null, "t_student2"))
    assert(!SparkSqlParser.isHiveTable(null, "t_student"))
    assert(SparkSqlParser.isHiveTable("dim", "baseuser"))
    assert(!SparkSqlParser.isHiveTable("dim", "baseuser12"))
  }

  /**
   * 断言select语句的解析
   */
  @Test
  def testSelect: Unit = {
    val select1 =
      """
        |select count(*)
        |from (select * from st.st_fwzl_transfer_kpi_detail_month) a
        |left join (select biz_no,bill_code from dw.dw_kf_center_to_center_dispatch_delay where ds>='20210101') b
        |on a.bill_code=b.bill_code
        |""".stripMargin

    val select2 =
      """
        |select bill_event_id,count(*) from hudi.hudi_bill_item group by bill_event_id
        |""".stripMargin

    this.assertSqlParse("select spark sql解析失败", select1, select2)
  }

  /**
   * 测试insert语句的解析
   */
  @Test
  def testInsert: Unit = {
    val insertInto =
      """
        |insert into ods.base select a,v from tmp.t_user t1 left join ods.test t2 on t1.id=t2.id
        |""".stripMargin
    val insertOverwrite = "insert overwrite table dw.kwang_test partition(ds='202106', city='beijing') values(4,'zz')"
    val insertSelect =
      """
        |insert into zto_cockpit_site_target_ds
        |SELECT site_id,scan_date,scan_day,
        |SUM(a.rec_cnt) rec_cnt,
        |SUM(a.order_cnt) order_cnt,
        |SUM(a.disp_cnt) disp_cnt,
        |SUM(a.sign_cnt) sign_cnt,
        |SUM(a.ele_cnt) ele_cnt,
        |SUM(a.bag_cnt) bag_cnt
        |FROM (
        |SELECT t1.site_id,t1.scan_date,t1.scan_day ,
        |t1.cnt rec_cnt,
        |0 order_cnt,
        |0 disp_cnt,
        |0 sign_cnt,
        |t1.ele_cnt ele_cnt,
        |t1.bag_cnt bag_cnt
        |FROM ztkb.zto_cockpit_site_rec_ds t1
        |WHERE t1.scan_day = '#date#'
        |UNION ALL
        |SELECT t2.site_id,t2.order_date scan_date,t2.order_day scan_day ,
        |0 rec_cnt,
        |t2.cnt order_cnt,
        |0 disp_cnt,
        |0 sign_cnt,
        |0 ele_cnt,
        |0 bag_cnt
        |FROM ztkb.zto_cockpit_site_order_ds t2
        |WHERE t2.order_day = '#date#'
        |UNION ALL
        |SELECT t3.site_id,t3.scan_date,t3.scan_day ,
        |0 rec_cnt,
        |0 order_cnt,
        |t3.cnt disp_cnt,
        |0 sign_cnt,
        |0 ele_cnt,
        |0 bag_cnt
        |FROM ztkb.zto_cockpit_site_disp_ds t3
        |WHERE t3.scan_day = '#date#'
        |UNION ALL
        |select t.record_site_id site_id,t.sign_date scan_date,t.sign_day scan_day,
        |0 rec_cnt,
        |0 order_cnt,
        |0 disp_cnt,
        |sum(t.cnt) sign_cnt,
        |0 ele_cnt,
        |0 bag_cnt
        |from ztkb.zto_cockpit_site_sign_ds t
        |where t.sign_day = '#date#'
        |group by t.record_site_id,t.sign_date,t.sign_day
        |) a
        |GROUP BY site_id,scan_date,scan_day
      """.stripMargin

    this.assertSqlParse("insert spark sql解析失败", insertInto, insertOverwrite, insertSelect)
  }

  /**
   * 测试alter语句解析
   */
  @Test
  def testAlter: Unit = {
    val alterTableAddPartitionStatement =
      """
        |alter table tmp.t_user add if not exists partition (ds='20210620', city = 'beijing')
        |""".stripMargin
    val renameTable =
      """
        |alter table tmp.t_user rename to ods.t_user2
        |""".stripMargin
    val dropPartition =
      """
        |ALTER TABLE tmp.food DROP IF EXISTS PARTITION (ds='20151219', city = 'beijing')
        |""".stripMargin
    val renamePartition =
      """
        |Alter table tmp.test partition (ds='201801', city='beijing') rename to partition(ds='202106', city='shanghai')
        |""".stripMargin
    this.assertSqlParse("解析alter语句失败", alterTableAddPartitionStatement, renameTable, dropPartition, renamePartition)
  }

  /**
   * 测试ddl语句的解析
   */
  @Test
  def testDDL: Unit = {
    val createTable =
      """
        |CREATE TABLE `tmp.test`(
        |  `dept_no` int,
        |  `addr` string,
        |  `tel` string)
        |partitioned by(ds string, city string)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |""".stripMargin

    val dropTable =
      """
        |drop table if exists tmp.test
        |""".stripMargin

    val dropDB = "drop database tmp"

    this.assertSqlParse("ddl语句解析失败", createTable, dropTable, dropDB)
  }
}
