package com.zto.fire.examples.spark.sql

import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.sql.SparkSqlParser
import com.zto.fire.spark.sync.SparkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * Spark SQL血缘解析工具
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Hive("test")
object SparkSqlParseTest extends SparkCore {

  override def process: Unit = {
    val ds = this.spark.createDataFrame(Student.newStudentList(), classOf[Student])
    ds.createOrReplaceTempView("t_student")
    println("t_student -> " + SparkSqlParser.isHiveTable(TableIdentifier("t_student")))
    println("tmp.baseuser ->" + SparkSqlParser.isHiveTable(TableIdentifier("tmp.baseuser")))

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

    val insertInto =
      """
        |insert into ods.base select a,v from tmp.t_user t1 left join ods.test t2 on t1.id=t2.id
        |""".stripMargin
    val alterTableAddPartitionStatement =
      """
        |alter table tmp.t_user add if not exists partition (ds='20210620', city = 'beijing')
        |""".stripMargin
    val dropTable =
      """
        |drop table if exists tmp.test
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
    val createTable =
      """
        |CREATE TABLE `tmp.test`(
        |  `dept_no` int,
        |  `addr` string,
        |  `tel` string)
        |partitioned by(ds string, city string)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |""".stripMargin
    val createTableAsSelect =
      """
        |create table if not exists tmp.zto_fire_test
        |select a.*,'sh' as city
        |from dw.mdb_md_dbs a left join student t on a.ds=t.name
        |where ds='20211001' limit 100
        |""".stripMargin
    val dropDB = "drop database if exists tmp12"
    val insertOverwrite = "insert overwrite table dw.kwang_test partition(ds='202106', city='beijing') values(4,'zz')"

    val insertIntoAsSelect =
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

    SparkSqlParser.sqlParser(select1)
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(SparkLineageAccumulatorManager.getValue) + "\n\n")
    }, 0, 10, TimeUnit.SECONDS)
    Thread.currentThread().join()
  }
}
