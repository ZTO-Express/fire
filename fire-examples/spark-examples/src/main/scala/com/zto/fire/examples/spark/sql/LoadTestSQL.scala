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

package com.zto.fire.examples.spark.sql

/**
 * 用于集群压测程序的SQL
 * @author ChengLong 2019年10月25日 13:32:19
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
object LoadTestSQL {

  def jsonParseSQL: String = {
    """
      |select
      | after.CAR_SIGN_CODE,
      | nvl(after.CAR_SIGN_CODE_OLD,'') CAR_SIGN_CODE_OLD,
      | after.CAR_DATE,
      | after.SCAN_SITE_ID,
      | after.PRE_OR_NEX_STA_ID,
      | after.PRE_OR_NEXT_STATION,
      | from_unixtime(
      |    unix_timestamp(after.CAR_DATE,'yyyy-MM-dd HH:mm:ss'),
      |    'yyyy-MM-dd HH:mm:ss'
      | ) ARRIVE_DATE
      |from test t1
      |left join dim_c2c_cost t2
      |on after.SCAN_SITE_ID=t2.start_site_id
      |and after.PRE_OR_NEX_STA_ID=t2.end_site_id
      |and substr(after.CAR_DATE,12,2)=t2.hh24
      |""".stripMargin
  }

  def loadSQL: String = {
    """
      |select
      |           f.site_name as site_name,
      |		   f.site_id as site_id,
      |		   f.collect_date as collect_date,
      |           sum(f.rec_count) as rec_count,
      |           sum(f.rec_weight) as rec_weight,
      |           sum(f.send_count) as send_count,
      |           sum(f.send_weight) as send_weight,
      |           sum(f.send_bag_count) as send_bag_count,
      |           sum(f.send_bag_bill_count) as send_bag_bill_count,
      |           sum(f.send_bag_weight) as send_bag_weight,
      |           sum(f.come_count) as come_count,
      |           sum(f.come_weight) as come_weight,
      |           sum(f.come_bag_count) as come_bag_count,
      |		   sum(f.come_bag_weight) as come_bag_weight,
      |           sum(f.come_bag_bill_count) as come_bag_bill_count,
      |           sum(f.disp_count) as disp_count,
      |           sum(f.disp_weight) as disp_weight,
      |           sum(f.sign_count) as sign_count,
      |		   f.ds as ds
      |    from (
      |    select f.scan_site as site_name,
      |	       f.scan_site_id as site_id,
      |		   f.scan_date as collect_date,
      |           count(f.bill_code) as rec_count,
      |           sum(nvl(f.weight,0)) as rec_weight,
      |           0 as send_count,
      |           0 as send_weight,
      |           0 as send_bag_count,
      |           0 as send_bag_bill_count,
      |           0 as send_bag_weight,
      |           0 as come_count,
      |           0 as come_weight,
      |           0 as come_bag_count,
      |           0 as come_bag_bill_count,
      |           0 as come_bag_weight,
      |           0 as disp_count,
      |           0 as disp_weight,
      |           0 as sign_count,
      |		   f.ds as ds
      |    from (select a.scan_site,
      |                 a.scan_site_id,
      |                 to_date(a.scan_date) as scan_date,
      |                 last_value(a.bill_code) over (partition by a.scan_site_id,a.bill_code order by a.scan_date desc) as bill_code,
      |                 max(a.weight) over (partition by a.scan_site_id,a.bill_code) as weight,
      |				 a.ds as ds
      |          from dw.dw_zt_zto_scan_rec a
      |          where a.ds>='20191020'
      |            and a.ds<'20191021'
      |            and a.scan_site_id>0
      |          ) f group by f.scan_site,f.scan_site_id,f.scan_date,f.ds
      |
      |    union all
      |    select f.scan_site as site_name,f.scan_site_id as site_id,f.scan_date as collect_date,
      |           0 as rec_count,
      |           0 as rec_weight,
      |           count(f.bill_code) as send_count,
      |           sum(f.weight) as send_weight,
      |           0 as send_bag_count,
      |           0 as send_bag_bill_count,
      |           0 as send_bag_weight,
      |           0 as come_count,
      |           0 as come_weight,
      |           0 as come_bag_count,
      |           0 as come_bag_bill_count,
      |           0 as come_bag_weight,
      |           0 as disp_count,
      |           0 as disp_weight,
      |           0 as sign_count,
      |		   f.ds as ds
      |    from (select a.scan_site,
      |                 a.scan_site_id,
      |                 to_date(a.scan_date) as scan_date,
      |                 last_value(a.bill_code) over (partition by a.scan_site_id,a.bill_code order by a.scan_date desc) as bill_code,
      |                 max(a.weight) over (partition by a.scan_site_id,a.bill_code) as weight,
      |				 a.ds as ds
      |          from dw.dw_zt_zto_scan_send a
      |          where a.ds>='20191020'
      |            and a.ds<'20191021'
      |                and a.scan_site_id>0
      |          ) f group by f.scan_site,f.scan_site_id,f.scan_date,f.ds
      |    union all
      |    select f.scan_site as site_name,f.scan_site_id as site_id,f.scan_date as collect_date,
      |           0 as rec_count,
      |           0 as rec_weight,
      |           0 as send_count,
      |           0 as send_weight,
      |           count(f.bill_code) as send_bag_count,
      |           sum(d.bagbillsum) as send_bag_bill_count,
      |           sum(nvl(f.weight,0)) as send_bag_weight,
      |           0 as come_count,
      |           0 as come_weight,
      |           0 as come_bag_count,
      |           0 as come_bag_bill_count,
      |           0 as come_bag_weight,
      |           0 as disp_count,
      |           0 as disp_weight,
      |           0 as sign_count,
      |		   f.ds as ds
      |    from (select a.scan_site,
      |                 a.scan_site_id,
      |                 to_date(a.scan_date) as scan_date ,
      |                 LAST_VALUE(a.bill_code) over (partition by a.scan_site_id,a.bill_code order by a.scan_date desc) as bill_code,
      |                 max(a.weight) over (partition by a.scan_site_id,a.bill_code) as weight,
      |				 a.ds as ds
      |          from dw.dw_zt_zto_scan_send_bag a
      |          where a.ds>='20191020'
      |            and a.ds<'20191021'
      |            and a.scan_site_id>0
      |          ) f
      |		 left join (select sum(bagbillsum) as bagbillsum, bill_code as owner_bag_no   from dw.zto_bagbillsum_weight   where ds>='20190920'
      |                and ds<='20191020'
      |                group by bill_code
      |             ) d on d.owner_bag_no=f.bill_code
      |		 group by f.scan_site,f.scan_site_id,f.scan_date,f.ds
      |    union all
      |    select f.scan_site as site_name,f.scan_site_id as site_id,f.scan_date as collect_date,
      |           0 as rec_count,
      |           0 as rec_weight,
      |           0 as send_count,
      |           0 as send_weight,
      |           0 as send_bag_count,
      |           0 as send_bag_bill_count,
      |           0 as send_bag_weight,
      |           count(f.bill_code) as come_count,
      |           sum(f.weight) as come_weight,
      |           0 as come_bag_count,
      |           0 as come_bag_bill_count,
      |           0 as come_bag_weight,
      |           0 as disp_count,
      |           0 as disp_weight,
      |           0 as sign_count,
      |		   f.ds as ds
      |    from (select a.scan_site,
      |                 a.scan_site_id,
      |                 to_date(a.scan_date) as scan_date,
      |                 last_value(a.bill_code) over (partition by a.scan_site_id,a.bill_code order by a.scan_date desc) as bill_code,
      |                 max(a.weight) over (partition by a.scan_site_id,a.bill_code) as weight,
      |				 a.ds as ds
      |          from dw.dw_zt_zto_scan_come a
      |          where a.ds>='20191020'
      |            and a.ds<'20191021'
      |            and a.scan_site_id>0
      |          ) f group by f.scan_site,f.scan_site_id,f.scan_date,f.ds
      |    union all
      |    select f.scan_site as site_name,f.scan_site_id as site_id,f.scan_date as collect_date,
      |           0 as rec_count,
      |           0 as rec_weight,
      |           0 as send_count,
      |           0 as send_weight,
      |           0 as send_bag_count,
      |           0 as send_bag_bill_count,
      |           0 as send_bag_weight,
      |           0 as come_count,
      |           0 as come_weight,
      |           count(f.bill_code) as come_bag_count,
      |           sum(bagbillsum) as come_bag_bill_count,
      |           sum(nvl(f.weight,0)) as come_bag_weight,
      |           0 as disp_count,
      |           0 as disp_weight,
      |           0 as sign_count,
      |		   f.ds as ds
      |    from (select a.scan_site,
      |                 a.scan_site_id,
      |                 to_date(a.scan_date) as scan_date,
      |                 last_value(a.bill_code) over (partition by a.scan_site_id,a.bill_code order by a.scan_date desc) as bill_code,
      |                 max(a.weight) over (partition by a.scan_site_id,a.bill_code) as weight,
      |				 a.ds as ds
      |          from dw.dw_zt_zto_scan_come_bag a
      |          where a.ds>='20191020'
      |            and a.ds<'20191021'
      |            and a.scan_site_id>0
      |          ) f
      |		  left join (
      |                   select sum(bagbillsum) as bagbillsum,bill_code as owner_bag_no  from dw.zto_bagbillsum_weight   where ds>='20190920'
      |                and ds<='20191020'
      |              group by bill_code
      |) d on d.owner_bag_no=f.bill_code
      |		  group by f.scan_site,f.scan_site_id,f.scan_date,f.ds
      |    union all
      |    select f.scan_site as site_name,f.scan_site_id as site_id,f.scan_date as collect_date,
      |           0 as rec_count,
      |           0 as rec_weight,
      |           0 as send_count,
      |           0 as send_weight,
      |           0 as send_bag_count,
      |           0 as send_bag_bill_count,
      |           0 as send_bag_weight,
      |           0 as come_count,
      |           0 as come_weight,
      |           0 as come_bag_count,
      |           0 as come_bag_bill_count,
      |           0 as come_bag_weight,
      |           count(f.bill_code) as disp_count,
      |           sum(nvl(f.weight,0)) as disp_weight,
      |           0 as sign_count,
      |		   f.ds as ds
      |    from (select a.scan_site,
      |                 a.scan_site_id,
      |                 to_date(a.scan_date) as scan_date,
      |                 last_value(a.bill_code) over (partition by a.scan_site_id,a.bill_code order by a.scan_date desc) as bill_code,
      |                 max(a.weight) over (partition by a.scan_site_id,a.bill_code) as weight,
      |				 a.ds as ds
      |          from dw.dw_zt_zto_scan_disp a
      |           where a.ds>='20191020'
      |            and a.ds<'20191021'
      |            and a.scan_site_id>0
      |          ) f group by f.scan_site,f.scan_site_id,f.scan_date,f.ds
      |    union all
      |    select
      |		   r.record_site as site_name,
      |		   r.record_site_id as site_id,
      |		   to_date(record_date) as collect_date,
      |           0 as rec_count,
      |           0 as rec_weight,
      |           0 as send_count,
      |           0 as send_weight,
      |           0 as send_bag_count,
      |           0 as send_bag_bill_count,
      |           0 as send_bag_weight,
      |           0 as come_count,
      |           0 as come_weight,
      |           0 as come_bag_count,
      |           0 as come_bag_bill_count,
      |           0 as come_bag_weight,
      |           0 as disp_count,
      |           0 as disp_weight,
      |           count(r.bill_code) as sign_count,
      |		   r.ds as ds
      |           from dw.dw_zt_zto_sign r
      |          where r.ds>='20191020'
      |            and r.ds<='20191021'
      |            and r.record_site_id>0
      |		 group by r.record_site,r.record_site_id,to_date(record_date),r.ds
      |		 )  f
      |		 group by  f.site_name ,
      |		   f.site_id ,
      |		   f.collect_date,
      |		   f.ds
      |DISTRIBUTE BY rand()
      |""".stripMargin
  }

  def cacheDim: String = {
    """
      |select cast(start_site_id as string),
      |       cast(end_site_id as string),
      |       substr(concat('0',cast(actual_start_date_hour as string)),-2,2) as hh24,
      |       cast(c2c_hour_percent50_onway_hour as string)  as cost_time
      |from ba.zy_tmp_center_onway_hour_configure
      """.stripMargin
  }
}
