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

package com.zto.fire.common.util

import org.junit.Test
import com.zto.fire.common.util.SQLUtils._

/**
 * SQLUtils单元测试
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-11-26 15:11
 */
class SQLUtilsTest {

  @Test
  def testParse: Unit = {
    val selectSql =
      """
        | select * FROM
        | student1 s join dev.teacher2 b
        |""".stripMargin
    tableParse(selectSql).foreach(tableName => println("匹配：" + tableName))

    val insertSQL =
      """
        |insert into dev.student3(id,name) values(1, 'root');
        |insert into teacher4(id,name) values(1, 'root');
        |""".stripMargin
    tableParse(insertSQL).foreach(tableName => println("匹配：" + tableName))

    val deleteSQL =
      """
        |delete from teacher5 where id=10;
        |delete from dev.teacher6 where id=10;
        |""".stripMargin
    tableParse(deleteSQL).foreach(tableName => println("匹配：" + tableName))

    val createSQL =
      """
        |create table hello7(idxxx);
        |create table if not EXISTS hello8;
        |CREATE TABLE student9 LIKE tmp.student10
        |""".stripMargin
    tableParse(createSQL).foreach(tableName => println("匹配：" + tableName))

    val alterSQL =
      """
        |LOAD DATA LOCAL INPATH '/home/hadoop/data/student1.txt' INTO TABLE student11
        |""".stripMargin
    tableParse(alterSQL).foreach(tableName => println("匹配：" + tableName))

    val testSQL =
      """
        |create table table_student12
        |insert into dev.student13_from
        |delete from `from_student14_from`
        |select * from (select * from student15)
        |select * from (select * from
        |student16)
        |""".stripMargin
    tableParse(testSQL).foreach(tableName => println("匹配：" + tableName))

    val start = System.currentTimeMillis()
    (1 to 1000).foreach(i => tableParse(selectSql))
    println("耗时：" + (System.currentTimeMillis() - start))
  }
}
