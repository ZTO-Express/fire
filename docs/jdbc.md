<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# JDBC读写

实时任务开发中，对jdbc读写的需求很高。为了简化jdbc开发步骤，fire框架对jdbc操作做了进一步封装，将许多常见操作简化成一行代码。另外，fire框架支持在同一个任务中对任意多个数据源进行读写。

### 一、数据源配置

数据源包括jdbc的url、driver、username与password等重要信息，建议将这些配置放到commons.properties中，避免每个任务单独配置。fire框架内置了c3p0数据库连接池，在分布式场景下，限制每个container默认最多3个connection，避免申请过多资源时申请太多的数据库连接。

```properties
db.jdbc.url                  =       jdbc:derby:memory:fire;create=true
db.jdbc.driver               =       org.apache.derby.jdbc.EmbeddedDriver
db.jdbc.maxPoolSize          =       3
db.jdbc.user                 =       fire
db.jdbc.password             =       fire

# 如果需要多个数据源，则可在每项配置的结尾添加对应的keyNum作为区分
db.jdbc.url2                  =       jdbc:mysql://localhost:3306/fire
db.jdbc.driver2               =       com.mysql.jdbc.Driver
db.jdbc.user2                 =       fire
db.jdbc.password2             =       fire
```

### 二、API使用

#### [2.1 spark任务](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/jdbc/JdbcTest.scala)

```scala
/**
   * 使用jdbc方式对关系型数据库进行增删改操作
   */
def testJdbcUpdate: Unit = {
    val timestamp = DateFormatUtils.formatCurrentDateTime()
    // 执行insert操作
    val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
    // 更新配置文件中指定的第二个关系型数据库
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1), keyNum = 2)

    // 执行更新操作
    val updateSql = s"UPDATE $tableName SET name=? WHERE id=?"
    this.fire.jdbcUpdate(updateSql, Seq("root", 1))

    // 执行批量操作
    val batchSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"

    this.fire.jdbcBatchUpdate(batchSql, Seq(Seq("spark1", 21, timestamp, 100.123, 1),
                                            Seq("flink2", 22, timestamp, 12.236, 0),
                                            Seq("flink3", 22, timestamp, 12.236, 0),
                                            Seq("flink4", 22, timestamp, 12.236, 0),
                                            Seq("flink5", 27, timestamp, 17.236, 0)))

    // 执行批量更新
    this.fire.jdbcBatchUpdate(s"update $tableName set sex=? where id=?", Seq(Seq(1, 1), Seq(2, 2), Seq(3, 3), Seq(4, 4), Seq(5, 5), Seq(6, 6)))

    // 方式一：通过this.fire方式执行delete操作
    val sql = s"DELETE FROM $tableName WHERE id=?"
    this.fire.jdbcUpdate(sql, Seq(2))
    // 方式二：通过JdbcConnector.executeUpdate

    // 同一个事务
    /*val connection = this.jdbc.getConnection()
    this.fire.jdbcBatchUpdate("insert", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("delete", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("update", connection = connection, commit = true, closeConnection = true)*/
}

  /**
   * 将DataFrame数据写入到关系型数据库中
   */
  def testDataFrameSave: Unit = {
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])

    val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    // 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序，batch用于指定批次大小，默认取spark.db.jdbc.batch.size配置的值
    df.jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 100)

    df.createOrReplaceTempViewCache("student")
    val sqlDF = this.fire.sql("select name, age, createTime from student where id>=1").repartition(1)
    // 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
    sqlDF.jdbcBatchUpdate("insert into spark_test(name, age, createTime) values(?, ?, ?)")
    // 等同以上方式
    // this.fire.jdbcBatchUpdateDF(sqlDF, "insert into spark_test(name, age, createTime) values(?, ?, ?)")
  }
```

#### [2.2 flink任务](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/stream/JdbcTest.scala)

```scala
/**
   * table的jdbc sink
   */
def testTableJdbcSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select name, age, createTime, length, sex from student group by name, age, createTime, length, sex")

    // 方式一、table中的列顺序和类型需与jdbc sql中的占位符顺序保持一致
    table.jdbcBatchUpdate(sql(this.tableName)).setParallelism(1)
    // 或者
    this.fire.jdbcBatchUpdateTable(table, sql(this.tableName), keyNum = 6).setParallelism(1)

    // 方式二、自定义row取数规则，适用于row中的列个数和顺序与sql占位符不一致的情况
    table.jdbcBatchUpdate2(sql(this.tableName), flushInterval = 10000, keyNum = 7)(row => {
        Seq(row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4))
    })
    // 或者
    this.flink.jdbcBatchUpdateTable2(table, sql(this.tableName), keyNum = 8)(row => {
        Seq(row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4))
    }).setParallelism(1)
}

/**
   * stream jdbc sink
   */
def testStreamJdbcSink(stream: DataStream[Student]): Unit = {
    // 方式一、指定字段列表，内部根据反射，自动获取DataStream中的数据并填充到sql中的占位符
    // 此处fields有两层含义：1. sql中的字段顺序（对应表） 2. DataStream中的JavaBean字段数据（对应JavaBean）
    // 注：要保证DataStream中字段名称是JavaBean的名称，非表中字段名称 顺序要与占位符顺序一致，个数也要一致
    stream.jdbcBatchUpdate(sql(this.tableName2), fields).setParallelism(3)
    // 或者
    this.fire.jdbcBatchUpdateStream(stream, sql(this.tableName2), fields, keyNum = 6).setParallelism(1)

    // 方式二、通过用户指定的匿名函数方式进行数据的组装，适用于上面方法无法反射获取值的情况，适用面更广
    stream.jdbcBatchUpdate2(sql(this.tableName2), 3, 30000, keyNum = 7) {
        // 在此处指定取数逻辑，定义如何将dstream中每列数据映射到sql中的占位符
        value => Seq(value.getName, value.getAge, DateFormatUtils.formatCurrentDateTime(), value.getLength, value.getSex)
    }.setParallelism(1)

    // 或者
    this.flink.jdbcBatchUpdateStream2(stream, sql(this.tableName2), keyNum = 8) {
        value => Seq(value.getName, value.getAge, DateFormatUtils.formatCurrentDateTime(), value.getLength, value.getSex)
    }.setParallelism(2)
}
```

### 三、多个数据源读写

fire框架支持同一个任务中读写任意个数的数据源，只需要通过keyNum指定即可。配置和使用方式可以参考：HBase、kafka等。