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

### Flink clickhouse connector

#### 一、DDL

```scala
this.fire.sql(
"""
|CREATE TABLE t_user (
|    `id` BIGINT,
|    `name` STRING,
|    `age` INT,
|    `sex` STRING,
|    `score` DECIMAL,
|    `birthday` TIMESTAMP
|) WITH (
|    'connector' = 'clickhouse',
|    'url' = 'jdbc:clickhouse://node01:8123,node02:8123,node03:8123',
|    'database-name' = 'study',
|    'username' = 'fire',
|    'password' = 'fire',
|    'use-local' = 'true', -- 指定为true，当分布式表写入时写的是本地表
|    'table-name' = 't_student',
|    'sink.batch-size' = '10',
|    'sink.flush-interval' = '3',
|    'sink.max-retries' = '3'
|)
|""".stripMargin)
```

#### [二、完整示例](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/clickhouse/ClickhouseTest.scala)

