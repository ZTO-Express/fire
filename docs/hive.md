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

# hive集成与配置

fire框架针对hive提供了更友好的支持方式，程序中可以通过配置文件指定读写的hive集群信息。通过fire框架，可以屏蔽spark或flink任务连接hive的具体细节，只需要通过hive.cluster=xxx即可指定任务如何去连hive。

### 一、spark任务

1. 配置文件

```properties
# 方式一：直接指定hive的thrift server地址，多个以逗号分隔
spark.hive.cluster=thrift://localhost:9083,thrift://localhost02:9083

# 方式二（推荐）：如果已经通过fire.hive.cluster.map.xxx指定了别名，则可以直接使用别名
# 公共信息特别是集群信息建议放到commons.properties中
fire.hive.cluster.map.batch=thrift://localhost:9083,thrift://localhost02:9083
# batch是上述url的hive别名，支持约定多个hive集群的别名
spark.hive.cluster=batch
```

2. [示例代码](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hive/HiveClusterReader.scala)

```scala
// 通过上述配置，代码中就可以直接通过以下方式连接指定的hive
this.fire.sql("select * from hive.tableName").show
```

3. NameNode HA

有时，hadoop集群维护，可能会导致那些读写hive的spark streaming任务挂掉。为了提高灵活性，避免将core-site.xml与hdfs-site.xml放到工程的resources目录下，fire提供了配置的方式，将Name Node HA信息通过配置文件进行指定。每项配置中的batch对应fire.hive.cluster.map.batch所指定的别名：batch，其他信息根据集群不同进行单独配置。如果有多个hive集群，可以配置多套HA配置。

```properties
# 用于是否启用HDFS HA
spark.hdfs.ha.enable=true
# 离线hive集群的HDFS HA配置项，规则为统一的ha前缀：spark.hdfs.ha.conf.+hive.cluster名称+hdfs专门的ha配置
spark.hdfs.ha.conf.batch.fs.defaultFS=hdfs://nameservice1
spark.hdfs.ha.conf.batch.dfs.nameservices=nameservice1
spark.hdfs.ha.conf.batch.dfs.ha.namenodes.nameservice1=namenode5231,namenode5229
spark.hdfs.ha.conf.batch.dfs.namenode.rpc-address.nameservice1.namenode5231=       192.168.1.1:8020
spark.hdfs.ha.conf.batch.dfs.namenode.rpc-address.nameservice1.namenode5229=       192.168.1.2:8020
spark.hdfs.ha.conf.batch.dfs.client.failover.proxy.provider.nameservice1=       org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
```

4. hive参数设置

```properties
# 以spark.hive.conf.为前缀的配置将直接生效，比如开启hive动态分区
# 原理是被直接执行：this.fire.sql("set hive.exec.dynamic.partition=true")
spark.hive.conf.hive.exec.dynamic.partition=true
spark.hive.conf.hive.exec.dynamic.partition.mode=nonstrict
spark.hive.conf.hive.exec.max.dynamic.partitions=5000
```

### 二、flink任务

1. 配置文件

```properties
# 方式一：最简配置，指定hive-site.xml所在的目录的绝对路径
flink.hive.cluster=/path/to/hive-site.xml_path/

# 方式二（推荐）：通过flink.fire.hive.site.path.map.前缀指定别名
flink.fire.hive.site.path.map.test=/tmp/hive/
# 此处的test为hive-site.xml所在目录的别名，引用flink.fire.hive.site.path.map.test，建议放到commons.properties中统一约定
flink.hive.cluster=test
```

2. [示例代码](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/stream/FlinkHiveTest.scala)

```scala
this.fire.sql("select name from hive.tableName group by name")
```

3. 个性化配置

```properties
# flink所集成的hive版本号
flink.hive.version                          =       1.1.0
# 默认的hive数据库
flink.default.database.name                 =       tmp
# 默认的hive分区字段名称
flink.default.table.partition.name          =       ds
# hive的catalog名称
flink.hive.catalog.name                     =       hive
```

