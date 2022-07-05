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

​		使用fire框架，仅需一行配置即可实现spark&flink与hive的无缝读写，甚至支持跨集群的读写（实时任务与hive不再同一个集群中）。

### 一、基于注解

```scala
// 指定thrift server url地址，多个以逗号分隔
@Hive("thrift://thrift01:9083")
// 指定thrift对应的别名，别名配置方式同kafka别名配置
@Hive("test")
```

### 二、配置文件

```properties
# 方式一：直接指定hive的thrift server地址，多个以逗号分隔
spark.hive.cluster																												=				thrift://hive01:9083,thrift://hive02:9083

# 方式二（推荐）：如果已经通过fire.hive.cluster.map.xxx指定了别名，则可以直接使用别名
# 公共信息特别是集群信息建议放到commons.properties中
fire.hive.cluster.map.batch																								=				thrift://hive03:9083,thrift://hive04:9083
# batch是上述url的hive别名，支持约定多个hive集群的别名
spark.hive.cluster																												=				batch
```

### 三、[示例代码](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hive/HiveClusterReader.scala)

```scala
// 通过上述配置，代码中就可以直接通过以下方式连接指定的hive
this.fire.sql("select * from hive.tableName").show
```

### 四、高可用

​		NameNode主备切换会导致那些读写hive的spark streaming任务挂掉。为了提高灵活性，避免将core-site.xml与hdfs-site.xml放到工程的resources目录下，fire提供了配置的方式，将Name Node HA信息通过配置文件进行指定。每项配置中的batch对应fire.hive.cluster.map.batch所指定的别名：batch，其他信息根据集群不同进行单独配置。如果有多个hive集群，可以配置多套HA配置。

```properties
# 用于是否启用HDFS HA
spark.hdfs.ha.enable																												=				true
# 离线hive集群的HDFS HA配置项，规则为统一的ha前缀：spark.hdfs.ha.conf.+hive.cluster名称+hdfs专门的ha配置
spark.hdfs.ha.conf.batch.fs.defaultFS																				=				hdfs://nameservice1
spark.hdfs.ha.conf.batch.dfs.nameservices																		=				nameservice1
spark.hdfs.ha.conf.batch.dfs.ha.namenodes.nameservice1											=				namenode5231,namenode5229
spark.hdfs.ha.conf.batch.dfs.namenode.rpc-address.nameservice1.namenode5231	=				namenode01:8020
spark.hdfs.ha.conf.batch.dfs.namenode.rpc-address.nameservice1.namenode5229	=				namenode02:8020
spark.hdfs.ha.conf.batch.dfs.client.failover.proxy.provider.nameservice1		=       org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
```

### 五、hive set参数

```properties
# 以spark.hive.conf.为前缀的配置将直接生效，比如开启hive动态分区
# 原理是被直接执行：this.fire.sql("set hive.exec.dynamic.partition=true")
spark.hive.conf.hive.exec.dynamic.partition																	=				true
spark.hive.conf.hive.exec.dynamic.partition.mode														=				nonstrict
spark.hive.conf.hive.exec.max.dynamic.partitions														=				5000
```

### 六、@Hive注解

```java
/**
 * hive连接别名：hive.cluster
 */
String value() default "";

/**
 * hive连接别名：hive.cluster，同value
 */
String cluster() default "";

/**
 * hive的版本：hive.version
 */
String version() default "";

/**
 * 在flink中hive的catalog名称：hive.catalog.name
 */
String catalog() default "";

/**
 * 分区名称（dt、ds）：default.table.partition.name
 */
String partition() default "";
```

### 七、个性化配置

| 参数名称                           | 引擎  | 含义                               |
| ---------------------------------- | ----- | ---------------------------------- |
| flink.hive.version                 | flink | flink所集成的hive版本号            |
| flink.default.database.name        | flink | tmp                                |
| flink.default.table.partition.name | flink | 默认的hive分区字段名称             |
| flink.hive.catalog.name            | flink | hive的catalog名称                  |
| fire.hive.cluster.map.             | 通用  | hive thrift url别名映射            |
| hive.conf.                         | 通用  | 通过固定的前缀配置支持所有hive参数 |

