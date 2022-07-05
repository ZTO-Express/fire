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

# fire框架参数

​		fire框架提供了很多参数，这些参数为个性化调优带来了很大的灵活性。参数大体分为：**fire框架参数**（*fire.properties*）、**spark引擎参数**（*spark.properties*）、**flink引擎参数**（*flink.properties*）、**kafka参数**、**hbase参数**等。详见以下列表：

# 一、fire框架参数

| 参数                                                | 默认值              | 含义                                                         | 生效版本 | 是否废弃 |
| --------------------------------------------------- | ------------------- | ------------------------------------------------------------ | -------- | -------- |
| fire.thread.pool.size                               | 5                   | fire内置线程池大小                                           | 0.4.0    | 否       |
| fire.thread.pool.schedule.size                      | 5                   | fire内置定时任务线程池大小                                   | 0.4.0    | 否       |
| fire.rest.enable                                    | true                | 用于配置是否启用fire框架内置的restful服务，可用于与平台系统做集成。 | 0.3.0    | 否       |
| fire.conf.show.enable                               | true                | 是否打印非敏感的配置信息                                     | 0.1.0    | 否       |
| fire.rest.url.show.enable                           | false               | 是否在日志中打印fire框架restful服务地址                      | 0.3.0    | 否       |
| fire.rest.url.hostname                              | false               | 是否启用hostname作为rest服务的访问地址                       | 2.0.0    | 否       |
| fire.acc.enable                                     | true                | 是否启用fire框架内置的所有累加器                             | 0.4.0    | 否       |
| fire.acc.log.enable                                 | true                | 是否启用fire框架日志累加器                                   | 0.4.0    | 否       |
| fire.acc.multi.counter.enable                       | true                | 是否启用多值累加器                                           | 0.4.0    | 否       |
| fire.acc.multi.timer.enable                         | true                | 是否启用时间维度累加器                                       | 0.4.0    | 否       |
| fire.log.enable                                     | true                | fire框架埋点日志开关，关闭以后将不再打印埋点日志             | 0.4.0    | 否       |
| fire.log.sql.length                                 | 100                 | 用于限定fire框架中sql日志的字符串长度                        | 0.4.1    | 否       |
| fire.jdbc.storage.level                             | memory_and_disk_ser | fire框架针对jdbc操作后数据集的缓存策略，避免重复查询数据库   | 0.4.0    | 否       |
| fire.jdbc.query.partitions                          | 10                  | 通过JdbcConnector查询后将数据集放到多少个分区中，需根据实际的结果集做配置 | 0.3.0    | 否       |
| fire.task.schedule.enable                           | true                | 是否启用fire框架定时任务，基于quartz实现                     | 0.4.0    | 否       |
| fire.dynamic.conf.enable                            | true                | 是否启用动态配置功能，fire框架允许在运行时更新用户配置信息，比如：rdd.repartition(this.conf.getInt(count))，此处可实现动态的改变分区大小，实现动态调优。 | 0.4.0    | 否       |
| fire.restful.max.thread                             | 8                   | fire框架rest接口服务最大线程数，如果平台调用fire接口比较频繁，建议调大。 | 0.4.0    | 否       |
| fire.quartz.max.thread                              | 8                   | quartz最大线程池大小，如果任务中的定时任务比较多，建议调大。 | 0.4.0    | 否       |
| fire.acc.log.min.size                               | 500                 | 收集日志记录保留的最小条数。                                 | 0.4.0    | 否       |
| fire.acc.log.max.size                               | 1000                | 收集日志记录保留的最大条数。                                 | 0.4.0    | 否       |
| fire.acc.timer.max.size                             | 1000                | timer累加器保留最大的记录数                                  | 0.4.0    | 否       |
| fire.acc.timer.max.hour                             | 12                  | timer累加器清理几小时之前的记录                              | 0.4.0    | 否       |
| fire.acc.env.enable                                 | true                | env累加器开关                                                | 0.4.0    | 否       |
| fire.acc.env.max.size                               | 500                 | env累加器保留最多的记录数                                    | 0.4.0    | 否       |
| fire.acc.env.min.size                               | 100                 | env累加器保留最少的记录数                                    | 0.4.0    | 否       |
| fire.scheduler.blacklist                            |                     | 定时调度任务黑名单，配置的value为定时任务方法名，多个以逗号分隔，配置黑名单的方法将不会被quartz定时调度。 | 0.4.1    | 否       |
| fire.conf.print.blacklist                           | .map.,pass,secret   | 配置打印黑名单，含有配置中指定的片段将不会被打印，也不会被展示到spark&flink的webui中。 | 0.4.2    | 否       |
| fire.restful.port.retry_num                         | 3                   | 启用fire restserver可能会因为端口冲突导致失败，通过该参数可允许fire重试几次。 | 1.0.0    | 否       |
| fire.restful.port.retry_duration                    | 1000                | 端口重试间隔时间（ms）                                       | 1.0.0    | 否       |
| fire.log.level.conf.org.apache.spark                | info                | 用于设置某个包的日志级别，默认将spark包所有的类日志级别设置为info | 1.0.0    | 否       |
| fire.deploy_conf.enable                             | true                | 是否进行累加器的分布式初始化                                 | 0.4.0    | 否       |
| fire.exception_bus.size                             | 1000                | 用于限制每个jvm实例内部queue用于存放异常对象数最大大小，避免队列过大造成内存溢出 | 2.0.0    | 否       |
| fire.buried_point.datasource.enable                 | true                | 是否开启数据源埋点，开启后fire将自动采集任务用到的数据源信息（kafka、jdbc、hbase、hive等）。 | 2.0.0    | 否       |
| fire.buried_point.datasource.max.size               | 200                 | 用于存放埋点的队列最大大小，超过该大小将会被丢弃             | 2.0.0    | 否       |
| fire.buried_point.datasource.initialDelay           | 30                  | 定时解析埋点SQL的初始延迟（s）                               | 2.0.0    | 否       |
| fire.buried_point.datasource.period                 | 60                  | 定时解析埋点SQL的执行频率（s）                               | 2.0.0    | 否       |
| fire.buried_point.datasource.map.tidb               | 4000                | 用于jdbc url的识别，当无法通过driver class识别数据源时，将从url中的端口号进行区分，不同数据配置使用统一的前缀：fire.buried_point.datasource.map. | 2.0.0    | 否       |
| fire.conf.adaptive.prefix                           | true                | 是否开启配置自适应前缀，自动为配置加上引擎前缀（spark.\|flink.） | 2.0.0    | 否       |
| fire.user.common.conf                               | common.properties   | 用户统一配置文件，允许用户在该配置文件中存放公共的配置信息，优先级低于任务配置文件（多个以逗号分隔） | 2.0.0    | 否       |
| fire.shutdown.auto.exit                             | true                | 是否在调用shutdown方法时主动退出jvm进程，如果为true，则执行到this.stop方法，关闭上下文信息，回收线程池后将调用System.exit(0)强制退出进程。 | 2.0.0    | 否       |
| fire.kafka.cluster.map.test                         | ip1:9092,ip2:9092   | kafka集群名称与集群地址映射，便于用户配置中通过别名即可消费指定的kafka。比如：kafka.brokers.name=test则表明消费ip1:9092,ip2:9092这个kafka集群。当然，也支持直接配置url：kafka.brokers.name=ip1:9092,ip2:9092。 | 0.1.0    | 否       |
| fire.hive.default.database.name                     | tmp                 | 默认的hive数据库                                             | 0.1.0    | 否       |
| fire.hive.table.default.partition.name              | ds                  | 默认的hive分区字段名称                                       | 0.1.0    | 否       |
| fire.hive.cluster.map.test                          | thrift://ip:9083    | 测试集群hive metastore地址（别名：test），任务中就可以通过fire.hive.cluster=test这种配置方式指定连接test对应的thrift server地址。 |          |          |
| fire.hbase.batch.size                               | 10000               | 单个线程读写HBase的数据量                                    | 0.1.0    | 否       |
| fire.hbase.storage.level                            | memory_and_disk_ser | fire框架针对hbase操作后数据集的缓存策略，避免因懒加载或其他原因导致的重复读取hbase问题，降低hbase压力。 | 0.3.2    | 否       |
| fire.hbase.scan.partitions                          | -1                  | 通过HBase scan后repartition的分区数，需根据scan后的数据量做配置，-1表示不生效。 | 0.3.2    | 否       |
| fire.hbase.table.exists.cache.enable                | true                | 是否开启HBase表存在判断的缓存，开启后表存在判断将避免大量的connection消耗 | 2.0.0    | 否       |
| fire.hbase.table.exists.cache.reload.enable         | true                | 是否开启HBase表存在列表缓存的定时更新任务，避免hbase表被drop导致报错。 | 2.0.0    | 否       |
| fire.hbase.table.exists.cache.initialDelay          | 60                  | 定时刷新缓存HBase表任务的初始延迟（s）                       | 2.0.0    | 否       |
| fire.hbase.table.exists.cache.period                | 600                 | 定时刷新缓存HBase表任务的执行频率（s）                       | 2.0.0    | 否       |
| fire.hbase.cluster.map.test                         | zk1:2181,zk2:2181   | 测试集群hbase的zk地址（别名：test）                          | 2.0.0    | 否       |
| fire.hbase.conf.hbase.zookeeper.property.clientPort | 2181                | hbase connection 配置，约定以：fire.hbase.conf.开头，比如：fire.hbase.conf.hbase.rpc.timeout对应hbase中的配置为hbase.rpc.timeout | 2.0.0    | 否       |
| fire.config_center.enable                           | true                | 是否在任务启动时从配置中心获取配置文件，以便实现动态覆盖jar包中的配置信息。 | 1.0.0    | 否       |
| fire.config_center.local.enable                     | false               | 本地运行环境下（Windows、Mac）是否调用配置中心接口获取配置信息。 | 1.0.0    | 否       |
| fire.config_center.register.conf.secret             |                     | 配置中心接口调用秘钥                                         | 1.0.0    | 否       |
| fire.config_center.register.conf.prod.address       |                     | 配置中心接口地址                                             | 0.4.1    | 否       |

# 二、Spark引擎参数

| 参数                                                         | 默认值                                                       | 含义                                                         | 生效版本 | 是否废弃 |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | -------- | -------- |
| spark.appName                                                |                                                              | spark的应用名称，为空则取默认获取类名                        | 0.1.0    | 否       |
| spark.local.cores                                            | *                                                            | spark local模式下使用多少core运行，默认为local[*]，自动根据当前pc的cpu核心数设置 | 0.4.1    | 否       |
| spark.chkpoint.dir                                           |                                                              | spark checkpoint目录地址                                     | 0.1.0    | 否       |
| spark.log.level                                              | WARN                                                         | spark的日志级别                                              | 0.1.0    | 否       |
| spark.fire.scheduler.blacklist                               | jvmMonitor                                                   | 定时任务黑名单，指定到@Scheduled所修饰的方法名，多个以逗号分隔。当配置了黑名单后，该定时任务将不会被定时调用。 | 0.4.0    | 否       |
| spark.kafka.group.id                                         | 指定spark消费kafka的groupId                                  | kafka的groupid，为空则取类名                                 | 0.1.0    | 否       |
| spark.kafka.brokers.name                                     |                                                              | 用于配置任务消费的kafka broker地址，如果通过fire.kafka.cluster.map.xxx指定了broker别名，则此处也可以填写别名。 | 0.1.0    | 否       |
| spark.kafka.topics                                           |                                                              | 消费的topic列表，多个以逗号分隔                              | 0.1.0    | 否       |
| spark.kafka.starting.offsets                                 | latest                                                       | 用于配置启动时的消费位点，默认取最新                         | 0.1.0    | 否       |
| spark.kafka.failOnDataLoss                                   | true                                                         | 数据丢失时执行失败                                           | 0.1.0    | 否       |
| spark.kafka.enable.auto.commit                               | false                                                        | 是否启用自动commit kafka的offset                             | 0.4.0    | 否       |
| spark.kafka.conf.xxx                                         |                                                              | 以spark.kafka.conf开头加上kafka参数，则可用于设置kafka相关的参数。比如：spark.kafka.conf.request.timeout.ms对应kafka的request.timeout.ms参数。 | 0.4.0    | 否       |
| spark.hive.cluster                                           |                                                              | 用于配置spark连接的hive thriftserver地址，支持url和别名两种配置方式。别名需要事先通过fire.hive.cluster.map.别名                                                    =       thrift://ip:9083指定。 | 0.1.0    | 否       |
| spark.rocket.cluster.map.别名                                | ip:9876                                                      | rocketmq别名列表                                             | 1.0.0    | 否       |
| spark.rocket.conf.xxx                                        |                                                              | 以spark.rocket.conf开头的配置支持所有rocket client的配置     | 1.0.0    | 否       |
| spark.hdfs.ha.enable                                         | true                                                         | 是否启用hdfs的ha配置，避免将hdfs-site.xml、core-site.xml放到resources中导致多hadoop集群hdfs不灵活的问题。同时也可以避免引namenode维护导致spark任务挂掉的问题。 | 1.0.0    | 否       |
| spark.hdfs.ha.conf.test.fs.defaultFS                         | hdfs://nameservice1                                          | 对应fs.defaultFS，其中test与fire.hive.cluster.map.test中指定的别名test相对应，当通过fire.hive.cluster=test指定读写test这个hive时，namenode的ha将生效。 | 1.0.0    | 否       |
| spark.hdfs.ha.conf.test.dfs.nameservices                     | nameservice1                                                 | 对应dfs.nameservices                                         | 1.0.0    | 否       |
| spark.hdfs.ha.conf.test.dfs.ha.namenodes.nameservice1        | namenode5231,namenode5229                                    | 对应dfs.ha.namenodes.nameservice1                            | 1.0.0    | 否       |
| spark.hdfs.ha.conf.test.dfs.namenode.rpc-address.nameservice1.namenode5231 | ip:8020                                                      | 对应dfs.namenode.rpc-address.nameservice1.namenode5231       | 1.0.0    | 否       |
| spark.hdfs.ha.conf.test.dfs.namenode.rpc-address.nameservice1.namenode5229 | ip2:8020                                                     | 对应dfs.namenode.rpc-address.nameservice1.namenode5229       | 1.0.0    | 否       |
| spark.hdfs.ha.conf.test.dfs.client.failover.proxy.provider.nameservice1 | org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider | 对应dfs.client.failover.proxy.provider.nameservice1          | 1.0.0    | 否       |
| spark.impala.connection.url                                  | jdbc:hive2://ip:21050/;auth=noSasl                           | impala jdbc地址                                              | 0.1.0    | 否       |
| spark.impala.jdbc.driver.class.name                          | org.apache.hive.jdbc.HiveDriver                              | impala jdbc驱动                                              | 0.1.0    | 否       |
| spark.datasource.options.                                    |                                                              | 以此开头的配置将被加载到datasource api的options中            | 2.0.0    | 否       |
| spark.datasource.format                                      |                                                              | datasource api的format                                       | 2.0.0    | 否       |
| spark.datasource.saveMode                                    | Append                                                       | datasource api的saveMode                                     | 2.0.0    | 否       |
| spark.datasource.saveParam                                   |                                                              | 用于dataFrame.write.format.save()参数                        | 2.0.0    | 否       |
| spark.datasource.isSaveTable                                 | false                                                        | 用于决定调用save(path)还是saveAsTable                        | 2.0.0    | 否       |
| spark.datasource.loadParam                                   |                                                              | 用于spark.read.format.load()参数                             | 2.0.0    | 否       |

# 三、Flink引擎参数

| 参数                                             | 默认值                                                       | 含义                                                         | 生效版本 | 是否废弃 |
| ------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | -------- | -------- |
| flink.appName                                    |                                                              | flink的应用名称，为空则取类名                                | 1.0.0    | 否       |
| flink.kafka.group.id                             |                                                              | kafka的groupid，为空则取类名                                 | 1.0.0    | 否       |
| flink.kafka.brokers.name                         |                                                              | 用于配置任务消费的kafka broker地址，如果通过fire.kafka.cluster.map.xxx指定了broker别名，则此处也可以填写别名。 | 1.0.0    | 否       |
| flink.kafka.topics                               |                                                              | 消费的kafka topic列表，多个以逗号分隔                        | 1.0.0    | 否       |
| flink.kafka.starting.offsets                     |                                                              | 用于配置启动时的消费位点，默认取最新                         | 1.0.0    | 否       |
| flink.kafka.failOnDataLoss                       | true                                                         | 数据丢失时执行失败                                           | 1.0.0    | 否       |
| flink.kafka.enable.auto.commit                   | false                                                        | 是否启用自动提交kafka offset                                 | 1.0.0    | 否       |
| flink.kafka.CommitOffsetsOnCheckpoints           | true                                                         | 是否在checkpoint时记录offset值                               | 1.0.0    | 否       |
| flink.kafka.StartFromTimestamp                   | 0                                                            | 设置从指定时间戳位置开始消费kafka                            | 1.0.0    | 否       |
| flink.kafka.StartFromGroupOffsets                | false                                                        | 从topic中指定的group上次消费的位置开始消费，必须配置group.id参数 | 1.0.0    | 否       |
| flink.log.level                                  | WARN                                                         | 默认的日志级别                                               | 1.0.0    | 否       |
| flink.hive.cluster                               |                                                              | 用于配置flink读写的hive集群别名                              | 1.0.0    | 否       |
| flink.hive.version                               |                                                              | 指定hive版本号                                               | 1.0.0    | 否       |
| flink.default.database.name                      | tmp                                                          | 默认的hive数据库                                             | 1.0.0    | 否       |
| flink.default.table.partition.name               | ds                                                           | 默认的hive分区字段名称                                       | 1.0.0    | 否       |
| flink.hive.catalog.name                          | hive                                                         | hive的catalog名称                                            | 1.0.0    | 否       |
| flink.fire.hive.site.path.map.别名               | test                                                         | /path/to/hive-site-path/                                     | 1.0.0    | 否       |
| flink.hbase.cluster                              | test                                                         | 读写的hbase集群zk地址                                        | 1.0.0    | 否       |
| flink.max.parallelism                            |                                                              | 用于配置flink的max parallelism                               | 1.0.0    | 否       |
| flink.default.parallelism                        |                                                              | 用于配置任务默认的parallelism                                | 1.0.0    | 否       |
| flink.stream.checkpoint.interval                 | -1                                                           | checkpoint频率，-1表示关闭                                   | 1.0.0    | 否       |
| flink.stream.checkpoint.mode                     | EXACTLY_ONCE                                                 | checkpoint的模式：EXACTLY_ONCE/AT_LEAST_ONCE                 | 1.0.0    | 否       |
| flink.stream.checkpoint.timeout                  | 600000                                                       | checkpoint超时时间，单位：毫秒                               | 1.0.0    | 否       |
| flink.stream.checkpoint.max.concurrent           | 1                                                            | 同时checkpoint操作的并发数                                   | 1.0.0    | 否       |
| flink.stream.checkpoint.min.pause.between        | 0                                                            | 两次checkpoint的最小停顿时间                                 | 1.0.0    | 否       |
| flink.stream.checkpoint.prefer.recovery          | false                                                        | 如果有更近的checkpoint时，是否将作业回退到该检查点           | 1.0.0    | 否       |
| flink.stream.checkpoint.tolerable.failure.number | 0                                                            | 可容忍checkpoint失败的次数，默认不允许失败                   | 1.0.0    | 否       |
| flink.stream.checkpoint.externalized             | RETAIN_ON_CANCELLATION                                       | 当cancel job时保留checkpoint                                 | 1.0.0    | 否       |
| flink.sql.log.enable                             | false                                                        | 是否打印组装with语句后的flink sql，由于with表达式中可能含有敏感信息，默认为关闭 | 2.0.0    | 否       |
| flink.sql.with.xxx                               | flink.sql.with.connector=jdbc flink.sql.with.url=jdbc:mysql://ip:3306/db | 以flink.sql.with.开头的配置，用于sql语句的with表达式。通过this.fire.sql(sql, keyNum)即可自动读取并映射成with表达式的sql。避免sql中的with表达式硬编码到代码中，提高灵活性。 | 2.0.0    | 否       |
| flink.sql_with.replaceMode.enable                | false                                                        | 是否启用配置文件中with强制替换sql中已有的with表达式，如果启用，则会强制替换掉代码中sql的with列表，达到最大的灵活性。 | 2.0.0    | 否       |
| flink.sql.udf.fireUdf.enable                     | false                                                        | 是否启用fire注册外部udf jar包中的类为发flink sql的udf函数    | 2.0.0    | 否       |
| flink.sql.conf.pipeline.jars                     | /path/to/udf/jar/                                            | 用于指定udf jar包路径                                        | 2.0.0    | 否       |
| flink.sql.udf.conf.xxx                           | 包名+类名                                                    | 用于指定udf函数名称与类名的对应关系，比如函数名为test，包名为com.udf.Udf，则配置为：flink.sql.udf.conf.test=com.udf.Udf | 2.0.0    | 否       |

