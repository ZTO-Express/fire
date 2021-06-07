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

# Kafka消息接入

### 一、API使用

使用fire框架可以很方便的消费kafka中的数据，并且支持在同一任务中消费多个kafka集群的多个topic。核心代码仅一行：

```scala
// Spark Streaming任务
val dstream = this.fire.createKafkaDirectStream()
// structured streaming任务
val kafkaDataset = this.fire.loadKafkaParseJson()
// flink 任务
val dstream = this.fire.createKafkaDirectStream()
```

以上的api均支持kafka相关参数的传入，但fire推荐将这些集群信息放到配置文件中，增强代码可读性，提高代码简洁性与灵活性。

### 二、kafka配置

你可能会疑惑，kafka的broker与topic信息并没有在代码中指定，程序是如何消费的呢？其实，这些信息都放到了任务同名的配置文件中。当然，你可以选择将这些kafka信息放到代码中指定。如果代码中指定了集群信息，同时配置文件中也有指定，则配置文件的优先级更高。

```properties
spark.kafka.brokers.name           =       localhost:9092,localhost02:9092
# 必须配置项：kafka的topic列表，以逗号分隔
spark.kafka.topics                 =       topic_name
# 用于指定groupId，如果不指定，则默认为当前类名
spark.kafka.group.id               =       fire
```

### 三、多kafka多topic消费

实际生产场景下，会有同一个任务消费多个kafka集群，多个topic的情况。面对这种需求，fire是如何应对的呢？fire框架约定，配置的key后缀区分不同的kafka配置项，详见以下配置列表：

```properties
# 以下配置中指定了两个kafka集群信息
spark.kafka.brokers.name           =       localhost:9092,localhost02:9092
# 多个topic以逗号分隔
spark.kafka.topics                 =       topic_name,topic_name02
spark.kafka.group.id               =       fire
# 注意key的数字后缀，对应代码中的keyNum=2
spark.kafka.brokers.name2          =       localhost:9092,localhost02:9092
spark.kafka.topics2                =       fire,flink
spark.kafka.group.id2              =       fire
```

那么，代码中是如何关联带有数字后缀的key的呢？答案是通过keyNum参数来指定：

```scala
// 对应spark.kafka.brokers.name这个kafka集群
val dstream = this.fire.createKafkaDirectStream(keyNum=1)
// 对应spark.kafka.brokers.name2这个kafka集群
val dstream2 = this.fire.createKafkaDirectStream(keyNum=2)
```

### 三、offset提交

```scala
dstream.kafkaCommitOffsets()
```

### 四、kafka-client参数调优

有时，需要对kafka消费进行client端的调优，fire支持所有的kafka-client参数，这些参数只需要添加到配置文件中即可生效：

```properties
# 用于配置启动时的消费位点，默认取最新
spark.kafka.starting.offsets		  =		  latest
# 数据丢失时执行失败
spark.kafka.failOnDataLoss			  =		  true
# 是否启用自动commit
spark.kafka.enable.auto.commit        =		  false
# 以spark.kafka.conf开头的配置支持所有kafka client的配置
spark.kafka.conf.session.timeout.ms   =       300000
spark.kafka.conf.request.timeout.ms   =       400000
spark.kafka.conf.session.timeout.ms2  =       300000
```

### 五、代码示例

[1. spark消费kafka demo](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/streaming/KafkaTest.scala)

[2. flink消费kafka demo](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/stream/HBaseTest.scala)

