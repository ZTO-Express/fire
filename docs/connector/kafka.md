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

# Kafka 数据源

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

#### 2.1 定义别名

　　建议将kafka集群url信息定义成别名，别名定义放到名为common.properties的配置文件中。别名的好处是一处维护到处生效，方便共用，便于记忆。

```properties
# 以下定义了两个kafka集群的别名，分别叫mq和test，别名与定义的url对应
fire.kafka.cluster.map.mq					=				kafka01:9092,kafka02:9092,kafka03:9092
fire.kafka.cluster.map.test				=				kafka-test01:9092,kafka-test02:9092,kafka-test03:9092
```

#### 2.2 基于注解配置

定义好别名以后，就可以使用注解的方式去配置kafka集群信息了，fire框架支持一个任务读写多个kafka：

```scala
@Kafka(brokers = "mq", topics = "fire", groupId = "fire")
@Kafka2(brokers = "test", topics = "fire", groupId = "fire", sessionTimeout = 600000, autoCommit = false)
```

#### 2.3 基于配置文件配置

```properties
spark.kafka.brokers.name           =       mq
# 必须配置项：kafka的topic列表，以逗号分隔
spark.kafka.topics                 =       fire
# 用于指定groupId，如果不指定，则默认为当前类名
spark.kafka.group.id               =       fire

# 配置消费明为test的kafka机器，注意key的后缀统一添加2，用于标识不同的kafka集群
spark.kafka.brokers.name2          =       test
# 必须配置项：kafka的topic列表，以逗号分隔
spark.kafka.topics2                =       fire
# 用于指定groupId，如果不指定，则默认为当前类名
spark.kafka.group.id2              =       fire
```

### 三、多kafka多topic消费

代码中是如何关联带有数字后缀的key的呢？答案是通过keyNum参数来指定：

```scala
// 对应spark.kafka.brokers.name=mq 或 @Kafka这个kafka("mq")集群，如果不知道keyNum，默认为1
val dstream = this.fire.createKafkaDirectStream()
// 对应spark.kafka.brokers.name2=test 或 @Kafka2("test")这个kafka集群
val dstream2 = this.fire.createKafkaDirectStream(keyNum=2)
```

### 三、offset提交

#### 3.1 主动提交

```scala
dstream.kafkaCommitOffsets()
```

#### 3.2 自动提交

　　Spark streaming在处理数据过程中，由于offset提交与数据处理可能不再一个算子中，就会出现stage失败，数据丢失，但offset却提交了。为了解决这个问题，fire框架提供了***foreachRDDAtLeastOnce***算子，来保证计算的数据不丢，失败重试（默认3次），成功自动提交等特性。

```scala
@Streaming(20) // spark streaming的批次时间
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object AtLeastOnceTest extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()

    // 至少一次的语义保证，处理成功自动提交offset，处理失败会重试指定次数，如果仍失败则任务退出
    dstream.foreachRDDAtLeastOnce(rdd => {
      val studentRDD = rdd.map(t => JSONUtils.parseObject[Student](t.value())).repartition(2)
      val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
      println("kafka.brokers.name=>" + this.conf.getString("kafka.brokers.name"))
      studentRDD.toDF().jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 1)
    })

    this.fire.start
  }
}
```

### 五、kafka-client参数调优

针对与kafka-client个性化的参数，需要使用config来进行配置：

```scala
@Kafka(brokers = "kafka01:9092", config = Array[String]("session.timeout.ms=30000", "request.timeout.ms=30000"))
```

基于配置文件的话使用kafka.conf开头加上kafka-client参数即可：

```properties
# 以kafka.conf开头的配置支持所有kafka client的配置
kafka.conf.session.timeout.ms   =     300000
kafka.conf.request.timeout.ms   =     400000
kafka.conf.session.timeout.ms2  =     300000
```

### 六、代码示例

[1. spark消费kafka demo](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/streaming/KafkaTest.scala)

[2. flink消费kafka demo](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/stream/HBaseTest.scala)

### 七、@Kafka注解

```java
/**
 * kafka集群连接信息，同value
 */
String brokers();

/**
 * kafka topics，多个使用逗号分隔
 */
String topics();

/**
 * 消费者标识
 */
String groupId();

/**
 * 指定从何处开始消费
 */
String startingOffset() default "";

/**
 * 指定消费到何处结束
 */
String endingOffsets() default "";

/**
 * 是否开启主动提交offset
 */
boolean autoCommit() default false;

/**
 * session超时时间（ms）
 */
long sessionTimeout() default -1;

/**
 * request超时时间（ms）
 */
long requestTimeout() default -1;

/**
 * poll的周期（ms）
 */
long pollInterval() default -1;

/**
 * 从指定的时间戳开始消费
 */
long startFromTimestamp() default -1;

/**
 * 指定从kafka中保持的offset开始继续消费
 */
boolean startFromGroupOffsets() default false;

/**
 * 是否强制覆盖checkpoint中保持的offset信息，从指定位置开始消费
 */
boolean forceOverwriteStateOffset() default false;

/**
 * 是否在开启checkpoint的情况下强制周期性提交offset到kafka
 */
boolean forceAutoCommit() default false;

/**
 * 强制提交的周期（ms）
 */
long forceAutoCommitInterval() default -1;

/**
 * kafka-client参数，以key=value形式注明
 */
String[] config() default "";
```

### 八、配置参数

| 参数名称                                 | 引擎  | 含义                                                         |
| ---------------------------------------- | ----- | ------------------------------------------------------------ |
| fire.kafka.cluster.map.                  | 通用  | 用于定义kafka集群别名                                        |
| kafka.conf.                              | 通用  | 用于设置kafka-client参数                                     |
| kafka.brokers.name                       | 通用  | 指定消费的kafka集群url或别名                                 |
| kafka.topics                             | 通用  | kafka的topic列表，以逗号分隔                                 |
| kafka.group.id                           | 通用  | 消费kafka的group id                                          |
| kafka.starting.offsets                   | 通用  | kafka起始消费位点                                            |
| kafka.ending.offsets                     | 通用  | kafka结束消费位点                                            |
| kafka.enable.auto.commit                 | 通用  | 是否自动维护offset                                           |
| kafka.failOnDataLoss                     | 通用  | 丢失数据是否失败                                             |
| kafka.session.timeout.ms                 | 通用  | kafka session超时时间                                        |
| kafka.request.timeout.ms                 | 通用  | kafka request超时时间                                        |
| kafka.max.poll.interval.ms               | 通用  | kafka的最大poll周期                                          |
| kafka.CommitOffsetsOnCheckpoints         | flink | 当checkpoint时是否提交offset                                 |
| kafka.StartFromTimestamp                 | flink | 从指定时间戳开始消费                                         |
| kafka.StartFromGroupOffsets              | flink | 从指定offset开始消费                                         |
| kafka.force.overwrite.stateOffset.enable | flink | 是否使状态中存放的offset不生效（请谨慎配置，用于kafka集群迁移等不正常状况的运维） |
| kafka.force.autoCommit.enable            | flink | 是否在开启checkpoint的情况下强制开启周期性offset提交         |
| kafka.force.autoCommit.Interval          | Flink | 周期性提交offset的时间间隔（ms）                             |

