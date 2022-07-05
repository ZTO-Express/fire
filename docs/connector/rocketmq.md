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

# RocketMQ消息接入

### 一、API使用

使用fire框架可以很方便的消费rocketmq中的数据，并且支持在同一任务中消费多个rocketmq集群的多个topic。核心代码仅一行：

```scala
// Spark Streaming或flink streaming任务
val dstream = this.fire.createRocketMqPullStream()
```

以上的api均支持rocketmq相关参数的传入，但fire推荐将这些集群信息放到配置文件中，增强代码可读性，提高代码简洁性与灵活性。

### 二、flink sql connector

```scala
this.fire.sql("""
                    |CREATE table source (
                    |  id bigint,
                    |  name string,
                    |  age int,
                    |  length double,
                    |  data DECIMAL(10, 5)
                    |) WITH
                    |   (
                    |   'connector' = 'fire-rocketmq',
                    |   'format' = 'json',
                    |   'rocket.brokers.name' = 'zms',
                    |   'rocket.topics'       = 'fire',
                    |   'rocket.group.id'     = 'fire',
                    |   'rocket.consumer.tag' = '*'
                    |   )
                    |""".stripMargin)
```

**with参数的使用：**

​		rocketmq sql connector中的with参数复用了api中的配置参数，如果需要进行rocketmq-client相关参数设置，可以以rocket.conf.为前缀，后面跟上rocketmq调优参数即可。

### 二、RocketMQ配置

#### 2.1 基于注解

```scala
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire", tag = "*")
@RocketMQ2(brokers = "bigdata_test", topics = "fire2", groupId = "fire2", tag = "*", startingOffset = "latest")
```

#### 2.2 基于配置文件

```properties
spark.rocket.brokers.name													=			rocketmq01:9876;rocketmq02:9876
spark.rocket.topics																=			topic_name
spark.rocket.group.id															=			groupId
spark.rocket.pull.max.speed.per.partition					=			15000
spark.rocket.consumer.tag													=			*
# 以spark.rocket.conf开头的配置支持所有rocket client的配置
spark.rocket.conf.pull.max.speed.per.partition		=   	5000
```

### 三、多RocketMQ多topic消费

​		实际生产场景下，会有同一个任务消费多个RocketMQ集群，多个topic的情况。面对这种需求，fire是如何应对的呢？fire框架约定，配置的key后缀区分不同的RocketMQ配置项，详见以下配置列表：

```properties
# 以下配置中指定了两个RocketMQ集群信息
spark.rocket.brokers.name													=			localhost:9876;localhost02:9876
spark.rocket.topics																=			topic_name
spark.rocket.consumer.instance										=			FireFramework
spark.rocket.group.id															=			groupId

# 注意key的数字后缀，对应代码中的keyNum=2
spark.rocket.brokers.name2												=			localhost:9876;localhost02:9876
spark.rocket.topics2															=			topic_name2
spark.rocket.consumer.instance2										=			FireFramework
spark.rocket.group.id2														=			groupId2
```

那么，代码中是如何关联带有数字后缀的key的呢？答案是通过keyNum参数来指定：

```scala
// 对应spark.rocket.brokers.name这个RocketMQ集群
val dstream = this.fire.createRocketMqPullStream(keyNum=1)
// 对应spark.rocket.brokers.name2这个RocketMQ集群
val dstream2 = this.fire.createRocketMqPullStream(keyNum=2)
```

### 四、RocketMQ-client参数调优

有时，需要对RocketMQ消费进行client端的调优，fire支持所有的RocketMQ-client参数，这些参数只需要添加到配置文件中即可生效：

```properties
# 以spark.rocket.conf开头的配置支持所有rocket client的配置
spark.rocket.conf.pull.max.speed.per.partition		=   5000
```

### 五、offset提交

#### 5.1 主动提交

```scala
dstream.rocketCommitOffsets()
```

#### 5.2 自动提交

​		spark streaming在处理数据过程中，由于offset提交与数据处理可能不再一个算子中，就会出现stage失败，数据丢失，但offset却提交了。为了解决这个问题，fire框架提供了***foreachRDDAtLeastOnce***算子，来保证计算的数据不丢，失败重试（默认3次），成功自动提交等特性。

```scala
@Streaming(20) // spark streaming的批次时间
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire", tag = "*")
object AtLeastOnceTest extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createRocketMqPullStream()

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

### 五、代码示例

[1. spark示例代码](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/streaming/RocketTest.scala)

[2. flink streaming示例代码](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/rocketmq/RocketTest.scala)

[3. flink sql connector示例代码](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/rocketmq/RocketMQConnectorTest.scala)

### 六、@RocketMQ

```java
/**
 * rocketmq集群连接信息
 */
String brokers();

/**
 * rocketmq topics，多个使用逗号分隔
 */
String topics();

/**
 * 消费者标识
 */
String groupId();

/**
 * 指定消费的tag
 */
String tag() default "*";

/**
 * 指定从何处开始消费
 */
String startingOffset() default "";

/**
 * 是否开启主动提交offset
 */
boolean autoCommit() default false;

/**
 * RocketMQ-client参数，以key=value形式注明
 */
String[] config() default "";
```

### 七、配置参数

| 参数名称                            | 引擎  | 含义                                                         |
| ----------------------------------- | ----- | ------------------------------------------------------------ |
| fire.rocket.cluster.map.            | 通用  | 用于配置rocketmq集群别名                                     |
| rocket.conf.                        | 通用  | 通过约定固定的前缀，支持rocketmq-client的所有参数            |
| rocket.brokers.name                 | 通用  | nameserver 地址或别名                                        |
| rocket.topics                       | 通用  | 主题名称                                                     |
| rocket.group.id                     | 通用  | 消费者id                                                     |
| rocket.failOnDataLoss               | 通用  | 丢失数据是否失败                                             |
| rocket.forceSpecial                 | 通用  | 如果 forceSpecial 为true，rocketmq 无论如何都会从特定的可用偏移量开始消费 |
| rocket.enable.auto.commit           | 通用  | 是否自动提交offset                                           |
| rocket.starting.offsets             | 通用  | RocketMQ起始消费位点                                         |
| rocket.consumer.tag                 | 通用  | rocketMq订阅的tag                                            |
| rocket.pull.max.speed.per.partition | spark | 每次拉取每个partition的消息数                                |
| rocket.consumer.instance            | spark | 用于区分不同的消费者实例                                     |
| ocket.sink.parallelism              | flink | sink的并行度                                                 |

