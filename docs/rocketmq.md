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

rocketmq sql connector中的with参数复用了api中的配置参数，如果需要进行rocketmq-client相关参数设置，可以以rocket.conf.为前缀，后面跟上rocketmq调优参数即可。

### 二、RocketMQ配置

```properties
spark.rocket.brokers.name							=	localhost:9876;localhost02:9876
spark.rocket.topics									=	topic_name
spark.rocket.consumer.instance						=	FireFramework
spark.rocket.group.id								=	groupId
spark.rocket.pull.max.speed.per.partition			=	15000
spark.rocket.consumer.tag							=	1||2||3||4||5||8||44||45
# 以spark.rocket.conf开头的配置支持所有rocket client的配置
#spark.rocket.conf.pull.max.speed.per.partition		=   5000
```

### 三、多RocketMQ多topic消费

实际生产场景下，会有同一个任务消费多个RocketMQ集群，多个topic的情况。面对这种需求，fire是如何应对的呢？fire框架约定，配置的key后缀区分不同的RocketMQ配置项，详见以下配置列表：

```properties
# 以下配置中指定了两个RocketMQ集群信息
spark.rocket.brokers.name							=	localhost:9876;localhost02:9876
spark.rocket.topics									=	topic_name
spark.rocket.consumer.instance						=	FireFramework
spark.rocket.group.id								=	groupId
# 注意key的数字后缀，对应代码中的keyNum=2
spark.rocket.brokers.name2							=	localhost:9876;localhost02:9876
spark.rocket.topics2								=	topic_name2
spark.rocket.consumer.instance2						=	FireFramework
spark.rocket.group.id2								=	groupId2
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

### 五、代码示例

[1. spark示例代码](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/streaming/RocketTest.scala)

[2. flink streaming示例代码](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/rocketmq/RocketTest.scala)

[3. flink sql connector示例代码](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/rocketmq/RocketMQConnectorTest.scala)

