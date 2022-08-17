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

### 一、编译与安装

```shell
# git clone https://github.com/ZTO-Express/fire.git
# mvn clean install -DskipTests -Pspark-3.0.2 -Pflink-1.14.3 -Pscala-2.12
```

建议将fire deploy到maven私服，便于每个人去使用。编译fire框架时，可根据实际需求编译成指定版本的spark或flink。官方适配的版本是如下：

| Apache Spark | Apache Spark |
| ------------ | ------------ |
| 2.3.x        | 1.10.x       |
| 2.4.x        | 1.11.x       |
| 3.0.x        | 1.12.x       |
| 3.1.x        | 1.13.x       |
| 3.2.x        | 1.14.x       |
| 3.3.x        | 1.15.x       |

### 二、maven依赖

- [spark项目pom样例](pom/spark-pom.xml)
- [flink项目pom样例](pom/flink-pom.xml)

### 三、开发步骤

Fire框架提供了统一的编码风格，基于这种编码风格，可以很轻松的进行spark或flink代码开发。

```scala
package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.anno.Checkpoint

/**
 * 基于Fire进行Flink Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |# 支持Flink调优参数、Fire框架参数、用户自定义参数等
    |state.checkpoints.num-retained=30
    |state.checkpoints.dir=hdfs:///user/flink/checkpoint
    |""")
@Hive("thrift://localhost:9083") // 配置连接到指定的hive
@Checkpoint(interval = 100, unaligned = true) // 100s做一次checkpoint，开启非对齐checkpoint
@Kafka(brokers = "localhost:9092", topics = "fire", groupId = "fire")
object FlinkDemo extends BaseFlinkStreaming {
	
  /** process方法中编写业务逻辑代码，该方法会被fire框架自动调起 **/
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream() 	// 使用api的方式消费kafka
    this.fire.sql("""create table statement ...""")
    this.fire.sql("""insert into statement ...""")
    this.fire.start
  }
}
```

从以上代码片段中可以看到，引入fire框架大体分为5个步骤：

#### 3.1 隐式转换

无论是spark还是flink任务，都需要引入以下的隐式转换，该隐式转换提供了众多简单易用的api。

```scala
import com.zto.fire._
```

#### 3.2 继承父类

Fire框架针对不同的引擎、不同的场景提供了对应的父类，用户需要根据实际情况去继承：

##### 3.2.1 spark引擎父类列表：

- **SparkStreaming**：适用于进行Spark Streaming任务的开发
- **BaseSparkCore**：适用于进行Spark批处理任务的开发
- **BaseStructuredStreaming**：适用于进行Spark Structured Streaming任务的开发

##### 3.2.2 flink引擎父类列表：

- **BaseFlinkStreaming**：适用于进行flink流式计算任务的开发
- **BaseFlinkBatch**：适用于进行flink批处理任务的开发

#### 3.3 业务逻辑

Fire父类中统一约定了process方法，该方法会被fire框架自动调用，用户无需在代码中主动调用该方法。process方法作为业务逻辑的聚集地，是业务逻辑的开始。

```scala
override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print
    // 提交streaming任务
    this.fire.start
}
```

***说明：**Fire框架无需编写main方法，无需主动初始化sparksession或flink的environment等对象。这些会被fire框架自动初始化完成，开发者只需在代码中使用this.的方式引用即可。如果有spark或flink调优参数，可以直接复制到@Config注解中，这些调优参数会在fire框架初始化spark或flink引擎上下文时自动生效。*

