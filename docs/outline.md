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

# 框架集成

## 一、环境准备

通过git拉取到fire源码后，需要开发者将集群环境信息配置到fire框架中，该配置文件位于fire-core/src/main/resources/cluster.properties。如何配置请参考：[fire配置手册](./properties.md)。配置完成后，即可通过maven进行编译打包，建议将fire的包deploy到公司的私服，方便团队共用。

## 二、开发步骤

fire框架提供一种代码结构，基于该结构有助于为spark或flink程序进行一定的代码梳理，便于形成统一的编程风格，利于团队协作、排查问题。

```scala
import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * Flink流式计算任务模板
 */
object Test extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print
    // 提交flink streaming任务，job名称不指定默认当前类名
    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    // 根据配置信息自动创建fire变量、StreamExecutionEnvironment、StreamTableEnvironment等
    this.init()
  }
}
```

从以上代码片段中可以看到，引入fire框架大体分为5个步骤：

### 一、隐式转换

无论是spark还是flink任务，都需要引入以下的隐式转换，该隐式转换提供了众多简单易用的api。

```scala
import com.zto.fire._
```

### 二、继承父类

​		fire框架针对不同的引擎、不同的场景提供了对应的父类，用户需要根据实际情况去继承：

​		**1. spark引擎父类列表：**

​			**SparkStreaming**：适用于进行Spark Streaming任务的开发

​			**BaseSparkCore**：适用于进行Spark批处理任务的开发

​			**BaseStructuredStreaming**：适用于进行Spark Structured Streaming任务的开发

​		**2. flink引擎父类列表：**

​			**BaseFlinkStreaming**：适用于进行flink流式计算任务的开发

​			**BaseFlinkBatch**：适用于进行flink批处理任务的开发

### 三、初始化

实时任务有一个特点就是一个任务一个类，由于缺少统一的规范，用户进行实时任务开发时，会将很多业务代码写到main方法中，导致main方法过胖。由此带来的问题是代码难以阅读、难以维护。另外，在进行代码开发时，难以避免重复的写初始化spark或flink引擎相关的上下文信息。为了解决以上问题，fire框架将引擎上下文初始化简化成了一行代码，并建议在main方法中只做初始化动作，业务逻辑则放到process方法中。

```scala
def main(args: Array[String]): Unit = {
    // 根据任务同名的配置文件进行引擎上下文的初始化
    this.init()
}
```

上述代码适用于spark或flink引擎，对于个性化的初始化需求，可以将一些参数信息放到任务同名的配置文件中。该配置文件会在初始化之前自动被加载，然后设置到SparkSession或flink的environment中。

### 四、业务逻辑

为了解决main方法“过胖”的问题，fire父类中统一约定了process方法，该方法会被fire框架自动调用，用户无需在代码中主动调用该方法。process方法作为业务逻辑的聚集地，是业务逻辑的开始。

```scala
override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print
    // 提交streaming任务
    this.fire.start
}
```

当然，如果业务逻辑很复杂，可以进一步抽取，然后在process中调用即可。

#### 五、配置文件

将配置信息硬编码到代码中是很不好的做法，为了让程序足够灵活，代码足够简洁，fire框架约定，每个任务可以有一个与类名同名的配置文件。比如说类名是：**Test.scala**，则fire框架在init的时候会自动扫描并加载src/main/resources/**Test.properties**文件。支持配置文件的嵌套结构，比如说在resources下可以进一步创建多个子目录，存放不同类别的配置文件，便于管理。![配置文件](D:\project\workspace\fire\docs\img\configuration.jpg)

​		

