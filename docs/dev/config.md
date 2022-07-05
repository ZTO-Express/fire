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

# 参数配置

​		实时计算任务调优参数多种多样，繁杂众多，使用fire框架可以很方便的进行各种参数的设置。在方便开发者开发和调优的同时，业务平台集成提供了配置接口，实现平台化的配置管理。

### 1. 用户参数配置

​		fire框架支持基于接口、apollo、配置文件以及注解等多种方式配置，支持将spark&flink等**引擎参数**、**[fire框架参数](properties.md)**以及**用户自定义参数**混合配置，支持运行时动态修改配置。几种常用配置方式如下（[*fire内置参数*](properties.md)）：

- **基于配置文件：** 创建类名同名的properties文件进行参数配置
- **基于接口配置：** fire框架提供了配置接口调用，通过接口获取所需的配置，可用于平台化的配置管理
- **基于注解配置：** 通过注解的方式实现集群环境、connector、调优参数的配置

#### 1.1 基于注解

```scala
// 通用的配置注解，支持任意的参数，还可以替代connector(如@Hive、@Kafka)类型参数，支持注释和多行配置
@Config(
  """
    |# 支持Flink调优参数、Fire框架参数、用户自定义参数等
    |state.checkpoints.num-retained=30
    |state.checkpoints.dir=hdfs:///user/flink/checkpoint
    |my.conf=hello
    |""")
// 配置连接到指定的hive，支持别名：@Hive("test")，别名需在cluster.properties中指定
@Hive("thrift://localhost:9083")
// 100s做一次checkpoint，开启非对齐checkpoint，还支持checkpoint其他设置，如超时时间，两次checkpoint间隔时间等
@Checkpoint(interval = 100, unaligned = true)
// 配置kafka connector，多个kafka消费通过不同数值后缀区分：@Kafka2、@Kafka3、@Kafka5等，支持url或别名
@Kafka(brokers = "localhost:9092", topics = "fire", groupId = "fire")
// 配置rocketmq connector，同样支持消费多个rocketmq，支持url或别名
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire", tag = "*", startingOffset = "latest")
// jdbc注解，可自动推断driverClass，支持配置多个jdbc数据源，支持url或别名
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "..root726")
// 配置Hbase数据源，支持配置多HBase集群读写，支持url或别名
@HBase("localhost:2181")
```

#### 1.2 基于配置文件

​		fire框架约定，在任务启动时自动加载与该任务同名的，位于resources目录下以.properties结尾的配置文件（支持目录）。配置文件中如果定义了与@Config注解或者其他配置注解相同的配置时，配置文件中的优先级更高。[*fire框架参数*](properties.md)

<img src="./img/configuration.png"></img>

​		另外，如果同一个项目中有多个任务共用一些配置信息，比如jdbc url、hbase集群地址等，可以将这些公共的配置放到resources目录下名为**common.properties**配置文件中。这样每个任务在启动前会先加载这个配置文件，实现配置复用。common.properties中的配置优先级低于任务级别的配置。

#### 1.3 基于平台

​		上述两种，无论是基于注解还是基于配置文件，修改参数时，都需要修改代码然后重新编译发布执行。为了节约开发时间，fire框架提供了参数设置接口，实时平台可通过接口调用的方式将web页面中任务级别的配置设置到不同的任务中，以此来实现在web页面中进行实时任务的调优。接口调用的参数优先级要高于配置文件和注解方式。

<img src="./img/web-config.png"></img>

#### 1.4 配置热更新

​		集成了fire框架的spark或flink任务，支持在运行时动态修改用户配置。比如想修改一个运行中任务的jdbc batch的大小，可以通过fire框架提供的配置热更新接口来实现。当接口调用后，fire框架会将最新的配置信息分布式的同步给spark的每一个executor以及flink的每一个taskmanager。

### 2. 配置获取

​		fire框架封装了统一的配置获取api，基于该api，无论是spark还是flink，无论是在Driver | JobManager端还是在Executor | TaskManager端，都可以直接一行代码获取所需配置。这套配置获取api，无需再在flink的map等算子中复写open方法了，用起来十分方便。

```scala
this.conf.getString("my.conf")
this.conf.getInt("state.checkpoints.num-retained")
...
```

### 3. 实时平台配置

​		fire框架是实时计算任务与实时平台之间沟通的桥梁，在设计之初，就充分考虑了与实时平台的集成。对于一些集群连接等敏感配置等配置，可通过配置中心来实现统一的约束。比如当迁移hive thrift地址时，可以在配置中心修改该地址，然后将配置的优先级调高为紧急，再通知对应实时任务重启任务即可实现hive thrift地址的统一修改。定义为紧急的配置，优先级是最高的，这样变实现了实时平台配置的统一兜底管理。

### 4. 配置别名

​		配置使用url是不方便记忆的，也不便于统一管理和维护。将如某个数据源的url地址需要改动，那很多任务都要受牵连。为了解决这个问题，fire框架支持将数据源的url定义别名，效果如下所示：

```scala
// 直接使用url
@Hive("thrift://localhost:9083")
@HBase("localhost:2181")

// 使用别名
@Hive("batch")
@HBase("test")
```

建议将别名统一定义到 *[cluster.properties](..//fire-core/src/main/resources/cluster.properties)* 配置文件中，以下分别举几个例子说明如何定义数据源别名：

```properties
# 定义hbase集群连接信息别名为test，代码中hbase配置简化为：@HBase("test")
fire.hbase.cluster.map.test=zk01:2181,zk02:2181,zk03:2181
# kafka集群名称与集群地址映射mq，代码中kafka配置简化为：@Kafka(brokers = "mq", topics = "fire", groupId = "fire")
fire.kafka.cluster.map.mq=kafka01:9092,kafka02:9092,kafka03:9092
# hive metastore地址定义别名为batch，则代码中配置简化为：@Hive("batch")
fire.hive.cluster.map.batch=thrift://thrift01:9083,thrift://thrift02:9083
```



### 5. 配置优先级

fire框架提供了很多种参数配置的方式，总结下来相同key的配置优先级如下：

***fire.properties <  cluster.properties < 配置中心通用配置 < spark.properties|flink.properties < spark-core.properties|spark-streaming.properties|structured-streaming.properties|flink-streaming.properties|flink-batch.properties  < common.properties < 注解配置方式 < 用户配置文件 < 配置中心紧急配置***

### 6. fire内置配置文件

fire框架内置了多个配置文件，用于应对多种引擎场景，分别是：

- **fire.properties**：该配置文件中fire框架的总配置文件，位于fire-core包中，其中的配置主要是针对fire框架的，不含有spark或flink引擎的配置
- **cluster.properties**：该配置文件用于存放各公司集群地址相关的映射信息，由于集群地址信息比较敏感，因此单独拿出来作为一个配置文件
- **spark.properties**：该配置文件是spark引擎的总配置文件，位于fire-spark包中，作为spark引擎任务的总配置文件
- **spark-core.properties**：该配置文件位于fire-spark包中，该配置文件用于配置spark core任务
- **spark-streaming.properties**：该配置文件位于fire-spark包中，主要用于spark streaming任务
- **structured-streaming.properties**：该配置文件位于fire-spark包中，用于进行structured streaming任务的配置
- **flink.properties**：该配置文件位于fire-flink包中，作为flink引擎的总配置文件
- **flink-streaming.properties**：该配置文件位于fire-flink包中，用于配置flink streaming任务
- **flink-batch.properties**：该配置文件位于fire-flink包中，用于配置flink批处理任务
