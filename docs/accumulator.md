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

# 累加器

Fire框架针对spark和flink的累加器进行了深度的定制，该api具有不需要事先声明累加器变量，可到处使用等优点。[示例代码](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/acc/FireAccTest.scala)

### 一、累加器的基本使用

```scala
// 消息接入
val dstream = this.fire.createKafkaDirectStream()
dstream.foreachRDD(rdd => {
    rdd.coalesce(this.conf.getInt(key, 10)).foreachPartition(t => {
        // 单值累加器
        this.acc.addCounter(1)
        // 多值累加器，根据key的不同分别进行数据的累加，以下两行代码表示分别对multiCounter
        // 和partitions这两个累加器进行累加
        this.acc.addMultiCounter("multiCounter", 1)
        this.acc.addMultiCounter("partitions", 1)
        // 多时间维度累加器，比多值累加器多了一个时间维度，
        // 如：hbaseWriter 2019-09-10 11:00:00  10
        // 如：hbaseWriter 2019-09-10 11:01:00  21
        this.acc.addMultiTimer("multiTimer", 1)
    })
})
```

### 二、累加器类型

1. 单值累加器

   单值累加器的特点是：只会将数据累加到同一个累加器中，全局唯一。

2. 多值累加器

   多值累加器的特点是：不同累加器实例使用不同的字符串key作为区分，相同的key的进行统一的累加，比单值累加器更强大。

3. 时间维度累加器

   时间维度累加器是在多值累加器的基础上进行了进一步的增强，引入了时间维度的概念。它以时间和累加器标识作为联合的累加器key。比如key为hbase_sink，那么统计的数据默认是按分钟进行，下一分钟是一个全新的累加窗口。时间维度累加器可以通过参数修改时间戳的格式，比如按分钟、小时、天、月、年等。

   ```scala
   // 多时间维度累加器，比多值累加器多了一个时间维度，
   // 如：hbaseWriter 2019-09-10 11:00:00  10
   // 如：hbaseWriter 2019-09-10 11:01:00  21
   this.acc.addMultiTimer("multiTimer", 1)
   // 指定时间戳，以小时作为统计窗口进行累加
   this.acc.addMultiTimer("multiTimer", 1, schema = "YYYY-MM-dd HH")
   ```

### 三、累加器值的获取

 1. 程序中获取

    ```scala
    /**
      * 获取累加器中的值
      */
    @Scheduled(fixedInterval = 60 * 1000)
    def printAcc: Unit = {
        this.acc.getMultiTimer.cellSet().foreach(t => println(s"key：" + t.getRowKey + "       时间：" + t.getColumnKey + " " + t.getValue + "条"))
        println("单值：" + this.acc.getCounter)
        this.acc.getMultiCounter.foreach(t => {
            println("多值：key=" + t._1 + " value=" + t._2)
        })
        val size = this.acc.getMultiTimer.cellSet().size()
        println(s"===multiTimer.size=${size}==log.size=${this.acc.getLog.size()}===")
    }
    ```

    

 2. 平台接口获取

    Fire框架针对累加器的获取提供了单独的接口，平台可以通过接口调用方式实时获取累加器的最新统计结果。

    | 接口地址             | 接口用途                         |
    | -------------------- | -------------------------------- |
    | /system/counter      | 用于获取累加器的值。             |
    | /system/multiCounter | 用于获取多值累加器的值。         |
    | /system/multiTimer   | 用于获取时间维度多值累加器的值。 |