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

# fire内置的restful接口

​		fire框架在提供丰富好用的api给开发者的同时，也提供了大量的restful接口给大数据实时计算平台。通过对外暴露的restful接口，可以将每个任务与实时平台进行深入绑定，为平台建设提供了更大的想象空间。其中包括：**实时热重启接口、动态批次时间调整接口、sql在线调试接口**、**Arthas诊断jvm**、**实时血缘分析**等。

| 引擎      | 接口                         | 含义                                                         |
| --------- | ---------------------------- | ------------------------------------------------------------ |
| 通用      | /system/kill                 | 用于kill 任务自身。                                          |
| 通用      | /system/cancelJob            | 生产环境中，通常会禁用掉spark webui的kill功能，但有时任务owner有kill的需求，为了满足此类需求，fire通过接口的方式将kill功能暴露给平台，由平台控制权限并完成kill job的触发。 |
| 通用      | /system/cancelStage          | 同job的kill功能，该接口用于kill指定的stage。                 |
| 通用      | /system/sql                  | 该接口允许用户传递sql给spark任务执行，可用于sql的动态调试，支持在任务开发阶段spark临时表与hive表的关联，降低sql开发的人力成本。 |
| 通用      | /system/sparkInfo            | 用户获取当前spark任务的配置信息。                            |
| 通用      | /system/counter              | 用于获取累加器的值。                                         |
| 通用      | /system/multiCounter         | 用于获取多值累加器的值。                                     |
| 通用      | /system/multiTimer           | 用于获取时间维度多值累加器的值。                             |
| 通用      | /system/log                  | 用于获取日志信息，平台可调用该接口获取日志并进行日志展示。   |
| 通用      | /system/env                  | 获取运行时状态信息，包括GC、jvm、thread、memory、cpu等       |
| 通用      | /system/listDatabases        | 用于列举当前spark任务catalog中所有的数据库，包括hive库等。   |
| 通用      | /system/listTables           | 用于列举指定库下所有的表信息。                               |
| 通用      | /system/listColumns          | 用于列举某张表的所有字段信息。                               |
| spark通用 | /system/listFunctions        | 用于列举当前任务支持的函数。                                 |
| 通用      | /system/setConf              | 用于配置热覆盖，在运行时动态修改指定的配置信息。比如动态修改spark streaming某个rdd的分区数，实现动态调优的目的。 |
| 通用      | /system/datasource           | 用于获取当前任务使用到的数据源信息、表信息等。支持jdbc、hbase、kafka、hive等众多组件，可用于和平台集成，做实时血缘关系。 |
| spark     | /system/streaming/hotRestart | spark streaming热重启接口，可以动态的修改运行中的spark streaming的批次时间。 |
| flink     | /system/checkpoint           | 用于运行时热修改checkpoint                                   |
| 通用      | /system/arthas               | 动态开启或关闭arthas服务，用于运行时分析诊断jvm              |

