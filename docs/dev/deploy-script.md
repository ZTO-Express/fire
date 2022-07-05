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

## 任务提交脚本

### 一、Flink on yarn

***说明：**强烈建议使用flink的run application模式提交任务，在run application模式下可以使用fire更多的功能，包括restful接口，配置管理等，便于与实时平台集成，per-job模式不推荐使用。*

```shell
#!/bin/bash
# author: wangchenglong
# date: 2022-06-30 13:10:13
# desc：提交flink任务通用脚本
# usage：./deploy.sh com.zto.fire.examples.flink.Test

export FLINK_HOME=/opt/flink-1.14.3
export PATH=$FLINK_HOME/bin:$PATH

# 以run application模式提交flink任务到yarn上
flink run-application -t yarn-application \	# 使用run-application模式提交，让flink任务与实时平台具有交互能力
-D taskmanager.memory.process.size=4g \
-D state.checkpoints.dir=hdfs:///user/flink/checkpoint/fire \
-D flink.stream.checkpoint.interval=6000 \
-D fire.shutdown.auto.exit=true \	# 可通过-D方式指定flink引擎参数、fire框架参数或用户自定义参数，代码中通过this.conf.get获取参数值
--allowNonRestoredState \
-s hdfs:/user/flink/checkpoint/xxx/chk-5/_metadata \	# 指定checkpoint路径
-ynm fire_test -yqu root.default -ynm test -ys 1 -ytm 2g -c $1 zto-flink*.jar $*
```

### 二、Spark on yarn

```shell
#!/bin/bash
# author: wangchenglong
# date: 2022-06-30 13:24:13
# desc：提交spark任务通用脚本
# usage：./deploy.sh com.zto.fire.examples.spark.Test

export SPARK_HOME=/opt/spark3.0.2
export PATH=$SPARK_HOME/bin:$PATH

# 以cluster模式提交spark任务到yarn上
spark-submit \
--master yarn --deploy-mode cluster --class $1 --num-executors 20 --executor-cores 1 \
--driver-memory 1g --executor-memory 1g \
--conf fire.shutdown.auto.exit=true \	# 可通过--conf方式指定spark引擎参数、fire框架参数或用户自定义参数，通过this.conf.get获取参数值
./zto-spark*.jar $*
```