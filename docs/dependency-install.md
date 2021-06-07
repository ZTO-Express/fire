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

# 第三方库jar包install

fire框架引用了众多第三方包，由于很多库已经很久没有更新了，甚至在maven的全球中央仓库中也没有，因此，为了使用方便，特地将基于scala2.12+spark3.0.2编译的jar包提供出来，放在fire框架的/docs/lib目录下。用户可以通过maven的命令将这些库install到本地或deploy到公司的私服。

## 一、本地maven install

### 1.1 hbase-spark包

```shell
mvn install:install-file -Dfile=/path/to/hbase-spark3_2.12-1.2.0-cdh5.12.1.jar -DgroupId=org.apache.hbase -DartifactId=hbase-spark3_2.12 -Dversion=1.2.0-cdh5.12.1 -Dpackaging=jar
```

### 1.2 hbase-client包

```shell
mvn install:install-file -Dfile=/path/to/hbase-client_2.12-1.2.0-cdh5.12.1.jar -DgroupId=org.apache.hbase -DartifactId=hbase-client_2.12 -Dversion=1.2.0-cdh5.12.1 -Dpackaging=jar
```

### 1.3 rocketmq-spark包

```shell
mvn install:install-file -Dfile=/path/to/rocketmq-spark3_2.12.jar -DgroupId=org.apache.rocketmq -DartifactId=rocketmq-spark3_2.12 -Dversion=0.0.2 -Dpackaging=jar
```

### 1.4 rocketmq-flink包

```shell
mvn install:install-file -Dfile=/path/to/rocketmq-flink_1.12_2.12.jar -DgroupId=org.apache.rocketmq -DartifactId=rocketmq-flink_1.12_2.12 -Dversion=0.0.2 -Dpackaging=jar
```

### 1.5 kudu-spark包

```shell
mvn install:install-file -Dfile=/path/to/kudu-spark3_2.12-1.4.0.jar -DgroupId=org.apache.kudu -DartifactId=kudu-spark3_2.12 -Dversion=1.4.0 -Dpackaging=jar
```

## 二、deploy到私服

以下命令以hbase-client包为例，将该包推送到公司自己的私服中。推送私服，首先需要在settings.xml中配置私服账号、密码等信息，可自行查阅资料。

```shell
mvn deploy:deploy-file -Dfile=/path/to/hbase-client_2.12-1.2.0-cdh5.12.1.jar -DgroupId=org.apache.hbase -DartifactId=hbase-client_2.12 -Dversion=1.2.0-cdh5.12.1 -Dpackaging=jar -DrepositoryId=releases -Durl=http://ip:port/nexus/content/repositories/releases/
```

## 三、自行编译

为了满足差异化、不同版本的编译需求，用户可以到github上找到相应库官方源码（需要进行定制化开发，比如适配scala2.12以及spark或flink版本差异带来的编译错误），进行编译，github地址如下：

[hbase-spark库源码地址](https://github.com/cloudera/hbase/tree/cdh5-1.2.0_5.12.2/hbase-spark)

[kudu-spark库源码地址](https://github.com/apache/kudu/tree/master/java/kudu-spark)

[rocketmq-spark库源码地址](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-spark)

[rocketmq-flink库源码地址](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-flink)

