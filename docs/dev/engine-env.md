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

# 依赖管理

　　Fire框架中很多依赖的scope指定为provided，好处是避免jar包冲突、避免jar包过于臃肿。带来的问题是会引擎依赖找不到（class not found）异常。解决这个问题的方案有两个，一个是将fire中使用到的生命周期为provided的依赖改成compile（任务的pom.xml中指定），一个是将相应依赖的jar包放到spark或flink的lib目录下。本文档选择的是第二个方案，将缺失的依赖放到引擎部署目录的lib下。缺失的jar包可以从[网盘下载](https://pan.baidu.com/s/16kUGQIj2gQjWZdbmuxuyXw?pwd=fire)、[*maven中央仓库*](http://mvnrepository.com/)或本地仓库搜索找到。依赖列表如下：

## 一、Flink on yarn环境

***说明：**下面的jar包列表以flink1.14.3为例*

```shell
[fire@node01 lib]$ pwd
/home/fire/opt/flink-1.14.3/lib
[fire@node01 lib]$ ll
总用量 307580
-rwxr-xr-x 1 fire fire   1112191 2月  17 13:43 antlr-3.4.jar
-rwxr-xr-x 1 fire fire    164368 2月  17 13:43 antlr-runtime-3.4.jar
-rwxr-xr-x 1 fire fire   7685845 1月  26 13:47 flink-connector-hive_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire    255974 4月  21 14:31 flink-connector-jdbc_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire    250201 2月  16 18:27 flink-connector-jdbc_2.12-1.14.3.jar.bak
-rwxr-xr-x 1 fire fire    389763 4月  18 15:30 flink-connector-kafka_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire     85584 1月  11 07:42 flink-csv-1.14.3.jar
-rwxr-xr-x 1 fire fire 136054094 1月  11 07:45 flink-dist_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire     78159 2月  14 13:20 flink-hadoop-compatibility_2.12-1.14.2.jar
-rwxr-xr-x 1 fire fire    153145 1月  11 07:42 flink-json-1.14.3.jar
-rwxr-xr-x 1 fire fire    757120 2月  14 13:23 flink-scala_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire  24051799 2月  16 17:42 flink-shaded-hadoop-2-2.6.5-9.0.jar
-rwxr-xr-x 1 fire fire   7709731 8月  22 2021  flink-shaded-zookeeper-3.4.14.jar
-rwxr-xr-x 1 fire fire  39633410 1月  11 07:45 flink-table_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire    119407 2月  16 18:34 flink-table-api-java-bridge_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire     79018 2月  16 18:35 flink-table-api-scala_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire     50197 2月  16 18:35 flink-table-api-scala-bridge_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire    928547 2月  16 18:34 flink-table-common-1.14.3.jar
-rwxr-xr-x 1 fire fire  35774469 2月  16 18:34 flink-table-planner_2.12-1.14.3.jar
-rwxr-xr-x 1 fire fire    241622 2月  17 09:29 gson-2.8.5.jar
-rwxr-xr-x 1 fire fire   2256213 2月  16 18:42 guava-18.0.jar
-rwxr-xr-x 1 fire fire      9808 2月  17 09:24 hadoop-client-2.6.0-cdh5.12.1.jar
-rwxr-xr-x 1 fire fire   3539744 2月  17 09:24 hadoop-common-2.6.0-cdh5.12.1.jar
-rwxr-xr-x 1 fire fire   1765829 2月  17 09:24 hadoop-core-2.6.0-mr1-cdh5.12.1.jar
-rwxr-xr-x 1 fire fire  11550546 2月  17 09:24 hadoop-hdfs-2.6.0-cdh5.12.1.jar
-rwxr-xr-x 1 fire fire   1321508 2月  16 21:18 hbase-client-1.2.0-cdh5.12.1.jar
-rwxr-xr-x 1 fire fire    585568 2月  16 21:18 hbase-common-1.2.0-cdh5.12.1.jar
-rwxr-xr-x 1 fire fire   4618035 2月  16 21:18 hbase-protocol-1.2.0-cdh5.12.1.jar
-rwxr-xr-x 1 fire fire    292289 2月  18 17:39 hive-common-1.2.1.jar
-rwxr-xr-x 1 fire fire  20599029 2月  18 17:39 hive-exec-1.2.1.jar
-rwxr-xr-x 1 fire fire   5505100 2月  18 17:39 hive-metastore-1.2.1.jar
-rwxr-xr-x 1 fire fire     95806 2月  16 18:46 javax.servlet-api-3.1.0.jar
-rwxr-xr-x 1 fire fire    201124 2月  17 09:29 jdo-api-3.0.1.jar
-rwxr-xr-x 1 fire fire   3269712 2月  16 18:33 kafka-clients-2.4.1.jar
-rwxr-xr-x 1 fire fire    275186 2月  17 09:29 libfb303-0.9.0.jar
-rwxr-xr-x 1 fire fire    208006 1月   9 04:13 log4j-1.2-api-2.17.1.jar
-rwxr-xr-x 1 fire fire    301872 1月   9 04:13 log4j-api-2.17.1.jar
-rwxr-xr-x 1 fire fire   1790452 1月   9 04:13 log4j-core-2.17.1.jar
-rwxr-xr-x 1 fire fire     24279 1月   9 04:13 log4j-slf4j-impl-2.17.1.jar
-rwxr-xr-x 1 fire fire     82123 2月  16 21:25 metrics-core-2.2.0.jar
-rwxr-xr-x 1 fire fire    992805 4月  21 15:15 mysql-connector-java-5.1.41.jar
```

## 二、Spark on yarn环境

***说明：**下面的jar包列表以spark3.0.2为例*。[网盘下载](https://pan.baidu.com/s/16kUGQIj2gQjWZdbmuxuyXw?pwd=fire)

```shell
[fire@node01 jars]$ pwd
/home/fire/opt/spark3.0.2/jars
[fire@node01 jars]$ ll
总用量 216276
-rw-r--r-- 1 fire fire    69409 2月  16 2021 activation-1.1.1.jar
-rw-r--r-- 1 fire fire   134044 2月  16 2021 aircompressor-0.10.jar
-rw-r--r-- 1 fire fire  1168113 2月  16 2021 algebra_2.12-2.0.0-M2.jar
-rw-r--r-- 1 fire fire   445288 2月  16 2021 antlr-2.7.7.jar
-rw-r--r-- 1 fire fire   336803 2月  16 2021 antlr4-runtime-4.7.1.jar
-rw-r--r-- 1 fire fire   164368 2月  16 2021 antlr-runtime-3.4.jar
-rw-r--r-- 1 fire fire     4467 2月  16 2021 aopalliance-1.0.jar
-rw-r--r-- 1 fire fire    27006 2月  16 2021 aopalliance-repackaged-2.6.1.jar
-rw-r--r-- 1 fire fire    44925 2月  16 2021 apacheds-i18n-2.0.0-M15.jar
-rw-r--r-- 1 fire fire   691479 2月  16 2021 apacheds-kerberos-codec-2.0.0-M15.jar
-rw-r--r-- 1 fire fire   448794 2月  16 2021 apache-log4j-extras-1.2.17.jar
-rw-r--r-- 1 fire fire    16560 2月  16 2021 api-asn1-api-1.0.0-M20.jar
-rw-r--r-- 1 fire fire    79912 2月  16 2021 api-util-1.0.0-M20.jar
-rw-r--r-- 1 fire fire  1194003 2月  16 2021 arpack_combined_all-0.1.jar
-rw-r--r-- 1 fire fire    64674 2月  16 2021 arrow-format-0.15.1.jar
-rw-r--r-- 1 fire fire   105777 2月  16 2021 arrow-memory-0.15.1.jar
-rw-r--r-- 1 fire fire  1437215 2月  16 2021 arrow-vector-0.15.1.jar
-rw-r--r-- 1 fire fire    20437 2月  16 2021 audience-annotations-0.5.0.jar
-rw-r--r-- 1 fire fire   176285 2月  16 2021 automaton-1.11-8.jar
-rw-r--r-- 1 fire fire  1556863 2月  16 2021 avro-1.8.2.jar
-rw-r--r-- 1 fire fire   132989 2月  16 2021 avro-ipc-1.8.2.jar
-rw-r--r-- 1 fire fire   187052 2月  16 2021 avro-mapred-1.8.2-hadoop2.jar
-rw-r--r-- 1 fire fire   110600 2月  16 2021 bonecp-0.8.0.RELEASE.jar
-rw-r--r-- 1 fire fire 13826799 2月  16 2021 breeze_2.12-1.0.jar
-rw-r--r-- 1 fire fire   134696 2月  16 2021 breeze-macros_2.12-1.0.jar
-rw-r--r-- 1 fire fire  3226851 2月  16 2021 cats-kernel_2.12-2.0.0-M4.jar
-rw-r--r-- 1 fire fire   211523 2月  16 2021 chill_2.12-0.9.5.jar
-rw-r--r-- 1 fire fire    58684 2月  16 2021 chill-java-0.9.5.jar
-rw-r--r-- 1 fire fire   246918 2月  16 2021 commons-beanutils-1.9.4.jar
-rw-r--r-- 1 fire fire    41123 2月  16 2021 commons-cli-1.2.jar
-rw-r--r-- 1 fire fire   284184 2月  16 2021 commons-codec-1.10.jar
-rw-r--r-- 1 fire fire   588337 2月  16 2021 commons-collections-3.2.2.jar
-rw-r--r-- 1 fire fire    71626 2月  16 2021 commons-compiler-3.0.16.jar
-rw-r--r-- 1 fire fire   632424 2月  16 2021 commons-compress-1.20.jar
-rw-r--r-- 1 fire fire   298829 2月  16 2021 commons-configuration-1.6.jar
-rw-r--r-- 1 fire fire   166244 2月  16 2021 commons-crypto-1.1.0.jar
-rw-r--r-- 1 fire fire   160519 2月  16 2021 commons-dbcp-1.4.jar
-rw-r--r-- 1 fire fire   143602 2月  16 2021 commons-digester-1.8.jar
-rw-r--r-- 1 fire fire   305001 2月  16 2021 commons-httpclient-3.1.jar
-rw-r--r-- 1 fire fire   185140 2月  16 2021 commons-io-2.4.jar
-rw-r--r-- 1 fire fire   284220 2月  16 2021 commons-lang-2.6.jar
-rw-r--r-- 1 fire fire   503880 2月  16 2021 commons-lang3-3.9.jar
-rw-r--r-- 1 fire fire    62050 2月  16 2021 commons-logging-1.1.3.jar
-rw-r--r-- 1 fire fire  2035066 2月  16 2021 commons-math3-3.4.1.jar
-rw-r--r-- 1 fire fire   273370 2月  16 2021 commons-net-3.1.jar
-rw-r--r-- 1 fire fire    96221 2月  16 2021 commons-pool-1.5.4.jar
-rw-r--r-- 1 fire fire   197176 2月  16 2021 commons-text-1.6.jar
-rw-r--r-- 1 fire fire    79845 2月  16 2021 compress-lzf-1.0.3.jar
-rw-r--r-- 1 fire fire   164422 2月  16 2021 core-1.1.2.jar
-rw-r--r-- 1 fire fire    69500 2月  16 2021 curator-client-2.7.1.jar
-rw-r--r-- 1 fire fire   186273 2月  16 2021 curator-framework-2.7.1.jar
-rw-r--r-- 1 fire fire   270342 2月  16 2021 curator-recipes-2.7.1.jar
-rw-r--r-- 1 fire fire   339666 2月  16 2021 datanucleus-api-jdo-3.2.6.jar
-rw-r--r-- 1 fire fire  1890075 2月  16 2021 datanucleus-core-3.2.10.jar
-rw-r--r-- 1 fire fire  1809447 2月  16 2021 datanucleus-rdbms-3.2.9.jar
-rw-r--r-- 1 fire fire  3224708 2月  16 2021 derby-10.12.1.1.jar
-rw-r--r-- 1 fire fire    18497 2月  16 2021 flatbuffers-java-1.9.0.jar
-rw-r--r-- 1 fire fire    14395 2月  16 2021 generex-1.0.2.jar
-rw-r--r-- 1 fire fire   190432 2月  16 2021 gson-2.2.4.jar
-rw-r--r-- 1 fire fire  2189117 2月  16 2021 guava-14.0.1.jar
-rw-r--r-- 1 fire fire   710492 2月  16 2021 guice-3.0.jar
-rw-r--r-- 1 fire fire    65012 2月  16 2021 guice-servlet-3.0.jar
-rw-r--r-- 1 fire fire    41094 2月  16 2021 hadoop-annotations-2.7.4.jar
-rw-r--r-- 1 fire fire    94621 2月  16 2021 hadoop-auth-2.7.4.jar
-rw-r--r-- 1 fire fire    26243 2月  16 2021 hadoop-client-2.7.4.jar
-rw-r--r-- 1 fire fire  3499224 2月  16 2021 hadoop-common-2.7.4.jar
-rw-r--r-- 1 fire fire  8350471 2月  16 2021 hadoop-hdfs-2.7.4.jar
-rw-r--r-- 1 fire fire   543852 2月  16 2021 hadoop-mapreduce-client-app-2.7.4.jar
-rw-r--r-- 1 fire fire   776862 2月  16 2021 hadoop-mapreduce-client-common-2.7.4.jar
-rw-r--r-- 1 fire fire  1558288 2月  16 2021 hadoop-mapreduce-client-core-2.7.4.jar
-rw-r--r-- 1 fire fire    62960 2月  16 2021 hadoop-mapreduce-client-jobclient-2.7.4.jar
-rw-r--r-- 1 fire fire    72050 2月  16 2021 hadoop-mapreduce-client-shuffle-2.7.4.jar
-rw-r--r-- 1 fire fire  2039372 2月  16 2021 hadoop-yarn-api-2.7.4.jar
-rw-r--r-- 1 fire fire   166121 2月  16 2021 hadoop-yarn-client-2.7.4.jar
-rw-r--r-- 1 fire fire  1679789 2月  16 2021 hadoop-yarn-common-2.7.4.jar
-rw-r--r-- 1 fire fire   388572 2月  16 2021 hadoop-yarn-server-common-2.7.4.jar
-rw-r--r-- 1 fire fire    58699 2月  16 2021 hadoop-yarn-server-web-proxy-2.7.4.jar
-rw-r--r-- 1 fire fire   138464 2月  16 2021 hive-beeline-1.2.1.spark2.jar
-rw-r--r-- 1 fire fire    40817 2月  16 2021 hive-cli-1.2.1.spark2.jar
-rw-r--r-- 1 fire fire 11498852 2月  16 2021 hive-exec-1.2.1.spark2.jar
-rw-r--r-- 1 fire fire   100680 2月  16 2021 hive-jdbc-1.2.1.spark2.jar
-rw-r--r-- 1 fire fire  5505200 2月  16 2021 hive-metastore-1.2.1.spark2.jar
-rw-r--r-- 1 fire fire   200223 2月  16 2021 hk2-api-2.6.1.jar
-rw-r--r-- 1 fire fire   203358 2月  16 2021 hk2-locator-2.6.1.jar
-rw-r--r-- 1 fire fire   131590 2月  16 2021 hk2-utils-2.6.1.jar
-rw-r--r-- 1 fire fire  1475955 2月  16 2021 htrace-core-3.1.0-incubating.jar
-rw-r--r-- 1 fire fire   767140 2月  16 2021 httpclient-4.5.6.jar
-rw-r--r-- 1 fire fire   328347 2月  16 2021 httpcore-4.4.12.jar
-rw-r--r-- 1 fire fire    27156 2月  16 2021 istack-commons-runtime-3.0.8.jar
-rw-r--r-- 1 fire fire  1282424 2月  16 2021 ivy-2.4.0.jar
-rw-r--r-- 1 fire fire    67889 2月  16 2021 jackson-annotations-2.10.0.jar
-rw-r--r-- 1 fire fire   348635 2月  16 2021 jackson-core-2.10.0.jar
-rw-r--r-- 1 fire fire   232248 2月  16 2021 jackson-core-asl-1.9.13.jar
-rw-r--r-- 1 fire fire  1400944 2月  16 2021 jackson-databind-2.10.0.jar
-rw-r--r-- 1 fire fire    46646 2月  16 2021 jackson-dataformat-yaml-2.10.0.jar
-rw-r--r-- 1 fire fire   105898 2月  16 2021 jackson-datatype-jsr310-2.10.3.jar
-rw-r--r-- 1 fire fire    18336 2月  16 2021 jackson-jaxrs-1.9.13.jar
-rw-r--r-- 1 fire fire   780664 2月  16 2021 jackson-mapper-asl-1.9.13.jar
-rw-r--r-- 1 fire fire    34991 2月  16 2021 jackson-module-jaxb-annotations-2.10.0.jar
-rw-r--r-- 1 fire fire    43740 2月  16 2021 jackson-module-paranamer-2.10.0.jar
-rw-r--r-- 1 fire fire   341862 2月  16 2021 jackson-module-scala_2.12-2.10.0.jar
-rw-r--r-- 1 fire fire    27084 2月  16 2021 jackson-xc-1.9.13.jar
-rw-r--r-- 1 fire fire    44399 2月  16 2021 jakarta.activation-api-1.2.1.jar
-rw-r--r-- 1 fire fire    25058 2月  16 2021 jakarta.annotation-api-1.3.5.jar
-rw-r--r-- 1 fire fire    18140 2月  16 2021 jakarta.inject-2.6.1.jar
-rw-r--r-- 1 fire fire    91930 2月  16 2021 jakarta.validation-api-2.0.2.jar
-rw-r--r-- 1 fire fire   140376 2月  16 2021 jakarta.ws.rs-api-2.1.6.jar
-rw-r--r-- 1 fire fire   115498 2月  16 2021 jakarta.xml.bind-api-2.3.2.jar
-rw-r--r-- 1 fire fire   926574 2月  16 2021 janino-3.0.16.jar
-rw-r--r-- 1 fire fire    16993 2月  16 2021 JavaEWAH-0.3.2.jar
-rw-r--r-- 1 fire fire   780265 2月  16 2021 javassist-3.25.0-GA.jar
-rw-r--r-- 1 fire fire     2497 2月  16 2021 javax.inject-1.jar
-rw-r--r-- 1 fire fire    95806 2月  16 2021 javax.servlet-api-3.1.0.jar
-rw-r--r-- 1 fire fire   395195 2月  16 2021 javolution-5.5.1.jar
-rw-r--r-- 1 fire fire   105134 2月  16 2021 jaxb-api-2.2.2.jar
-rw-r--r-- 1 fire fire  1013367 2月  16 2021 jaxb-runtime-2.3.2.jar
-rw-r--r-- 1 fire fire    16537 2月  16 2021 jcl-over-slf4j-1.7.30.jar
-rw-r--r-- 1 fire fire   201124 2月  16 2021 jdo-api-3.0.1.jar
-rw-r--r-- 1 fire fire   244502 2月  16 2021 jersey-client-2.30.jar
-rw-r--r-- 1 fire fire  1166647 2月  16 2021 jersey-common-2.30.jar
-rw-r--r-- 1 fire fire    32091 2月  16 2021 jersey-container-servlet-2.30.jar
-rw-r--r-- 1 fire fire    73349 2月  16 2021 jersey-container-servlet-core-2.30.jar
-rw-r--r-- 1 fire fire    76733 2月  16 2021 jersey-hk2-2.30.jar
-rw-r--r-- 1 fire fire    85815 2月  16 2021 jersey-media-jaxb-2.30.jar
-rw-r--r-- 1 fire fire   927721 2月  16 2021 jersey-server-2.30.jar
-rw-r--r-- 1 fire fire   539912 2月  16 2021 jetty-6.1.26.jar
-rw-r--r-- 1 fire fire    18891 2月  16 2021 jetty-sslengine-6.1.26.jar
-rw-r--r-- 1 fire fire   177131 2月  16 2021 jetty-util-6.1.26.jar
-rw-r--r-- 1 fire fire   232470 2月  16 2021 JLargeArrays-1.5.jar
-rw-r--r-- 1 fire fire   268780 2月  16 2021 jline-2.14.6.jar
-rw-r--r-- 1 fire fire   643043 2月  16 2021 joda-time-2.10.5.jar
-rw-r--r-- 1 fire fire   427780 2月  16 2021 jodd-core-3.5.2.jar
-rw-r--r-- 1 fire fire    12131 2月  16 2021 jpam-1.1.jar
-rw-r--r-- 1 fire fire    83632 2月  16 2021 json4s-ast_2.12-3.6.6.jar
-rw-r--r-- 1 fire fire   482486 2月  16 2021 json4s-core_2.12-3.6.6.jar
-rw-r--r-- 1 fire fire    36175 2月  16 2021 json4s-jackson_2.12-3.6.6.jar
-rw-r--r-- 1 fire fire   349025 2月  16 2021 json4s-scalap_2.12-3.6.6.jar
-rw-r--r-- 1 fire fire   100636 2月  16 2021 jsp-api-2.1.jar
-rw-r--r-- 1 fire fire    33031 2月  16 2021 jsr305-3.0.0.jar
-rw-r--r-- 1 fire fire    15071 2月  16 2021 jta-1.1.jar
-rw-r--r-- 1 fire fire  1175798 2月  16 2021 JTransforms-3.1.jar
-rw-r--r-- 1 fire fire     4592 2月  16 2021 jul-to-slf4j-1.7.30.jar
-rw-r--r-- 1 fire fire   410874 2月  16 2021 kryo-shaded-4.0.2.jar
-rw-r--r-- 1 fire fire   775174 2月  16 2021 kubernetes-client-4.9.2.jar
-rw-r--r-- 1 fire fire 11908731 2月  16 2021 kubernetes-model-4.9.2.jar
-rw-r--r-- 1 fire fire     3954 2月  16 2021 kubernetes-model-common-4.9.2.jar
-rw-r--r-- 1 fire fire  1045744 2月  16 2021 leveldbjni-all-1.8.jar
-rw-r--r-- 1 fire fire   313702 2月  16 2021 libfb303-0.9.3.jar
-rw-r--r-- 1 fire fire   246445 2月  16 2021 libthrift-0.12.0.jar
-rw-r--r-- 1 fire fire   489884 2月  16 2021 log4j-1.2.17.jar
-rw-r--r-- 1 fire fire    12488 2月  16 2021 logging-interceptor-3.12.6.jar
-rw-r--r-- 1 fire fire   649950 2月  16 2021 lz4-java-1.7.1.jar
-rw-r--r-- 1 fire fire    33786 2月  16 2021 machinist_2.12-0.6.8.jar
-rw-r--r-- 1 fire fire     3180 2月  16 2021 macro-compat_2.12-1.1.1.jar
-rw-r--r-- 1 fire fire  7343426 2月  16 2021 mesos-1.4.0-shaded-protobuf.jar
-rw-r--r-- 1 fire fire   105365 2月  16 2021 metrics-core-4.1.1.jar
-rw-r--r-- 1 fire fire    22042 2月  16 2021 metrics-graphite-4.1.1.jar
-rw-r--r-- 1 fire fire    20889 2月  16 2021 metrics-jmx-4.1.1.jar
-rw-r--r-- 1 fire fire    16642 2月  16 2021 metrics-json-4.1.1.jar
-rw-r--r-- 1 fire fire    23909 2月  16 2021 metrics-jvm-4.1.1.jar
-rw-r--r-- 1 fire fire     5711 2月  16 2021 minlog-1.3.0.jar
-rw-r--r-- 1 fire fire  4153218 2月  16 2021 netty-all-4.1.47.Final.jar
-rw-r--r-- 1 fire fire    54391 2月  16 2021 objenesis-2.5.1.jar
-rw-r--r-- 1 fire fire   423175 2月  16 2021 okhttp-3.12.6.jar
-rw-r--r-- 1 fire fire    88732 2月  16 2021 okio-1.15.0.jar
-rw-r--r-- 1 fire fire    19827 2月  16 2021 opencsv-2.3.jar
-rw-r--r-- 1 fire fire  1580620 2月  16 2021 orc-core-1.5.10-nohive.jar
-rw-r--r-- 1 fire fire   814061 2月  16 2021 orc-mapreduce-1.5.10-nohive.jar
-rw-r--r-- 1 fire fire    27749 2月  16 2021 orc-shims-1.5.10.jar
-rw-r--r-- 1 fire fire    65261 2月  16 2021 oro-2.0.8.jar
-rw-r--r-- 1 fire fire    19479 2月  16 2021 osgi-resource-locator-1.0.3.jar
-rw-r--r-- 1 fire fire    34654 2月  16 2021 paranamer-2.8.jar
-rw-r--r-- 1 fire fire  1097799 2月  16 2021 parquet-column-1.10.1.jar
-rw-r--r-- 1 fire fire    94995 2月  16 2021 parquet-common-1.10.1.jar
-rw-r--r-- 1 fire fire   848750 2月  16 2021 parquet-encoding-1.10.1.jar
-rw-r--r-- 1 fire fire   723203 2月  16 2021 parquet-format-2.4.0.jar
-rw-r--r-- 1 fire fire   285732 2月  16 2021 parquet-hadoop-1.10.1.jar
-rw-r--r-- 1 fire fire  2796935 2月  16 2021 parquet-hadoop-bundle-1.6.0.jar
-rw-r--r-- 1 fire fire  1048171 2月  16 2021 parquet-jackson-1.10.1.jar
-rw-r--r-- 1 fire fire   533455 2月  16 2021 protobuf-java-2.5.0.jar
-rw-r--r-- 1 fire fire   123052 2月  16 2021 py4j-0.10.9.jar
-rw-r--r-- 1 fire fire   100431 2月  16 2021 pyrolite-4.30.jar
-rw-r--r-- 1 fire fire   325335 2月  16 2021 RoaringBitmap-0.7.45.jar
-rw-r--r-- 1 fire fire   112235 2月  16 2021 scala-collection-compat_2.12-2.1.1.jar
-rw-r--r-- 1 fire fire 10672015 2月  16 2021 scala-compiler-2.12.10.jar
-rw-r--r-- 1 fire fire  5276900 2月  16 2021 scala-library-2.12.10.jar
-rw-r--r-- 1 fire fire   222980 2月  16 2021 scala-parser-combinators_2.12-1.1.2.jar
-rw-r--r-- 1 fire fire  3678534 2月  16 2021 scala-reflect-2.12.10.jar
-rw-r--r-- 1 fire fire   556575 2月  16 2021 scala-xml_2.12-1.2.0.jar
-rw-r--r-- 1 fire fire  3243337 2月  16 2021 shapeless_2.12-2.3.3.jar
-rw-r--r-- 1 fire fire     4028 2月  16 2021 shims-0.7.45.jar
-rw-r--r-- 1 fire fire    41472 2月  16 2021 slf4j-api-1.7.30.jar
-rw-r--r-- 1 fire fire    12211 2月  16 2021 slf4j-log4j12-1.7.30.jar
-rw-r--r-- 1 fire fire   302558 2月  16 2021 snakeyaml-1.24.jar
-rw-r--r-- 1 fire fire    48720 2月  16 2021 snappy-0.2.jar
-rw-r--r-- 1 fire fire  1969177 2月  16 2021 snappy-java-1.1.8.2.jar
-rw-r--r-- 1 fire fire  9409634 2月  16 2021 spark-catalyst_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire  9880087 2月  16 2021 spark-core_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire   430762 2月  16 2021 spark-graphx_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire   693694 2月  16 2021 spark-hive_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire  1886671 2月  16 2021 spark-hive-thriftserver_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire   374948 2月  16 2021 spark-kubernetes_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire    59868 2月  16 2021 spark-kvstore_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire    75937 2月  16 2021 spark-launcher_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire   295158 2月  16 2021 spark-mesos_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire  5887713 2月  16 2021 spark-mllib_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire   111921 2月  16 2021 spark-mllib-local_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire  2397705 2月  16 2021 spark-network-common_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire    86942 2月  16 2021 spark-network-shuffle_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire    52496 2月  16 2021 spark-repl_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire    30353 2月  16 2021 spark-sketch_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire  7160215 2月  16 2021 spark-sql_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire  1138146 2月  16 2021 spark-streaming_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire    15155 2月  16 2021 spark-tags_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire    10375 2月  16 2021 spark-tags_2.12-3.0.2-tests.jar
-rw-r--r-- 1 fire fire    51308 2月  16 2021 spark-unsafe_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire   331837 7月  28 2021 spark-yarn_2.12-3.0.2.jar
-rw-r--r-- 1 fire fire   331935 2月  16 2021 spark-yarn_2.12-3.0.2.jar.bak
-rw-r--r-- 1 fire fire  7188024 2月  16 2021 spire_2.12-0.17.0-M1.jar
-rw-r--r-- 1 fire fire    79588 2月  16 2021 spire-macros_2.12-0.17.0-M1.jar
-rw-r--r-- 1 fire fire     8261 2月  16 2021 spire-platform_2.12-0.17.0-M1.jar
-rw-r--r-- 1 fire fire    34601 2月  16 2021 spire-util_2.12-0.17.0-M1.jar
-rw-r--r-- 1 fire fire   236660 2月  16 2021 ST4-4.0.4.jar
-rw-r--r-- 1 fire fire    26514 2月  16 2021 stax-api-1.0.1.jar
-rw-r--r-- 1 fire fire    23346 2月  16 2021 stax-api-1.0-2.jar
-rw-r--r-- 1 fire fire   178149 2月  16 2021 stream-2.9.6.jar
-rw-r--r-- 1 fire fire   148627 2月  16 2021 stringtemplate-3.2.1.jar
-rw-r--r-- 1 fire fire    93210 2月  16 2021 super-csv-2.2.0.jar
-rw-r--r-- 1 fire fire   233745 2月  16 2021 threeten-extra-1.5.0.jar
-rw-r--r-- 1 fire fire   443986 2月  16 2021 univocity-parsers-2.9.0.jar
-rw-r--r-- 1 fire fire   281356 2月  16 2021 xbean-asm7-shaded-4.15.jar
-rw-r--r-- 1 fire fire  1386397 2月  16 2021 xercesImpl-2.12.0.jar
-rw-r--r-- 1 fire fire   220536 2月  16 2021 xml-apis-1.4.01.jar
-rw-r--r-- 1 fire fire    15010 2月  16 2021 xmlenc-0.52.jar
-rw-r--r-- 1 fire fire    99555 2月  16 2021 xz-1.5.jar
-rw-r--r-- 1 fire fire    35518 2月  16 2021 zjsonpatch-0.3.0.jar
-rw-r--r-- 1 fire fire   911603 2月  16 2021 zookeeper-3.4.14.jar
-rw-r--r-- 1 fire fire  4210625 2月  16 2021 zstd-jni-1.4.4-3.jar
```

