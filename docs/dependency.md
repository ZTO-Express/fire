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

### 依赖管理

```xml
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-common_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-core_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-jdbc_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-hbase_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<!-- spark任务单独引入fire-spark依赖 -->
<dependency>
    <groupId>com.zto.fire</groupId>
    <artifactId>fire-spark_${spark.reference}</artifactId>
    <version>${project.version}</version>
</dependency>
<!-- flink任务单独引入fire-flink依赖 -->
<dependency>
    <groupId>com.zto.fire</groupId>
    <artifactId>fire-flink_${flink.reference}</artifactId>
    <version>${project.version}</version>
</dependency>
```

### 示例pom.xml

- [spark项目](pom/spark-pom.xml)
- [flink项目](pom/flink-pom.xml)

