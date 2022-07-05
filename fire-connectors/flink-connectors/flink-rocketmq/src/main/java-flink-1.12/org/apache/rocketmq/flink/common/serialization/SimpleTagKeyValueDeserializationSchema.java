/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.common.serialization;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import scala.Tuple3;

import java.nio.charset.StandardCharsets;

/**
 * 反序列化MessageExt，将tag、key、value以tuple3方式返回
 *
 * @author ChengLong 2021-5-10 09:44:55
 */
public class SimpleTagKeyValueDeserializationSchema implements TagKeyValueDeserializationSchema<Tuple3<String, String, String>> {

    @Override
    public Tuple3<String, String, String> deserializeTagKeyAndValue(byte[] tag, byte[] key, byte[] value) {
        String tagString = tag != null ? new String(tag, StandardCharsets.UTF_8) : null;
        String keyString = key != null ? new String(key, StandardCharsets.UTF_8) : null;
        String valueString = value != null ? new String(value, StandardCharsets.UTF_8) : null;
        return new Tuple3<>(tagString, keyString, valueString);
    }

    @Override
    public TypeInformation<Tuple3<String, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String,String, String>>(){});
    }
}
