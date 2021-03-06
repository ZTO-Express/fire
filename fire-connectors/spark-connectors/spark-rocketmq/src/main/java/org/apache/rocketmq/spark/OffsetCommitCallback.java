/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.spark;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Map;

/**
 * A callback interface that the user can implement to trigger custom actions when a commit request completes.
 */
public interface OffsetCommitCallback {
    /**
     * A callback method the user can implement to provide asynchronous handling of commit request completion.
     * This method will be called by InputDstream when the last batch is handled successfully.
     * @param offsets the offsets which already are handled successfully
     * @param exception The exception thrown during processing of the request, or null if the commit completed successfully
     */
    void onComplete(Map<MessageQueue, Long> offsets, Exception exception);
}