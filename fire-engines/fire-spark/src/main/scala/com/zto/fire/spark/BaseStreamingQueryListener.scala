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

package com.zto.fire.spark

import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * structured streaming事件监听器
 *
 * @author ChengLong 2019年12月24日 16:26:33
 * @since 0.4.1
 */
class BaseStreamingQueryListener extends StreamingQueryListener {
  @volatile protected var latestBatchId = -1L

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      // onQueryStarted
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    this.latestBatchId = event.progress.batchId
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    // onQueryTerminated
  }
}
