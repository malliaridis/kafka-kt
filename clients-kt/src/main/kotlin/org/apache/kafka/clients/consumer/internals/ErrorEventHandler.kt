/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.consumer.internals

import java.util.Queue
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent

class ErrorEventHandler(private val backgroundEventQueue: Queue<BackgroundEvent>) {

    fun handle(e: Throwable) {
        backgroundEventQueue.add(ErrorBackgroundEvent(e))
    }
}
