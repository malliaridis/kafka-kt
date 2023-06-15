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

package org.apache.kafka.common.network

import java.io.IOException

class NetworkSend(
    val destinationId: String,
    val send: Send
) : Send {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("destinationId")
    )
    fun destinationId(): String = destinationId

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("send")
    )
    fun send(): Send = send

    override fun completed(): Boolean = send.completed()

    @Throws(IOException::class)
    override fun writeTo(channel: TransferableChannel): Long {
        return send.writeTo(channel)
    }

    override fun size(): Long {
        return send.size()
    }
}
