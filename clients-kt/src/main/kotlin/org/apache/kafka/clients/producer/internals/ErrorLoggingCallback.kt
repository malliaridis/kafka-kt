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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory

class ErrorLoggingCallback(
    private val topic: String,
    private val key: ByteArray?,
    value: ByteArray?,
    private val logAsString: Boolean,
) : Callback {

    private val value: ByteArray? = if (logAsString) value else null

    private val valueLength: Int = value?.size ?: -1

    override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
        if (exception != null) {
            val keyString = if (key == null) "null"
            else if (logAsString) String(key)
            else key.size.toString() + " bytes"

            val valueString = if (valueLength == -1) "null"
            else if (logAsString) String(value!!)
            else "$valueLength bytes"

            log.error(
                "Error when sending message to topic {} with key: {}, value: {} with error:",
                topic,
                keyString,
                valueString,
                exception,
            )
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ErrorLoggingCallback::class.java)
    }
}
