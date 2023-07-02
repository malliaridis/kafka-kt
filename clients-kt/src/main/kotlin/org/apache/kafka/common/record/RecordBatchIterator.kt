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

package org.apache.kafka.common.record

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.utils.AbstractIterator
import java.io.EOFException
import java.io.IOException

internal class RecordBatchIterator<T : RecordBatch>(
    private val logInputStream: LogInputStream<T>,
) : AbstractIterator<T>() {

    override fun makeNext(): T? {
        return try {
            logInputStream.nextBatch() ?: allDone()
        } catch (e: EOFException) {
            throw CorruptRecordException(
                "Unexpected EOF while attempting to read the next batch",
                e,
            )
        } catch (e: IOException) {
            throw KafkaException(cause = e)
        }
    }
}
