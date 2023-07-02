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

/**
 * Defines the record format versions supported by Kafka.
 *
 * For historical reasons, the record format version is also known as `magic` and
 * `message format version`. Note that the version actually applies to the [RecordBatch] (instead of
 * the [Record]). Finally, the `message.format.version` topic config confusingly expects an
 * ApiVersion instead of a RecordVersion.
 */
enum class RecordVersion(val value: Byte) {

    V0(0.toByte()),

    V1(1.toByte()),

    V2(2.toByte());

    /**
     * Check whether this version precedes another version.
     *
     * @return true only if the magic value is less than the other's
     */
    fun precedes(other: RecordVersion): Boolean = value < other.value

    companion object {

        fun lookup(value: Byte): RecordVersion {
            require(!(value < 0 || value >= values().size)) { "Unknown record version: $value" }
            return values()[value.toInt()]
        }

        fun current(): RecordVersion = V2
    }
}
