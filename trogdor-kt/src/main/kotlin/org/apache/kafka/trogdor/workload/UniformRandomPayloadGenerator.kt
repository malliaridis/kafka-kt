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

package org.apache.kafka.trogdor.workload

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.Random
import kotlin.math.min

/**
 * A PayloadGenerator which generates a uniform random payload.
 *
 * This generator generates pseudo-random payloads that can be reproduced from run to run. The guarantees are the same
 * as those of java.util.Random.
 *
 * This payload generator also has the option to append padding bytes at the end of the payload. The padding bytes
 * are always the same, no matter what the position is. This is useful when simulating a partly-compressible stream
 * of user data.
 */
class UniformRandomPayloadGenerator @JsonCreator constructor(
    @param:JsonProperty("size") private val size: Int,
    @param:JsonProperty("seed") private val seed: Long,
    @param:JsonProperty("padding") private val padding: Int,
) : PayloadGenerator {

    private val random = Random()

    private val padBytes: ByteArray

    private val randomBytes: ByteArray

    init {
        if (padding < 0 || padding > size) throw RuntimeException(
            "Invalid value $padding for padding: the number of padding bytes must not be smaller than 0 or " +
                    "greater than the total payload size."
        )

        padBytes = ByteArray(padding)
        random.setSeed(seed)
        random.nextBytes(padBytes)
        randomBytes = ByteArray(size - padding)
    }

    @JsonProperty
    fun size(): Int = size

    @JsonProperty
    fun seed(): Long = seed

    @JsonProperty
    fun padding(): Int = padding

    @Synchronized
    override fun generate(position: Long): ByteArray {
        val result = ByteArray(size)
        if (randomBytes.isNotEmpty()) {
            random.setSeed(seed + position)
            random.nextBytes(randomBytes)
            System.arraycopy(randomBytes, 0, result, 0, min(randomBytes.size, result.size)
            )
        }
        if (padBytes.isNotEmpty()) {
            System.arraycopy(padBytes, 0, result, randomBytes.size, result.size - randomBytes.size)
        }
        return result
    }
}
