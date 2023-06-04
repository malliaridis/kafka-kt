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

package org.apache.kafka.clients.consumer

import java.io.Serializable
import org.apache.kafka.common.requests.OffsetFetchResponse

/**
 * The Kafka offset commit API allows users to provide additional metadata (in the form of a string)
 * when an offset is committed. This can be useful (for example) to store information about which
 * node made the commit, what time the commit was made, etc.
 *
 * Constructor constructs a new [OffsetAndMetadata] object for committing through [KafkaConsumer].
 * @property offset The offset to be committed
 * @property leaderEpoch The leader epoch or `null` if not known
 * @property metadata Non-null metadata
 */
class OffsetAndMetadata(
    val offset: Long,
    // We use null to represent the absence of a leader epoch to simplify serialization.
    // I.e., older serializations of this class which do not have this field will automatically
    // initialize its value to null.
    val leaderEpoch: Int? = null,
    // The server converts null metadata to an empty string. So we store it as an empty string
    // as well on the client to be consistent.
    val metadata: String = OffsetFetchResponse.NO_METADATA
) : Serializable {

    init {
        require(offset >= 0) { "Invalid negative offset" }
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("offset"),
    )
    fun offset(): Long {
        return offset
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("metadata"),
    )
    fun metadata(): String {
        return metadata
    }

    /**
     * Get the leader epoch of the previously consumed record (if one is known). Log truncation is
     * detected if there exists a leader epoch which is larger than this epoch and begins at an
     * offset earlier than the committed offset.
     *
     * @return the leader epoch or empty if not known
     */
    fun leaderEpoch(): Int? {
        return if (leaderEpoch == null || leaderEpoch < 0) null else leaderEpoch
    }

    override fun toString(): String {
        return "OffsetAndMetadata{" +
                "offset=$offset" +
                ", leaderEpoch=$leaderEpoch" +
                ", metadata='$metadata'" +
                '}'
    }

    companion object {
        private const val serialVersionUID = 2019555404968089681L
    }
}
