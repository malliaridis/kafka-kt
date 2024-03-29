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

class CipherInformation(
    cipher: String? = null,
    protocol: String? = null,
) {

    val cipher: String

    val protocol: String

    init {
        this.cipher = if (cipher.isNullOrEmpty()) "unknown" else cipher
        this.protocol = if (protocol.isNullOrEmpty()) "unknown" else protocol
    }

    override fun toString(): String = "CipherInformation(cipher=$cipher, protocol=$protocol)"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CipherInformation

        if (cipher != other.cipher) return false
        if (protocol != other.protocol) return false

        return true
    }

    override fun hashCode(): Int {
        var result = cipher.hashCode()
        result = 31 * result + protocol.hashCode()
        return result
    }
}
