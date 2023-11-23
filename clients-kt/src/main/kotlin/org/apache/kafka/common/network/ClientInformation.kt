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

class ClientInformation(
    softwareName: String? = null,
    softwareVersion: String? = null,
) {

    val softwareName: String

    val softwareVersion: String

    init {
        this.softwareName =
            if (softwareName.isNullOrEmpty()) UNKNOWN_NAME_OR_VERSION
            else softwareName
        this.softwareVersion =
            if (softwareVersion.isNullOrEmpty()) UNKNOWN_NAME_OR_VERSION
            else softwareVersion
    }

    override fun toString(): String {
        return "ClientInformation(softwareName=" + softwareName +
                ", softwareVersion=" + softwareVersion + ")"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ClientInformation

        if (softwareName != other.softwareName) return false
        if (softwareVersion != other.softwareVersion) return false

        return true
    }

    override fun hashCode(): Int {
        var result = softwareName.hashCode()
        result = 31 * result + softwareVersion.hashCode()
        return result
    }

    companion object {

        const val UNKNOWN_NAME_OR_VERSION = "unknown"

        val EMPTY = ClientInformation(UNKNOWN_NAME_OR_VERSION, UNKNOWN_NAME_OR_VERSION)
    }
}
