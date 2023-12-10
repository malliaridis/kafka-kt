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

package org.apache.kafka.message

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class MessageSpec @JsonCreator constructor(
    @JsonProperty("name") name: String,
    @JsonProperty("validVersions") validVersions: String?,
    @JsonProperty("fields") fields: List<FieldSpec>?,
    @JsonProperty("apiKey") val apiKey: Short?,
    @JsonProperty("type") val type: MessageSpecType,
    @JsonProperty("commonStructs") commonStructs: List<StructSpec>?,
    @JsonProperty("flexibleVersions") flexibleVersions: String?,
    @JsonProperty("listeners") val listeners: List<RequestListenerType>?,
    @JsonProperty("latestVersionUnstable") val latestVersionUnstable: Boolean,
) {

    val struct: StructSpec = StructSpec(name, validVersions, fields)

    @JsonProperty("commonStructs")
    val commonStructs: List<StructSpec> = commonStructs?.toList() ?: emptyList()

    val flexibleVersions: Versions

    init {
        if (flexibleVersions == null) throw RuntimeException(
            "You must specify a value for flexibleVersions. Please use 0+ for all new messages."
        )
        this.flexibleVersions = Versions.parse(flexibleVersions, Versions.NONE)!!
        if (!this.flexibleVersions.isEmpty && this.flexibleVersions.highest < Short.MAX_VALUE)
            throw RuntimeException(
                "Field $name specifies flexibleVersions ${this.flexibleVersions}, which is not " +
                        "open-ended. flexibleVersions must be either none, or an open-ended " +
                        "range (that ends with a plus sign)."
            )

        if (!listeners.isNullOrEmpty() && (type != MessageSpecType.REQUEST)) throw RuntimeException(
            "The `requestScope` property is only valid for messages with type `request`"
        )

        if (latestVersionUnstable == true && type != MessageSpecType.REQUEST) throw RuntimeException(
            "The `latestVersionUnstable` property is only valid for messages with type `request`"
        )
    }

    @get:JsonProperty("name")
    val name: String
        get() = struct.name

    val validVersions: Versions
        get() = struct.versions

    @get:JsonProperty("validVersions")
    val validVersionsString: String
        get() = struct.versionsString

    @get:JsonProperty("fields")
    val fields: List<FieldSpec>
        get() = struct.fields

    @JsonProperty("flexibleVersions")
    fun flexibleVersionsString(): String = flexibleVersions.toString()

    fun dataClassName(): String = when (type) {
        MessageSpecType.HEADER,
        MessageSpecType.REQUEST,
        MessageSpecType.RESPONSE,
        ->
            // We append the Data suffix to request/response/header classes to avoid
            // collisions with existing objects. This can go away once the protocols
            // have all been converted and we begin using the generated types directly.
            struct.name + "Data"

        else -> struct.name
    }
}
