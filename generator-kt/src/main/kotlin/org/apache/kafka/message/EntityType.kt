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

import com.fasterxml.jackson.annotation.JsonProperty

enum class EntityType(private val baseType: FieldType?) {

    @JsonProperty("unknown")
    UNKNOWN(null),

    @JsonProperty("transactionalId")
    TRANSACTIONAL_ID(FieldType.StringFieldType.INSTANCE),

    @JsonProperty("producerId")
    PRODUCER_ID(FieldType.Int64FieldType.INSTANCE),

    @JsonProperty("groupId")
    GROUP_ID(FieldType.StringFieldType.INSTANCE),

    @JsonProperty("topicName")
    TOPIC_NAME(FieldType.StringFieldType.INSTANCE),

    @JsonProperty("brokerId")
    BROKER_ID(FieldType.Int32FieldType.INSTANCE);

    fun verifyTypeMatches(fieldName: String, type: FieldType) {
        if (this == UNKNOWN) return

        if (type is FieldType.ArrayType) verifyTypeMatches(fieldName, type.elementType)
        else if (type.toString() != baseType.toString()) throw RuntimeException(
            "Field $fieldName has entity type $name, but field type $type, which does not match."
        )
    }
}
