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

package org.apache.kafka.clients.admin.internals

import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType

class CoordinatorKey private constructor(
    val type: CoordinatorType,
    val idValue: String,
) {

    override fun toString(): String = "CoordinatorKey(idValue='$idValue', type=$type)"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CoordinatorKey

        if (type != other.type) return false
        if (idValue != other.idValue) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + idValue.hashCode()
        return result
    }

    companion object {

        fun byGroupId(groupId: String): CoordinatorKey {
            return CoordinatorKey(CoordinatorType.GROUP, groupId)
        }

        fun byTransactionalId(transactionalId: String): CoordinatorKey {
            return CoordinatorKey(CoordinatorType.TRANSACTION, transactionalId)
        }
    }
}
