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

package org.apache.kafka.common.quota

/**
 * Describes a configuration alteration to be made to a client quota entity.
 *
 * @property entity the entity whose config will be modified
 * @property ops the alteration to perform
 */
class ClientQuotaAlteration(
    private val entity: ClientQuotaEntity,
    private val ops: Collection<Op>,
) {

    /**
     * @property key the quota type to alter
     * @property value if set then the existing value is updated, otherwise if `null`, the existing
     * value is cleared
     */
    class Op(
        val key: String,
        val value: Double? = null,
    ) {

        /**
         * @return the quota type to alter
         */
        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("key"),
        )
        fun key(): String = key

        /**
         * @return if set then the existing value is updated,
         * otherwise if null, the existing value is cleared
         */
        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("value"),
        )
        fun value(): Double? = value

        override fun toString(): String = "ClientQuotaAlteration.Op(key=$key, value=$value)"
    }

    /**
     * @return the entity whose config will be modified
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("entity"),
    )
    fun entity(): ClientQuotaEntity = entity

    /**
     * @return the alteration to perform
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("ops"),
    )
    fun ops(): Collection<Op> = ops

    override fun toString(): String = "ClientQuotaAlteration(entity=$entity, ops=$ops)"
}
