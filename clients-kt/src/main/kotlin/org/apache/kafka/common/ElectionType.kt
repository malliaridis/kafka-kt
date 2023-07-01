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

package org.apache.kafka.common

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * Options for [org.apache.kafka.clients.admin.Admin.electLeaders].
 *
 * The API of this class is evolving, see [org.apache.kafka.clients.admin.Admin] for details.
 */
@Evolving
enum class ElectionType(val value: Byte) {
    PREFERRED(0.toByte()),
    UNCLEAN(1.toByte());

    companion object {
        fun valueOf(value: Byte): ElectionType {
            return when (value) {
                PREFERRED.value -> PREFERRED
                UNCLEAN.value -> UNCLEAN
                else -> throw IllegalArgumentException(
                    String.format("Value %s must be one of %s", value, values())
                )
            }
        }
    }
}
