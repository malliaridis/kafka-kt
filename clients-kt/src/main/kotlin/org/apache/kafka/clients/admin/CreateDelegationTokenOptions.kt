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

package org.apache.kafka.clients.admin

import java.util.LinkedList
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.security.auth.KafkaPrincipal

/**
 * Options for [Admin.createDelegationToken].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class CreateDelegationTokenOptions : AbstractOptions<CreateDelegationTokenOptions>() {

    var maxLifeTimeMs: Long = -1

    var renewers: List<KafkaPrincipal> = LinkedList()

    var owner: KafkaPrincipal? = null

    @Deprecated("User property instead")
    fun renewers(renewers: List<KafkaPrincipal>): CreateDelegationTokenOptions {
        this.renewers = renewers
        return this
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("renewers"),
    )
    fun renewers(): List<KafkaPrincipal> = renewers

    @Deprecated("User property instead")
    fun owner(owner: KafkaPrincipal?): CreateDelegationTokenOptions {
        this.owner = owner
        return this
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("owner"),
    )
    fun owner(): KafkaPrincipal? = owner

    @Deprecated("User property instead")
    fun maxlifeTimeMs(maxLifeTimeMs: Long): CreateDelegationTokenOptions {
        this.maxLifeTimeMs = maxLifeTimeMs
        return this
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("maxLifeTimeMs"),
    )
    fun maxlifeTimeMs(): Long = maxLifeTimeMs
}
