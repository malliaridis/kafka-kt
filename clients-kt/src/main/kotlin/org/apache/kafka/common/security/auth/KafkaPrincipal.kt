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

package org.apache.kafka.common.security.auth

import java.security.Principal

/**
 * Principals in Kafka are defined by a type and a name. The principal type will always be `"User"`
 * for the simple authorizer that is enabled by default, but custom authorizers can leverage
 * different principal types (such as to enable group or role-based ACLs). The
 * [KafkaPrincipalBuilder] interface is used when you need to derive a different principal type from
 * the authentication context, or when you need to represent relations between different principals.
 * For example, you could extend [KafkaPrincipal] in order to link a user principal to one or more
 * role principals.
 *
 * For custom extensions of [KafkaPrincipal], there two key points to keep in mind:
 *
 * 1. To be compatible with the ACL APIs provided by Kafka (including the command line tool), each
 *    ACL can only represent a permission granted to a single principal (consisting of a principal
 *    type and name). It is possible to use richer ACL semantics, but you must implement your own
 *    mechanisms for adding and removing ACLs.
 * 2. In general, [KafkaPrincipal] extensions are only useful when the corresponding Authorizer is
 *    also aware of the extension. If you have a [KafkaPrincipalBuilder] which derives user groups
 *    from the authentication context (e.g. from an SSL client certificate), then you need a custom
 *    authorizer which is capable of using the additional group information.
 */
data class KafkaPrincipal(
    val principalType: String,
    @get:JvmName("getPrincipalName") // workaround due to naming conflict
    val name: String,
) : Principal {

    @field:Volatile
    var tokenAuthenticated: Boolean = false

    constructor(
        principalType: String,
        name: String,
        tokenAuthenticated: Boolean = false
    ) : this(
        principalType = principalType,
        name = name,
    ) {
        this.tokenAuthenticated = tokenAuthenticated
    }

    override fun toString(): String = "$principalType:$name"

    override fun getName(): String = name

    fun tokenAuthenticated(tokenAuthenticated: Boolean) {
        this.tokenAuthenticated = tokenAuthenticated
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenAuthenticated")
    )
    fun tokenAuthenticated(): Boolean = tokenAuthenticated

    companion object {

        const val USER_TYPE = "User"

        val ANONYMOUS = KafkaPrincipal(USER_TYPE, "ANONYMOUS")
    }
}
