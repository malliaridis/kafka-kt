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

package org.apache.kafka.common.security.token.delegation.internals

import org.apache.kafka.common.security.scram.ScramCredentialCallback

data class DelegationTokenCredentialCallback(
    var tokenOwner: String? = null,
    var tokenExpiryTimestamp: Long? = null,
) : ScramCredentialCallback() {

    @Deprecated("Use property instead")
    fun tokenOwner(tokenOwner: String?) {
        this.tokenOwner = tokenOwner
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenOwner"),
    )
    fun tokenOwner(): String? = tokenOwner

    @Deprecated("Use property instead")
    fun tokenExpiryTimestamp(tokenExpiryTimestamp: Long?) {
        this.tokenExpiryTimestamp = tokenExpiryTimestamp
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenExpiryTimestamp"),
    )
    fun tokenExpiryTimestamp(): Long? {
        return tokenExpiryTimestamp
    }
}
