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

/**
 * A request to alter a user's SASL/SCRAM credentials.
 *
 * @see [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API)
 */
abstract class UserScramCredentialAlteration protected constructor(val user: String) {

    /**
     * @return the always non-null user
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("user"),
    )
    fun user(): String = user
}
