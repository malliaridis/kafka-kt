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

package org.apache.kafka.common.security.scram.internals

import java.util.*
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.ScramCredential

/**
 * SCRAM Credential persistence utility functions. Implements format conversion used for the
 * credential store implemented in Kafka. Credentials are persisted as a comma-separated String
 * of key-value pairs:
 *
 * ```
 * salt=*salt*,stored_key=*stored_key*,server_key=*server_key*,iterations=*iterations*
 * ```
 */
object ScramCredentialUtils {

    private const val SALT = "salt"

    private const val STORED_KEY = "stored_key"

    private const val SERVER_KEY = "server_key"

    private const val ITERATIONS = "iterations"

    fun credentialToString(credential: ScramCredential): String {
        return String.format(
            "%s=%s,%s=%s,%s=%s,%s=%d",
            SALT,
            Base64.getEncoder().encodeToString(credential.salt),
            STORED_KEY,
            Base64.getEncoder().encodeToString(credential.storedKey),
            SERVER_KEY,
            Base64.getEncoder().encodeToString(credential.serverKey),
            ITERATIONS,
            credential.iterations
        )
    }

    fun credentialFromString(str: String): ScramCredential {
        val props = toProps(str)
        require(
            !(props.size != 4
                    || !props.containsKey(SALT)
                    || !props.containsKey(STORED_KEY)
                    || !props.containsKey(SERVER_KEY)
                    || !props.containsKey(ITERATIONS))
        ) { "Credentials not valid: $str" }

        val salt = Base64.getDecoder().decode(props.getProperty(SALT))
        val storedKey = Base64.getDecoder().decode(props.getProperty(STORED_KEY))
        val serverKey = Base64.getDecoder().decode(props.getProperty(SERVER_KEY))
        val iterations = props.getProperty(ITERATIONS).toInt()

        return ScramCredential(salt, storedKey, serverKey, iterations)
    }

    private fun toProps(str: String): Properties {
        val props = Properties()
        val tokens = str.split(",".toRegex())
            .dropLastWhile { it.isEmpty() }
            .toTypedArray()

        tokens.forEach { token ->
            val index = token.indexOf('=')
            require(index > 0) { "Credentials not valid: $str" }
            props[token.substring(0, index)] = token.substring(index + 1)
        }

        return props
    }

    fun createCache(cache: CredentialCache, mechanisms: Collection<String>) {
        ScramMechanism.mechanismNames.forEach { mechanism ->
            if (mechanisms.contains(mechanism))
                cache.createCache(mechanism, ScramCredential::class.java)
        }
    }
}
