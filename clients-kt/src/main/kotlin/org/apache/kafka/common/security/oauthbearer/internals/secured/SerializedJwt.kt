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

package org.apache.kafka.common.security.oauthbearer.internals.secured

/**
 * A JWT contains three sections (header, payload, and signature) that are seperated by a dot.
 */
private const val JWT_SECTION_COUNT = 3

/**
 * SerializedJwt provides a modicum of structure and validation around a JWT's serialized form by
 * splitting and making the three sections (header, payload, and signature) available to the user.
 */
class SerializedJwt(token: String?) {

    /**
     * Returns the entire base 64-encoded JWT.
     *
     * @return JWT
     */
    val token: String

    /**
     * Returns the first section--the JWT header--in its base 64-encoded form.
     *
     * @return Header section of the JWT
     */
    val header: String

    /**
     * Returns the second section--the JWT payload--in its base 64-encoded form.
     *
     * @return Payload section of the JWT
     */
    val payload: String

    /**
     * Returns the third section--the JWT signature--in its base 64-encoded form.
     *
     * @return Signature section of the JWT
     */
    val signature: String

    init {
        val trimmedToken = token?.trim { it <= ' ' } ?: ""
        if (trimmedToken.isEmpty()) throw ValidateException(
            "Empty JWT provided; expected three sections (header, payload, and signature)"
        )

        val splits = trimmedToken.split("\\.".toRegex())
            .dropLastWhile { it.isEmpty() }
            .toTypedArray()

        if (splits.size != JWT_SECTION_COUNT) throw ValidateException(
            String.format(
                "Malformed JWT provided (%s); expected three sections (header, payload, and " +
                        "signature), but %s sections provided",
                trimmedToken,
                splits.size
            )
        )

        this.token = trimmedToken
        header = validateSection(splits[0], "header")
        payload = validateSection(splits[1], "payload")
        signature = validateSection(splits[2], "signature")
    }

    @Throws(ValidateException::class)
    private fun validateSection(section: String, sectionName: String): String {
        val trimmedSection = section.trim { it <= ' ' }

        if (trimmedSection.isEmpty()) throw ValidateException(
            String.format(
                "Malformed JWT provided; expected at least three sections (header, payload, and " +
                        "signature), but %s section missing",
                sectionName
            )
        )
        return trimmedSection
    }
}
