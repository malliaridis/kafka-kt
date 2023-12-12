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
 * Simple utility class to perform basic cleaning and validation on input values so that they're
 * performed consistently throughout the code base.
 */
object ClaimValidationUtils {

    /**
     * Validates that the scopes are valid, where *invalid* means *any* of the following:
     *
     * - Collection is `null`
     * - Collection has duplicates
     * - Any of the elements in the collection are `null`
     * - Any of the elements in the collection are zero length
     * - Any of the elements in the collection are whitespace only
     *
     * @param scopeClaimName Name of the claim used for the scope values
     * @param scopes Collection of String scopes
     *
     * @return Unmodifiable [Set] that includes the values of the original set, but with each value
     * trimmed
     * @throws ValidateException Thrown if the value contains duplicates, or if any of the values in
     * the set are empty or whitespace only
     */
    @Throws(ValidateException::class)
    fun validateScopes(scopeClaimName: String, scopes: Collection<String>): Set<String> {
        val copy = mutableSetOf<String>()
        scopes.forEach { scope ->
            val validatedScope = validateString(scopeClaimName, scope)
            if (copy.contains(validatedScope)) throw ValidateException(
                "$scopeClaimName value must not contain duplicates - $validatedScope already present"
            )
            copy.add(validatedScope)
        }
        return copy.toSet()
    }

    /**
     * Validates that the given lifetime is valid, where *invalid* means *any* of the following:
     *
     * - `null`
     * - Negative
     *
     * @param claimName  Name of the claim
     * @param claimValue Expiration time (in milliseconds)
     * @return Input parameter, as provided
     * @throws ValidateException Thrown if the value is `null` or negative
     */
    @Throws(ValidateException::class)
    fun validateExpiration(claimName: String, claimValue: Long?): Long {
        if (claimValue == null)
            throw ValidateException("$claimName value must be non-null")

        if (claimValue < 0)
            throw ValidateException("$claimName value must be non-negative; value given was \"$claimValue\"")
        return claimValue
    }

    /**
     * Validates that the given claim value is valid, where *invalid* means *any* of
     * the following:
     *
     * - `null`
     * - Zero length
     * - Whitespace only
     *
     * @param claimName  Name of the claim
     * @param claimValue Name of the subject
     * @return Trimmed version of the `claimValue` parameter
     * @throws ValidateException Thrown if the value is `null`, empty, or whitespace only
     */
    @Throws(ValidateException::class)
    fun validateSubject(claimName: String, claimValue: String?): String {
        return validateString(claimName, claimValue)
    }

    /**
     * Validates that the given issued at claim name is valid, where *invalid* means *any* of
     * the following:
     *
     * - Negative
     *
     * @param claimName  Name of the claim
     * @param claimValue Start time (in milliseconds) or `null` if not used
     * @return Input parameter, as provided
     * @throws ValidateException Thrown if the value is negative
     */
    @Throws(ValidateException::class)
    fun validateIssuedAt(claimName: String, claimValue: Long?): Long? {
        if (claimValue != null && claimValue < 0) throw ValidateException(
            "$claimName value must be null or non-negative; value given was \"$claimValue\""
        )
        return claimValue
    }

    /**
     * Validates that the given claim name override is valid, where *invalid* means *any* of the
     * following:
     *
     * - Zero length
     * - Whitespace only
     *
     * @param name  "Standard" name of the claim, e.g. `sub`
     * @param value "Override" name of the claim, e.g. `email`
     *
     * @return Trimmed version of the `value` parameter
     *
     * @throws ValidateException Thrown if the value is empty, or whitespace only
     */
    @Throws(ValidateException::class)
    fun validateClaimNameOverride(name: String, value: String): String = validateString(name, value)

    @Throws(ValidateException::class)
    private fun validateString(name: String, value: String?): String {
        if (value == null) throw ValidateException("$name value must be non-null")

        if (value.isEmpty()) throw ValidateException("$name value must be non-empty")
        val updatedValue = value.trim { it <= ' ' }
        if (updatedValue.isEmpty()) throw ValidateException("$name value must not contain only whitespace")
        return updatedValue
    }
}
