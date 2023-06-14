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

package org.apache.kafka.common.security.oauthbearer.internals.unsecured

import java.util.*
import java.util.regex.Pattern

/**
 * Utility class for help dealing with
 * [Access Token Scopes](https://tools.ietf.org/html/rfc6749#section-3.3).
 */
object OAuthBearerScopeUtils {
    private val INDIVIDUAL_SCOPE_ITEM_PATTERN = Pattern.compile("[\\x23-\\x5B\\x5D-\\x7E\\x21]+")

    /**
     * Return `true` if the given value meets the definition of a valid scope item as per
     * [RFC 6749 Section 3.3](https://tools.ietf.org/html/rfc6749#section-3.3), otherwise `false`.
     *
     * @param scopeItem the mandatory scope item to check for validity
     * @return `true` if the given value meets the definition of a valid scope item, otherwise
     * `false`
     */
    fun isValidScopeItem(scopeItem: String): Boolean =
        INDIVIDUAL_SCOPE_ITEM_PATTERN.matcher(scopeItem).matches()

    /**
     * Convert a space-delimited list of scope values (for example, `"scope1 scope2"`) to a List
     * containing the individual elements (`"scope1"` and `"scope2"`).
     *
     * @param spaceDelimitedScope the mandatory (but possibly empty) space-delimited scope values,
     * each of which must be valid according to [isValidScopeItem]
     * @return the list of the given (possibly empty) space-delimited values
     * @throws OAuthBearerConfigException if any of the individual scope values are
     * malformed/illegal
     */
    @Throws(OAuthBearerConfigException::class)
    fun parseScope(spaceDelimitedScope: String): List<String> {
        return spaceDelimitedScope.split(" ".toRegex())
            .dropLastWhile { it.isEmpty() }
            .mapNotNull { individualScopeItem ->

            if (individualScopeItem.isNotEmpty()) {
                if (!isValidScopeItem(individualScopeItem)) throw OAuthBearerConfigException(
                    String.format("Invalid scope value: %s", individualScopeItem)
                )
                individualScopeItem
            }
            null
        }
    }
}
