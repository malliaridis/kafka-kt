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

import javax.security.auth.Subject

/**
 * A simple immutable value object class holding customizable SASL extensions.
 * 
 * **Note on object identity and equality**: `SaslExtensions` *intentionally* overrides the standard
 * [equals] and [hashCode] methods calling their respective [Object.equals] and [Object.hashCode]
 * implementations. In so doing, it provides equality *only* via reference identity and will not
 * base equality on the underlying values of its [extentions map][extensionsMap].
 * 
 * The reason for this approach to equality is based off of the manner in which credentials are
 * stored in a [Subject]. `SaslExtensions` are added to and removed from a [Subject] via its
 * [public credentials][Subject.getPublicCredentials]. The public credentials are stored in a [Set]
 * in the [Subject], so object equality therefore becomes a concern. With shallow, reference-based
 * equality, distinct `SaslExtensions` instances with the same map values can be considered unique.
 * This is critical to operations like token refresh.
 *
 * See [KAFKA-14062](https://issues.apache.org/jira/browse/KAFKA-14062) for more detail.
 */
open class SaslExtensions(private val extensionsMap: Map<String, String>) {

    /**
     * Returns an **immutable** map of the extension names and their values
     */
    fun map(): Map<String, String> = extensionsMap

    /**
     * Implements equals using the reference comparison implementation from [Object.equals].
     *
     * See the class-level documentation for details.
     *
     * @param other Other object to compare
     * @return True if `other == this`
     */
    override fun equals(other: Any?): Boolean = super.equals(other)

    /**
     * Implements `hashCode` using the native implementation from [Object.hashCode].
     *
     * See the class-level documentation for details.
     *
     * @return Hash code of instance
     */
    override fun hashCode(): Int = super.hashCode()

    override fun toString(): String =
        "${SaslExtensions::class.java.simpleName}[extensionsMap=$extensionsMap]"

    companion object {

        /**
         * Creates an "empty" instance indicating no SASL extensions. *Do not cache the result of
         * this method call* for use by multiple [Subject]s as the references need to be
         * unique.
         *
         * See the class-level documentation for details.
         * @return Unique, but empty, `SaslExtensions` instance
         */
        fun empty(): SaslExtensions {
            // It's ok to re-use the EMPTY_MAP instance as the object equality is on the outer
            // SaslExtensions reference.
            return SaslExtensions(emptyMap())
        }
    }
}
