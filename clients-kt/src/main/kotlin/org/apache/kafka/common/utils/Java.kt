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

package org.apache.kafka.common.utils

import java.util.*

object Java {

    /**
     * Java 9 major version number.
     */
    private const val JAVA_VERSION_9 = 9

    /**
     * Java 11 major version number.
     */
    private const val JAVA_VERSION_11 = 11

    private val VERSION = parseVersion(System.getProperty("java.specification.version"))

    // Package private for testing
    internal fun parseVersion(versionString: String?): Version {
        val st = StringTokenizer(versionString, ".")
        val majorVersion = st.nextToken().toInt()
        val minorVersion: Int = if (st.hasMoreTokens()) st.nextToken().toInt() else 0
        return Version(majorVersion, minorVersion)
    }

    // Having these as static final provides the best opportunity for compiler optimization
    val IS_JAVA9_COMPATIBLE = VERSION.isJava9Compatible

    val IS_JAVA11_COMPATIBLE = VERSION.isJava11Compatible

    val isIbmJdk: Boolean
        get() = System.getProperty("java.vendor").contains("IBM")

    val isIbmJdkSemeru: Boolean
        get() = isIbmJdk && System.getProperty("java.runtime.name", "").contains("Semeru")

    // Package private for testing
    internal data class Version(
        val majorVersion: Int,
        val minorVersion: Int,
    ) {
        override fun toString(): String =
            "Version(majorVersion=$majorVersion, minorVersion=$minorVersion)"

        val isJava9Compatible: Boolean
            // Package private for testing
            get() = majorVersion >= JAVA_VERSION_9

        val isJava11Compatible: Boolean
            get() = majorVersion >= JAVA_VERSION_11
    }
}
