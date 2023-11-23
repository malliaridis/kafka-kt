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

import java.io.UnsupportedEncodingException
import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import javax.management.ObjectName
import org.apache.kafka.common.KafkaException

/**
 * Utility class for sanitizing/desanitizing/quoting values used in JMX metric names
 * or as ZooKeeper node name.
 *
 * User principals and client-ids are URL-encoded using ([sanitize] for use as ZooKeeper node names.
 * User principals are URL-encoded in all metric names as well. All other metric tags including
 * client-id are quoted if they contain special characters using [jmxSanitize] when registering in
 * JMX.
 */
object Sanitizer {

    /**
     * Even though only a small number of characters are disallowed in JMX, quote any string
     * containing special characters to be safe. All characters in strings sanitized using
     * [sanitize] are safe for JMX and hence included here.
     */
    private val MBEAN_PATTERN = Regex("[\\w-%. \t]*")

    /**
     * Sanitize `name` for safe use as JMX metric name as well as ZooKeeper node name using
     * URL-encoding.
     */
    fun sanitize(name: String?): String {
        val encoded: String
        return try {
            encoded = URLEncoder.encode(name, StandardCharsets.UTF_8.name())
            val builder = StringBuilder()

            for (element in encoded) when (element) {
                '*' -> builder.append("%2A") // Metric ObjectName treats * as pattern
                '+' -> builder.append("%20") // Space URL-encoded as +, replace with percent encoding
                else -> builder.append(element)
            }

            builder.toString()
        } catch (e: UnsupportedEncodingException) {
            throw KafkaException(cause = e)
        }
    }

    /**
     * Desanitize name that was URL-encoded using [sanitize]. This is used to obtain the desanitized
     * version of node names in ZooKeeper.
     */
    fun desanitize(name: String?): String {
        return try {
            URLDecoder.decode(name, StandardCharsets.UTF_8.name())
        } catch (e: UnsupportedEncodingException) {
            throw KafkaException(cause = e)
        }
    }

    /**
     * Quote `name` using [ObjectName.quote] if `name` contains characters that are not safe for use
     * in JMX. User principals that are already sanitized using [sanitize] will not be quoted since
     * they are safe for JMX.
     */
    fun jmxSanitize(name: String?): String =
        if (name?.let { MBEAN_PATTERN.matches(it) } == true) name
        else ObjectName.quote(name)
}
