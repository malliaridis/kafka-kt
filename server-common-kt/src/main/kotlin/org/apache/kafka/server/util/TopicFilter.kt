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

package org.apache.kafka.server.util

import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException
import org.apache.kafka.common.internals.Topic.isInternal
import org.slf4j.LoggerFactory

abstract class TopicFilter(rawRegex: String) {

    protected val regex: String

    var pattern: Pattern? = null

    init {
        regex = rawRegex
            .trim { it <= ' ' }
            .replace(',', '|')
            .replace(" ", "")
            .replace("^[\"']+".toRegex(), "")
            .replace("[\"']+$".toRegex(), "") // property files may bring quotes
        try {
            pattern = Pattern.compile(regex)
        } catch (e: PatternSyntaxException) {
            throw RuntimeException("$regex is an invalid regex.")
        }
    }

    abstract fun isTopicAllowed(topic: String, excludeInternalTopics: Boolean): Boolean

    override fun toString(): String = regex

    class IncludeList(rawRegex: String) : TopicFilter(rawRegex) {

        private val log = LoggerFactory.getLogger(IncludeList::class.java)

        override fun isTopicAllowed(topic: String, excludeInternalTopics: Boolean): Boolean {
            val allowed = topic.matches(regex.toRegex()) && !(isInternal(topic) && excludeInternalTopics)

            if (allowed) log.debug("{} allowed", topic)
            else log.debug("{} filtered", topic)

            return allowed
        }
    }
}
