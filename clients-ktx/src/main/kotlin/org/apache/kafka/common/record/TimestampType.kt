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

package org.apache.kafka.common.record

/**
 * The timestamp type of the records.
 */
enum class TimestampType(
    val id: Int,
    val altName: String,
) {

    NO_TIMESTAMP_TYPE(-1, "NoTimestampType"),

    CREATE_TIME(0, "CreateTime"),

    LOG_APPEND_TIME(1, "LogAppendTime");

    override fun toString(): String = altName

    companion object {

        fun forName(name: String): TimestampType = values().firstOrNull { it.altName == name }
                ?: throw NoSuchElementException("Invalid timestamp type $name")
    }
}
