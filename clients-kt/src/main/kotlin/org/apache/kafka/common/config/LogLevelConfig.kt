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

package org.apache.kafka.common.config

/**
 * This class holds definitions for log level configurations related to Kafka's application logging.
 * See KIP-412 for additional information
 */
object LogLevelConfig {

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL
     * BREAK USER CODE.
     */

    /**
     * The `FATAL` level designates a very severe error that will lead the Kafka broker to abort.
     */
    const val FATAL_LOG_LEVEL = "FATAL"

    /**
     * The `ERROR` level designates error events that might still allow the broker to continue
     * running.
     */
    const val ERROR_LOG_LEVEL = "ERROR"

    /**
     * The `WARN` level designates potentially harmful situations.
     */
    const val WARN_LOG_LEVEL = "WARN"

    /**
     * The `INFO` level designates informational messages that highlight normal Kafka events at a
     * coarse-grained level.
     */
    const val INFO_LOG_LEVEL = "INFO"

    /**
     * The `DEBUG` level designates fine-grained informational events that are most useful to debug
     * Kafka.
     */
    const val DEBUG_LOG_LEVEL = "DEBUG"

    /**
     * The `TRACE` level designates finer-grained informational events than the `DEBUG` level.
     */
    const val TRACE_LOG_LEVEL = "TRACE"

    val VALID_LOG_LEVELS: Set<String> = setOf(
        FATAL_LOG_LEVEL,
        ERROR_LOG_LEVEL,
        WARN_LOG_LEVEL,
        INFO_LOG_LEVEL,
        DEBUG_LOG_LEVEL,
        TRACE_LOG_LEVEL
    )
}
