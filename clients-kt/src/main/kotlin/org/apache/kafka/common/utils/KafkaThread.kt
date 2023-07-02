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

import java.lang.Thread.UncaughtExceptionHandler
import org.slf4j.LoggerFactory

/**
 * A wrapper for Thread that sets things up nicely
 */
open class KafkaThread : Thread {

    private val log = LoggerFactory.getLogger(javaClass)

    constructor(name: String, daemon: Boolean) : super(name) {
        configureThread(name, daemon)
    }

    constructor(name: String, runnable: Runnable?, daemon: Boolean) : super(runnable, name) {
        configureThread(name, daemon)
    }

    private fun configureThread(name: String, daemon: Boolean) {
        isDaemon = daemon
        uncaughtExceptionHandler = UncaughtExceptionHandler { _, exception ->
            log.error("Uncaught exception in thread '{}':", name, exception)
        }
    }

    companion object {

        fun daemon(name: String, runnable: Runnable?): KafkaThread =
            KafkaThread(name, runnable, true)

        fun nonDaemon(name: String, runnable: Runnable?): KafkaThread =
            KafkaThread(name, runnable, false)
    }
}
