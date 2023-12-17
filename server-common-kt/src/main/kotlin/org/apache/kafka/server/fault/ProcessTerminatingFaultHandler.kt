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

package org.apache.kafka.server.fault

import java.util.Objects
import org.apache.kafka.common.utils.Exit.exit
import org.apache.kafka.common.utils.Exit.halt
import org.slf4j.LoggerFactory

/**
 * This is a fault handler which terminates the JVM process.
 */
class ProcessTerminatingFaultHandler private constructor(
    private val shouldHalt: Boolean,
    private val action: Runnable,
) : FaultHandler {

    override fun handleFault(failureMessage: String, cause: Throwable?): RuntimeException? {
        if (cause == null) log.error("Encountered fatal fault: {}", failureMessage)
        else log.error("Encountered fatal fault: {}", failureMessage, cause)

        try {
            action.run()
        } catch (e: Throwable) {
            log.error("Failed to run terminating action.", e)
        }

        val statusCode = 1
        if (shouldHalt) halt(statusCode)
        else exit(statusCode)

        return null
    }

    class Builder {

        private var shouldHalt = true

        private var action = Runnable {}

        /**
         * Set if halt or exit should be used.
         *
         * When `value` is `false` `Exit.exit` is called, otherwise `Exit.halt` is called. The default value is `true`.
         *
         * The default implementation of `Exit.exit` calls `Runtime.exit` which blocks on all of the shutdown hooks
         * executing.
         *
         * The default implementation of `Exit.halt` calls `Runtime.halt` which forcibly terminates the JVM.
         */
        fun setShouldHalt(value: Boolean): Builder {
            shouldHalt = value
            return this
        }

        /**
         * Set the `Runnable` to run when handling a fault.
         */
        fun setAction(action: Runnable): Builder {
            this.action = Objects.requireNonNull(action)
            return this
        }

        fun build(): ProcessTerminatingFaultHandler {
            return ProcessTerminatingFaultHandler(shouldHalt, action)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ProcessTerminatingFaultHandler::class.java)
    }
}
