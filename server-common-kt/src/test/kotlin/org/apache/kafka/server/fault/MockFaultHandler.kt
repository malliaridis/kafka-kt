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

import org.slf4j.LoggerFactory

/**
 * This is a fault handler suitable for use in JUnit tests. It will store the result of the first
 * call to handleFault that was made.
 */
class MockFaultHandler(private val name: String) : FaultHandler {

    private var firstException: FaultHandlerException? = null

    private var ignore = false

    @Synchronized
    override fun handleFault(failureMessage: String, cause: Throwable?): RuntimeException? {
        if (cause == null) log.error("Encountered {} fault: {}", name, failureMessage)
        else log.error("Encountered {} fault: {}", name, failureMessage, cause)

        val e =
            if (cause == null) FaultHandlerException("$name: $failureMessage")
            else FaultHandlerException("$name: $failureMessage: ${cause.message}", cause)

        if (firstException == null) firstException = e
        return firstException
    }

    @Synchronized
    fun maybeRethrowFirstException() {
        firstException?.let { if (!ignore) throw it }
    }

    @Synchronized
    fun firstException(): FaultHandlerException? = firstException

    @Synchronized
    fun setIgnore(ignore: Boolean) {
        this.ignore = ignore
    }

    companion object {
        private val log = LoggerFactory.getLogger(MockFaultHandler::class.java)
    }
}
