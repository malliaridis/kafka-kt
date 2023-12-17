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
 * A fault handler which logs an error message and executes a runnable.
 */
class LoggingFaultHandler(
    private val type: String,
    private val action: Runnable,
) : FaultHandler {

    override fun handleFault(failureMessage: String, cause: Throwable?): RuntimeException? {
        if (cause == null) log.error("Encountered {} fault: {}", type, failureMessage)
        else log.error("Encountered {} fault: {}", type, failureMessage, cause)
        try {
            action.run()
        } catch (e: Throwable) {
            log.error("Failed to run LoggingFaultHandler action.", e)
        }
        return FaultHandlerException(failureMessage, cause)
    }

    companion object {
        private val log = LoggerFactory.getLogger(LoggingFaultHandler::class.java)
    }
}
