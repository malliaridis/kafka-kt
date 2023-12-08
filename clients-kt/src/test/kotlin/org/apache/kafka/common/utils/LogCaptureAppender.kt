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

import java.util.LinkedList
import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.spi.LoggingEvent

class LogCaptureAppender : AppenderSkeleton(), AutoCloseable {

    private val events = mutableListOf<LoggingEvent>()

    data class Event(
        val level: String,
        val message: String,
        val throwableInfo: String?,
    )

    override fun append(event: LoggingEvent) {
        synchronized(events) { events.add(event) }
    }

    val messages: List<String>
        get() {
            val result = LinkedList<String>()
            synchronized(events) {
                for (event in events) {
                    result.add(event.getRenderedMessage())
                }
            }
            return result
        }

    fun getEvents(): List<Event> {
        val result = LinkedList<Event>()
        synchronized(events) {
            for (event in events) {
                val throwableStrRep = event.throwableStrRep
                val throwableString = throwableStrRep?.let {
                    val throwableStringBuilder = StringBuilder()
                    for (s in throwableStrRep) throwableStringBuilder.append(s)
                    throwableStringBuilder.toString()
                }
                result.add(
                    Event(
                        level = event.getLevel().toString(),
                        message = event.getRenderedMessage(),
                        throwableInfo = throwableString,
                    )
                )
            }
        }
        return result
    }

    override fun close() = unregister(this)

    override fun requiresLayout(): Boolean = false

    companion object {

        fun createAndRegister(): LogCaptureAppender {
            val logCaptureAppender = LogCaptureAppender()
            Logger.getRootLogger().addAppender(logCaptureAppender)
            return logCaptureAppender
        }

        fun createAndRegister(clazz: Class<*>?): LogCaptureAppender {
            val logCaptureAppender = LogCaptureAppender()
            Logger.getLogger(clazz).addAppender(logCaptureAppender)
            return logCaptureAppender
        }

        fun setClassLoggerToDebug(clazz: Class<*>?) {
            Logger.getLogger(clazz).level = Level.DEBUG
        }

        fun setClassLoggerToTrace(clazz: Class<*>?) {
            Logger.getLogger(clazz).level = Level.TRACE
        }

        fun unregister(logCaptureAppender: LogCaptureAppender?) {
            Logger.getRootLogger().removeAppender(logCaptureAppender)
        }
    }
}
