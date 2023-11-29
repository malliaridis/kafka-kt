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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.slf4j.helpers.MessageFormatter
import org.slf4j.spi.LocationAwareLogger

/**
 * This class provides a way to instrument loggers with a common context which can be used to
 * automatically enrich log messages. For example, in the KafkaConsumer, it is often useful to know
 * the groupId of the consumer, so this can be added to a context object which can then be passed to
 * all the dependent components in order to build new loggers. This removes the need to manually add
 * the groupId to each message.
 */
class LogContext(logPrefix: String? = "") {

    val logPrefix: String = logPrefix ?: ""

    fun logger(clazz: Class<*>): Logger {
        val logger = LoggerFactory.getLogger(clazz)
        return if (logger is LocationAwareLogger) LocationAwareKafkaLogger(logPrefix, logger)
        else LocationIgnorantKafkaLogger(logPrefix, logger)
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("logPrefix")
    )
    fun logPrefix(): String = logPrefix

    private abstract class AbstractKafkaLogger protected constructor(
        private val prefix: String,
    ) : Logger {

        protected fun addPrefix(message: String): String = prefix + message
    }

    private class LocationAwareKafkaLogger(
        logPrefix: String,
        private val logger: LocationAwareLogger,
    ) : AbstractKafkaLogger(logPrefix) {

        private val fqcn: String= LocationAwareKafkaLogger::class.java.name

        override fun getName(): String = logger.name

        override fun isTraceEnabled(): Boolean = logger.isTraceEnabled

        override fun isTraceEnabled(marker: Marker): Boolean = logger.isTraceEnabled(marker)

        override fun isDebugEnabled(): Boolean = logger.isDebugEnabled

        override fun isDebugEnabled(marker: Marker): Boolean = logger.isDebugEnabled(marker)

        override fun isInfoEnabled(): Boolean = logger.isInfoEnabled

        override fun isInfoEnabled(marker: Marker): Boolean = logger.isInfoEnabled(marker)

        override fun isWarnEnabled(): Boolean = logger.isWarnEnabled

        override fun isWarnEnabled(marker: Marker): Boolean = logger.isWarnEnabled(marker)

        override fun isErrorEnabled(): Boolean = logger.isErrorEnabled

        override fun isErrorEnabled(marker: Marker): Boolean = logger.isErrorEnabled(marker)

        override fun trace(message: String) {
            if (logger.isTraceEnabled) writeLog(
                level = LocationAwareLogger.TRACE_INT,
                format = message,
            )
        }

        override fun trace(format: String, arg: Any?) {
            if (logger.isTraceEnabled) writeLog(
                level = LocationAwareLogger.TRACE_INT,
                format = format,
                args = arrayOf(arg),
            )
        }

        override fun trace(format: String, arg1: Any?, arg2: Any?) {
            if (logger.isTraceEnabled) writeLog(
                level = LocationAwareLogger.TRACE_INT,
                format = format,
                args = arrayOf(arg1, arg2),
            )
        }

        override fun trace(format: String, vararg args: Any?) {
            if (logger.isTraceEnabled) writeLog(
                level = LocationAwareLogger.TRACE_INT,
                format = format,
                args = arrayOf(*args),
            )
        }

        override fun trace(message: String, throwable: Throwable) {
            if (logger.isTraceEnabled) writeLog(
                level = LocationAwareLogger.TRACE_INT,
                format = message,
                exception = throwable,
            )
        }

        override fun trace(marker: Marker, message: String) {
            if (logger.isTraceEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.TRACE_INT,
                format = message,
            )
        }

        override fun trace(marker: Marker, format: String, arg: Any?) {
            if (logger.isTraceEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.TRACE_INT,
                format = format,
                args = arrayOf(arg),
            )
        }

        override fun trace(marker: Marker, format: String, arg1: Any?, arg2: Any?) {
            if (logger.isTraceEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.TRACE_INT,
                format = format,
                args = arrayOf(arg1, arg2),
            )
        }

        override fun trace(marker: Marker, format: String, vararg argArray: Any?) {
            if (logger.isTraceEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.TRACE_INT,
                format = format,
                args = arrayOf(*argArray),
            )
        }

        override fun trace(marker: Marker, message: String, throwable: Throwable) {
            if (logger.isTraceEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.TRACE_INT,
                format = message,
                exception = throwable,
            )
        }

        override fun debug(message: String) {
            if (logger.isDebugEnabled) writeLog(
                level = LocationAwareLogger.DEBUG_INT,
                format = message,
            )
        }

        override fun debug(format: String, arg: Any?) {
            if (logger.isDebugEnabled) writeLog(
                level = LocationAwareLogger.DEBUG_INT,
                format = format,
                args = arrayOf(arg),
            )
        }

        override fun debug(format: String, arg1: Any?, arg2: Any?) {
            if (logger.isDebugEnabled) writeLog(
                level = LocationAwareLogger.DEBUG_INT,
                format = format,
                args = arrayOf(arg1, arg2),
            )
        }

        override fun debug(format: String, vararg args: Any?) {
            if (logger.isDebugEnabled) writeLog(
                level = LocationAwareLogger.DEBUG_INT,
                format = format,
                args = arrayOf(*args),
            )
        }

        override fun debug(message: String, throwable: Throwable) {
            if (logger.isDebugEnabled) writeLog(
                level = LocationAwareLogger.DEBUG_INT,
                format = message,
                exception = throwable,
            )
        }

        override fun debug(marker: Marker, message: String) {
            if (logger.isDebugEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.DEBUG_INT,
                format = message,
            )
        }

        override fun debug(marker: Marker, format: String, arg: Any?) {
            if (logger.isDebugEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.DEBUG_INT,
                format = format,
                args = arrayOf(arg),
            )
        }

        override fun debug(marker: Marker, format: String, arg1: Any?, arg2: Any?) {
            if (logger.isDebugEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.DEBUG_INT,
                format = format,
                args = arrayOf(arg1, arg2),
            )
        }

        override fun debug(marker: Marker, format: String, vararg arguments: Any?) {
            if (logger.isDebugEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.DEBUG_INT,
                format = format,
                args = arrayOf(*arguments),
            )
        }

        override fun debug(marker: Marker, message: String, throwable: Throwable) {
            if (logger.isDebugEnabled) writeLog(
                marker = marker,
                level = LocationAwareLogger.DEBUG_INT,
                format = message,
                exception = throwable,
            )
        }

        override fun warn(message: String) = writeLog(
            level = LocationAwareLogger.WARN_INT,
            format = message,
        )

        override fun warn(format: String, arg: Any?) = writeLog(
            level = LocationAwareLogger.WARN_INT,
            format = format,
            args = arrayOf(arg),
        )

        override fun warn(message: String, arg1: Any?, arg2: Any?) = writeLog(
            level = LocationAwareLogger.WARN_INT,
            format = message,
            args = arrayOf(arg1, arg2),
        )

        override fun warn(format: String, vararg args: Any?) = writeLog(
            level = LocationAwareLogger.WARN_INT,
            format = format,
            args = arrayOf(*args),
        )

        override fun warn(message: String, throwable: Throwable) = writeLog(
            level = LocationAwareLogger.WARN_INT,
            format = message,
            exception = throwable,
        )

        override fun warn(marker: Marker, message: String) = writeLog(
            marker = marker,
            level = LocationAwareLogger.WARN_INT,
            message,
        )

        override fun warn(marker: Marker, format: String, arg: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.WARN_INT,
            format = format,
            args = arrayOf(arg),
        )

        override fun warn(marker: Marker, format: String, arg1: Any?, arg2: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.WARN_INT,
            format = format,
            args = arrayOf(arg1, arg2),
        )

        override fun warn(marker: Marker, format: String, vararg arguments: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.WARN_INT,
            format = format,
            args = arrayOf(*arguments)
        )

        override fun warn(marker: Marker, message: String, throwable: Throwable) = writeLog(
            marker = marker,
            level = LocationAwareLogger.WARN_INT,
            format = message,
            exception = throwable,
        )

        override fun error(message: String) = writeLog(
            level = LocationAwareLogger.ERROR_INT,
            format = message,
        )

        override fun error(format: String, arg: Any?) = writeLog(
            level = LocationAwareLogger.ERROR_INT,
            format = format,
            args = arrayOf(arg),
        )

        override fun error(format: String, arg1: Any?, arg2: Any?) = writeLog(
            level = LocationAwareLogger.ERROR_INT,
            format = format,
            args = arrayOf(arg1, arg2),
        )

        override fun error(format: String, vararg args: Any?) = writeLog(
            level = LocationAwareLogger.ERROR_INT,
            format = format,
            args = arrayOf(*args),
        )

        override fun error(message: String, throwable: Throwable) = writeLog(
            level = LocationAwareLogger.ERROR_INT,
            format = message,
            exception = throwable,
        )

        override fun error(marker: Marker, message: String) = writeLog(
            marker = marker,
            level = LocationAwareLogger.ERROR_INT,
            format = message,
        )

        override fun error(marker: Marker, format: String, arg: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.ERROR_INT,
            format = format,
            args = arrayOf(arg),
        )

        override fun error(marker: Marker, format: String, arg1: Any?, arg2: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.ERROR_INT,
            format = format,
            args = arrayOf(arg1, arg2),
        )

        override fun error(marker: Marker, format: String, vararg arguments: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.ERROR_INT,
            format = format,
            args = arrayOf(*arguments),
        )

        override fun error(marker: Marker, message: String, throwable: Throwable) = writeLog(
            marker = marker,
            level = LocationAwareLogger.ERROR_INT,
            format = message,
            exception = throwable
        )

        override fun info(message: String) = writeLog(
            level = LocationAwareLogger.INFO_INT,
            format = message,
        )

        override fun info(format: String, arg: Any?) = writeLog(
            level = LocationAwareLogger.INFO_INT,
            format = format,
            args = arrayOf(arg),
        )

        override fun info(format: String, arg1: Any?, arg2: Any?) = writeLog(
            level = LocationAwareLogger.INFO_INT,
            format = format,
            args = arrayOf(arg1, arg2),
        )

        override fun info(format: String, vararg args: Any?) = writeLog(
            level = LocationAwareLogger.INFO_INT,
            format = format,
            args = arrayOf(*args),
        )

        override fun info(message: String, throwable: Throwable) = writeLog(
            level = LocationAwareLogger.INFO_INT,
            format = message,
            exception = throwable,
        )

        override fun info(marker: Marker, message: String) = writeLog(
            marker = marker,
            level = LocationAwareLogger.INFO_INT,
            format = message,
        )

        override fun info(marker: Marker, format: String, arg: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.INFO_INT,
            format = format,
            args = arrayOf(arg)
        )

        override fun info(marker: Marker, format: String, arg1: Any?, arg2: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.INFO_INT,
            format = format,
            args = arrayOf(arg1, arg2),
        )

        override fun info(marker: Marker, format: String, vararg arguments: Any?) = writeLog(
            marker = marker,
            level = LocationAwareLogger.INFO_INT,
            format = format,
            args = arrayOf(*arguments)
        )

        override fun info(marker: Marker, message: String, throwable: Throwable) = writeLog(
            marker = marker,
            level = LocationAwareLogger.INFO_INT,
            message,
            null,
            exception = throwable
        )

        private fun writeLog(
            marker: Marker? = null,
            level: Int,
            format: String,
            args: Array<Any?>? = null,
            exception: Throwable? = null
        ) {
            var exception = exception
            var message = format
            if (!args.isNullOrEmpty()) {
                val formatted = MessageFormatter.arrayFormat(format, args)
                if (exception == null && formatted.throwable != null) exception =
                    formatted.throwable
                message = formatted.message
            }
            logger.log(marker, fqcn, level, addPrefix(message), null, exception)
        }
    }

    private class LocationIgnorantKafkaLogger(
        logPrefix: String,
        private val logger: Logger,
    ) : AbstractKafkaLogger(logPrefix) {

        override fun getName(): String = logger.name

        override fun isTraceEnabled(): Boolean = logger.isTraceEnabled

        override fun isTraceEnabled(marker: Marker): Boolean = logger.isTraceEnabled(marker)

        override fun isDebugEnabled(): Boolean = logger.isDebugEnabled

        override fun isDebugEnabled(marker: Marker): Boolean = logger.isDebugEnabled(marker)

        override fun isInfoEnabled(): Boolean = logger.isInfoEnabled

        override fun isInfoEnabled(marker: Marker): Boolean = logger.isInfoEnabled(marker)

        override fun isWarnEnabled(): Boolean = logger.isWarnEnabled

        override fun isWarnEnabled(marker: Marker): Boolean = logger.isWarnEnabled(marker)

        override fun isErrorEnabled(): Boolean = logger.isErrorEnabled

        override fun isErrorEnabled(marker: Marker): Boolean = logger.isErrorEnabled(marker)

        override fun trace(message: String) {
            if (logger.isTraceEnabled) logger.trace(addPrefix(message))
        }

        override fun trace(message: String, arg: Any?) {
            if (logger.isTraceEnabled) logger.trace(addPrefix(message), arg)
        }

        override fun trace(message: String, arg1: Any?, arg2: Any?) {
            if (logger.isTraceEnabled)
                logger.trace(addPrefix(message), arg1, arg2)
        }

        override fun trace(message: String, vararg args: Any?) {
            if (logger.isTraceEnabled)
                logger.trace(addPrefix(message), *args)
        }

        override fun trace(message: String, throwable: Throwable) {
            if (logger.isTraceEnabled) logger.trace(addPrefix(message), throwable)
        }

        override fun trace(marker: Marker, message: String) {
            if (logger.isTraceEnabled) logger.trace(marker, addPrefix(message))
        }

        override fun trace(marker: Marker, format: String, arg: Any?) {
            if (logger.isTraceEnabled) logger.trace(marker, addPrefix(format), arg)
        }

        override fun trace(marker: Marker, format: String, arg1: Any?, arg2: Any?) {
            if (logger.isTraceEnabled) logger.trace(marker, addPrefix(format), arg1, arg2)
        }

        override fun trace(marker: Marker, format: String, vararg argArray: Any?) {
            if (logger.isTraceEnabled) logger.trace(marker, addPrefix(format), *argArray)
        }

        override fun trace(marker: Marker, message: String, throwable: Throwable) {
            if (logger.isTraceEnabled) logger.trace(marker, addPrefix(message), throwable)
        }

        override fun debug(message: String) {
            if (logger.isDebugEnabled) logger.debug(addPrefix(message))
        }

        override fun debug(message: String, arg: Any?) {
            if (logger.isDebugEnabled) logger.debug(addPrefix(message), arg)
        }

        override fun debug(message: String, arg1: Any?, arg2: Any?) {
            if (logger.isDebugEnabled) logger.debug(addPrefix(message), arg1, arg2)
        }

        override fun debug(message: String, vararg args: Any?) {
            if (logger.isDebugEnabled) logger.debug(addPrefix(message), *args)
        }

        override fun debug(message: String, throwable: Throwable) {
            if (logger.isDebugEnabled) logger.debug(addPrefix(message), throwable)
        }

        override fun debug(marker: Marker, message: String) {
            if (logger.isDebugEnabled) logger.debug(marker, addPrefix(message))
        }

        override fun debug(marker: Marker, format: String, arg: Any?) {
            if (logger.isDebugEnabled) logger.debug(marker, addPrefix(format), arg)
        }

        override fun debug(marker: Marker, format: String, arg1: Any?, arg2: Any?) {
            if (logger.isDebugEnabled) logger.debug(marker, addPrefix(format), arg1, arg2)
        }

        override fun debug(marker: Marker, format: String, vararg arguments: Any?) {
            if (logger.isDebugEnabled) logger.debug(marker, addPrefix(format), *arguments)
        }

        override fun debug(marker: Marker, message: String, throwable: Throwable) {
            if (logger.isDebugEnabled) logger.debug(marker, addPrefix(message), throwable)
        }

        override fun warn(message: String) = logger.warn(addPrefix(message))

        override fun warn(message: String, arg: Any?) = logger.warn(addPrefix(message), arg)

        override fun warn(message: String, arg1: Any?, arg2: Any?) =
            logger.warn(addPrefix(message), arg1, arg2)

        override fun warn(message: String, vararg args: Any?) =
            logger.warn(addPrefix(message), *args)

        override fun warn(message: String, throwable: Throwable) =
            logger.warn(addPrefix(message), throwable)

        override fun warn(marker: Marker, message: String) =
            logger.warn(marker, addPrefix(message))

        override fun warn(marker: Marker, format: String, arg: Any?) =
            logger.warn(marker, addPrefix(format), arg)

        override fun warn(marker: Marker, format: String, arg1: Any?, arg2: Any?) =
            logger.warn(marker, addPrefix(format), arg1, arg2)

        override fun warn(marker: Marker, format: String, vararg arguments: Any?) =
            logger.warn(marker, addPrefix(format), *arguments)

        override fun warn(marker: Marker, message: String, throwable: Throwable) =
            logger.warn(marker, addPrefix(message), throwable)

        override fun error(message: String) = logger.error(addPrefix(message))

        override fun error(message: String, arg: Any?) = logger.error(addPrefix(message), arg)

        override fun error(message: String, arg1: Any?, arg2: Any?) =
            logger.error(addPrefix(message), arg1, arg2)

        override fun error(message: String, vararg args: Any?) =
            logger.error(addPrefix(message), *args)

        override fun error(message: String, throwable: Throwable) =
            logger.error(addPrefix(message), throwable)

        override fun error(marker: Marker, message: String) =
            logger.error(marker, addPrefix(message))

        override fun error(marker: Marker, format: String, arg: Any?) =
            logger.error(marker, addPrefix(format), arg)

        override fun error(marker: Marker, format: String, arg1: Any?, arg2: Any?) =
            logger.error(marker, addPrefix(format), arg1, arg2)

        override fun error(marker: Marker, format: String, vararg arguments: Any?) =
            logger.error(marker, addPrefix(format), *arguments)

        override fun error(marker: Marker, message: String, throwable: Throwable) =
            logger.error(marker, addPrefix(message), throwable)

        override fun info(message: String) = logger.info(addPrefix(message))

        override fun info(message: String, arg: Any?) = logger.info(addPrefix(message), arg)

        override fun info(message: String, arg1: Any?, arg2: Any?) =
            logger.info(addPrefix(message), arg1, arg2)

        override fun info(message: String, vararg args: Any?) =
            logger.info(addPrefix(message), *args)

        override fun info(message: String, throwable: Throwable) =
            logger.info(addPrefix(message), throwable)

        override fun info(marker: Marker, message: String) =
            logger.info(marker, addPrefix(message))

        override fun info(marker: Marker, format: String, arg: Any?) =
            logger.info(marker, addPrefix(format), arg)

        override fun info(marker: Marker, format: String, arg1: Any?, arg2: Any?) =
            logger.info(marker, addPrefix(format), arg1, arg2)

        override fun info(marker: Marker, format: String, vararg arguments: Any?) =
            logger.info(marker, addPrefix(format), *arguments)

        override fun info(marker: Marker, message: String, throwable: Throwable) =
            logger.info(marker, addPrefix(message), throwable)
    }
}
