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

package org.apache.kafka.trogdor.rest

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import javax.ws.rs.NotFoundException
import javax.ws.rs.core.Response
import javax.ws.rs.ext.ExceptionMapper
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.SerializationException
import org.slf4j.LoggerFactory

class RestExceptionMapper : ExceptionMapper<Throwable> {

    override fun toResponse(exception: Throwable): Response {
        if (log.isDebugEnabled) log.debug("Uncaught exception in REST call: ", exception)
        else if (log.isInfoEnabled) log.info("Uncaught exception in REST call: {}", exception.message)

        return when (exception) {
            is NotFoundException -> buildResponse(Response.Status.NOT_FOUND, exception)
            is InvalidRequestException -> buildResponse(Response.Status.BAD_REQUEST, exception)
            is InvalidTypeIdException -> buildResponse(Response.Status.NOT_IMPLEMENTED, exception)
            is JsonMappingException -> buildResponse(Response.Status.BAD_REQUEST, exception)
            is ClassNotFoundException -> buildResponse(Response.Status.NOT_IMPLEMENTED, exception)
            is SerializationException -> buildResponse(Response.Status.BAD_REQUEST, exception)
            is RequestConflictException -> buildResponse(Response.Status.CONFLICT, exception)
            else -> buildResponse(Response.Status.INTERNAL_SERVER_ERROR, exception)
        }
    }

    private fun buildResponse(code: Response.Status, exception: Throwable): Response {
        return Response.status(code).entity(ErrorResponse(code.statusCode, exception.message)).build()
    }

    companion object {

        private val log = LoggerFactory.getLogger(RestExceptionMapper::class.java)

        @Throws(Exception::class)
        fun toException(code: Int, msg: String?): Exception = when (code) {
            Response.Status.NOT_FOUND.statusCode -> throw NotFoundException(msg)
            Response.Status.NOT_IMPLEMENTED.statusCode -> throw ClassNotFoundException(msg)
            Response.Status.BAD_REQUEST.statusCode -> throw InvalidRequestException(msg)
            Response.Status.CONFLICT.statusCode -> throw RequestConflictException(msg)
            else -> throw RuntimeException(msg)
        }
    }
}
