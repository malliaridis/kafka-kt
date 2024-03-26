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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import javax.ws.rs.NotFoundException
import javax.ws.rs.core.Response
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.SerializationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RestExceptionMapperTest {
    
    @Test
    fun testToResponseNotFound() {
        val mapper = RestExceptionMapper()
        val resp = mapper.toResponse(NotFoundException())
        assertEquals(resp.status, Response.Status.NOT_FOUND.statusCode)
    }

    @Test
    fun testToResponseInvalidTypeIdException() {
        val mapper = RestExceptionMapper()
        val parser: JsonParser? = null
        val type: JavaType? = null
        val resp = mapper.toResponse(InvalidTypeIdException.from(parser, "dummy msg", type, "dummy typeId"))
        assertEquals(resp.status, Response.Status.NOT_IMPLEMENTED.statusCode)
    }

    @Test
    fun testToResponseJsonMappingException() {
        val mapper = RestExceptionMapper()
        val parser: JsonParser? = null
        val resp = mapper.toResponse(JsonMappingException.from(parser, "dummy msg"))
        assertEquals(resp.status, Response.Status.BAD_REQUEST.statusCode)
    }

    @Test
    fun testToResponseClassNotFoundException() {
        val mapper = RestExceptionMapper()
        val resp = mapper.toResponse(ClassNotFoundException())
        assertEquals(resp.status, Response.Status.NOT_IMPLEMENTED.statusCode)
    }

    @Test
    fun testToResponseSerializationException() {
        val mapper = RestExceptionMapper()
        val resp = mapper.toResponse(SerializationException())
        assertEquals(resp.status, Response.Status.BAD_REQUEST.statusCode)
    }

    @Test
    fun testToResponseInvalidRequestException() {
        val mapper = RestExceptionMapper()
        val resp = mapper.toResponse(InvalidRequestException("invalid request"))
        assertEquals(resp.status, Response.Status.BAD_REQUEST.statusCode)
    }

    @Test
    fun testToResponseUnknownException() {
        val mapper = RestExceptionMapper()
        val resp = mapper.toResponse(Exception("Unkown exception"))
        assertEquals(resp.status, Response.Status.INTERNAL_SERVER_ERROR.statusCode)
    }

    @Test
    fun testToExceptionNotFoundException() {
        assertFailsWith<NotFoundException> {
            RestExceptionMapper.toException(Response.Status.NOT_FOUND.statusCode, "Not Found")
        }
    }

    @Test
    fun testToExceptionClassNotFoundException() {
        assertFailsWith<ClassNotFoundException> {
            RestExceptionMapper.toException(Response.Status.NOT_IMPLEMENTED.statusCode, "Not Implemented")
        }
    }

    @Test
    fun testToExceptionSerializationException() {
        assertFailsWith<InvalidRequestException> {
            RestExceptionMapper.toException(Response.Status.BAD_REQUEST.statusCode, "Bad Request")
        }
    }

    @Test
    fun testToExceptionRuntimeException() {
        assertFailsWith<RuntimeException> { RestExceptionMapper.toException(-1, "Unkown status code") }
    }
}
