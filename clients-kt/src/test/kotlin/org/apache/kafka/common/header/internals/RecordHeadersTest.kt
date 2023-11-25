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

package org.apache.kafka.common.header.internals

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.test.Ignore
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class RecordHeadersTest {

    @Test
    fun testAdd() {
        val headers: Headers = RecordHeaders()
        headers.add(RecordHeader("key", "value".toByteArray()))
        val header = headers.first()
        assertHeader("key", "value", header)
        headers.add(RecordHeader("key2", "value2".toByteArray()))
        assertHeader("key2", "value2", headers.lastHeader("key2"))
        assertEquals(2, getCount(headers))
    }

    @Test
    fun testRemove() {
        val headers: Headers = RecordHeaders()
        headers.add(RecordHeader("key", "value".toByteArray()))
        assertTrue(headers.iterator().hasNext())
        headers.remove("key")
        assertFalse(headers.iterator().hasNext())
    }

    @Test
    fun testAddRemoveInterleaved() {
        val headers: Headers = RecordHeaders()
        headers.add(RecordHeader("key", "value".toByteArray()))
        headers.add(RecordHeader("key2", "value2".toByteArray()))
        assertTrue(headers.iterator().hasNext())
        headers.remove("key")
        assertEquals(1, getCount(headers))
        headers.add(RecordHeader("key3", "value3".toByteArray()))
        assertNull(headers.lastHeader("key"))
        assertHeader("key2", "value2", headers.lastHeader("key2"))
        assertHeader("key3", "value3", headers.lastHeader("key3"))
        assertEquals(2, getCount(headers))
        headers.remove("key2")
        assertNull(headers.lastHeader("key"))
        assertNull(headers.lastHeader("key2"))
        assertHeader("key3", "value3", headers.lastHeader("key3"))
        assertEquals(1, getCount(headers))
        headers.add(RecordHeader("key3", "value4".toByteArray()))
        assertHeader("key3", "value4", headers.lastHeader("key3"))
        assertEquals(2, getCount(headers))
        headers.add(RecordHeader("key", "valueNew".toByteArray()))
        assertEquals(3, getCount(headers))
        assertHeader("key", "valueNew", headers.lastHeader("key"))
        headers.remove("key3")
        assertEquals(1, getCount(headers))
        assertNull(headers.lastHeader("key2"))
        headers.remove("key")
        assertFalse(headers.iterator().hasNext())
    }

    @Test
    fun testLastHeader() {
        val headers: Headers = RecordHeaders()
        headers.add(RecordHeader("key", "value".toByteArray()))
        headers.add(RecordHeader("key", "value2".toByteArray()))
        headers.add(RecordHeader("key", "value3".toByteArray()))
        assertHeader("key", "value3", headers.lastHeader("key"))
        assertEquals(3, getCount(headers))
    }

    @Test
    @Throws(IOException::class)
    fun testReadOnly() {
        val headers = RecordHeaders()
        headers.add(RecordHeader("key", "value".toByteArray()))
        val headerIteratorBeforeClose = headers.iterator()
        headers.setReadOnly()
        try {
            headers.add(RecordHeader("key", "value".toByteArray()))
            fail("IllegalStateException expected as headers are closed")
        } catch (_: IllegalStateException) {
            //expected  
        }
        try {
            headers.remove("key")
            fail("IllegalStateException expected as headers are closed")
        } catch (_: IllegalStateException) {
            //expected  
        }
        try {
            val headerIterator = headers.iterator()
            headerIterator.next()
            headerIterator.remove()
            fail("IllegalStateException expected as headers are closed")
        } catch (_: IllegalStateException) {
            //expected  
        }
        try {
            headerIteratorBeforeClose.next()
            headerIteratorBeforeClose.remove()
            fail("IllegalStateException expected as headers are closed")
        } catch (_: IllegalStateException) {
            //expected  
        }
    }

    @Test
    @Throws(IOException::class)
    fun testHeaders() {
        val headers = RecordHeaders()
        headers.add(RecordHeader("key", "value".toByteArray()))
        headers.add(RecordHeader("key1", "key1value".toByteArray()))
        headers.add(RecordHeader("key", "value2".toByteArray()))
        headers.add(RecordHeader("key2", "key2value".toByteArray()))
        var keyHeaders: Iterator<Header?> = headers.headers("key").iterator()
        assertHeader("key", "value", keyHeaders.next())
        assertHeader("key", "value2", keyHeaders.next())
        assertFalse(keyHeaders.hasNext())
        keyHeaders = headers.headers("key1").iterator()
        assertHeader("key1", "key1value", keyHeaders.next())
        assertFalse(keyHeaders.hasNext())
        keyHeaders = headers.headers("key2").iterator()
        assertHeader("key2", "key2value", keyHeaders.next())
        assertFalse(keyHeaders.hasNext())
    }

    @Test
    @Throws(IOException::class)
    fun testNew() {
        val headers = RecordHeaders()
        headers.add(RecordHeader("key", "value".toByteArray()))
        headers.setReadOnly()
        val newHeaders = RecordHeaders(headers)
        newHeaders.add(RecordHeader("key", "value2".toByteArray()))

        //Ensure existing headers are not modified
        assertHeader("key", "value", headers.lastHeader("key"))
        assertEquals(1, getCount(headers))

        //Ensure new headers are modified
        assertHeader("key", "value2", newHeaders.lastHeader("key"))
        assertEquals(2, getCount(newHeaders))
    }

    @Test
    @Disabled("Kotlin Migration: null is not allowed and in Kotlin already covered via non-nullable parameter")
    fun shouldThrowNpeWhenAddingNullHeader() {
//        val recordHeaders = RecordHeaders()
//        assertFailsWith<NullPointerException> { recordHeaders.add(null) }
    }

    @Test
    @Disabled("Kotlin Migration: null is not allowed and in Kotlin already covered via non-nullable generic")
    fun shouldThrowNpeWhenAddingCollectionWithNullHeader() {
        // assertFailsWith<NullPointerException> { RecordHeaders(arrayOfNulls<Header>(1)) }
    }

    private fun getCount(headers: Headers): Int {
        var count = 0
        val headerIterator = headers.iterator()
        while (headerIterator.hasNext()) {
            headerIterator.next()
            count++
        }
        return count
    }

    companion object {
        fun assertHeader(key: String?, value: String, actual: Header?) {
            assertNotNull(actual)
            assertEquals(key, actual.key)
            assertTrue(value.toByteArray().contentEquals(actual.value))
        }
    }
}
