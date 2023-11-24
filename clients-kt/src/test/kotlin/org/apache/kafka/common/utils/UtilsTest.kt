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

import java.io.Closeable
import java.io.DataOutputStream
import java.io.EOFException
import java.io.File
import java.io.IOException
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.text.ParseException
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.temporal.ChronoUnit
import java.util.Date
import java.util.Properties
import java.util.TreeSet
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.Stream
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.utils.Utils.abs
import org.apache.kafka.common.utils.Utils.closeAll
import org.apache.kafka.common.utils.Utils.closeAllQuietly
import org.apache.kafka.common.utils.Utils.delete
import org.apache.kafka.common.utils.Utils.diff
import org.apache.kafka.common.utils.Utils.formatAddress
import org.apache.kafka.common.utils.Utils.formatBytes
import org.apache.kafka.common.utils.Utils.from32BitField
import org.apache.kafka.common.utils.Utils.getDateTime
import org.apache.kafka.common.utils.Utils.getHost
import org.apache.kafka.common.utils.Utils.getNullableSizePrefixedArray
import org.apache.kafka.common.utils.Utils.getPort
import org.apache.kafka.common.utils.Utils.intersection
import org.apache.kafka.common.utils.Utils.isEqualConstantTime
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.common.utils.Utils.loadProps
import org.apache.kafka.common.utils.Utils.min
import org.apache.kafka.common.utils.Utils.mkString
import org.apache.kafka.common.utils.Utils.murmur2
import org.apache.kafka.common.utils.Utils.propsToMap
import org.apache.kafka.common.utils.Utils.readBytes
import org.apache.kafka.common.utils.Utils.readFileAsString
import org.apache.kafka.common.utils.Utils.readFully
import org.apache.kafka.common.utils.Utils.readFullyOrFail
import org.apache.kafka.common.utils.Utils.to32BitField
import org.apache.kafka.common.utils.Utils.toArray
import org.apache.kafka.common.utils.Utils.toLogDateTimeFormat
import org.apache.kafka.common.utils.Utils.union
import org.apache.kafka.common.utils.Utils.utf8
import org.apache.kafka.common.utils.Utils.utf8Length
import org.apache.kafka.common.utils.Utils.validHostPattern
import org.apache.kafka.common.utils.Utils.writeTo
import org.apache.kafka.test.TestUtils.tempDirectory
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.function.Executable
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.`when`
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import kotlin.random.Random
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class UtilsTest {

    @Test
    fun testMurmur2() {
        val cases: MutableMap<ByteArray, Int> = HashMap()
        cases["21".toByteArray()] = -973932308
        cases["foobar".toByteArray()] = -790332482
        cases["a-little-bit-long-string".toByteArray()] = -985981536
        cases["a-little-bit-longer-string".toByteArray()] = -1486304829
        cases["lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8".toByteArray()] = -58897971
        cases[byteArrayOf('a'.code.toByte(), 'b'.code.toByte(), 'c'.code.toByte())] = 479470107

        for ((key, value) in cases) assertEquals(value, murmur2(key))
    }

    @Test
    fun testGetHost() {
        assertEquals("127.0.0.1", getHost("127.0.0.1:8000"))
        assertEquals("mydomain.com", getHost("PLAINTEXT://mydomain.com:8080"))
        assertEquals("MyDomain.com", getHost("PLAINTEXT://MyDomain.com:8080"))
        assertEquals("My_Domain.com", getHost("PLAINTEXT://My_Domain.com:8080"))
        assertEquals("::1", getHost("[::1]:1234"))
        assertEquals(
            expected = "2001:db8:85a3:8d3:1319:8a2e:370:7348",
            actual = getHost("PLAINTEXT://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678"),
        )
        assertEquals(
            expected = "2001:DB8:85A3:8D3:1319:8A2E:370:7348",
            actual = getHost("PLAINTEXT://[2001:DB8:85A3:8D3:1319:8A2E:370:7348]:5678"),
        )
        assertEquals(
            expected = "fe80::b1da:69ca:57f7:63d8%3",
            actual = getHost("PLAINTEXT://[fe80::b1da:69ca:57f7:63d8%3]:5678"),
        )
    }

    @Test
    fun testHostPattern() {
        assertTrue(validHostPattern("127.0.0.1"))
        assertTrue(validHostPattern("mydomain.com"))
        assertTrue(validHostPattern("MyDomain.com"))
        assertTrue(validHostPattern("My_Domain.com"))
        assertTrue(validHostPattern("::1"))
        assertTrue(validHostPattern("2001:db8:85a3:8d3:1319:8a2e:370"))
    }

    @Test
    fun testGetPort() {
        assertEquals(8000, getPort("127.0.0.1:8000"))
        assertEquals(8080, getPort("mydomain.com:8080"))
        assertEquals(8080, getPort("MyDomain.com:8080"))
        assertEquals(1234, getPort("[::1]:1234"))
        assertEquals(5678, getPort("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678"))
        assertEquals(5678, getPort("[2001:DB8:85A3:8D3:1319:8A2E:370:7348]:5678"))
        assertEquals(5678, getPort("[fe80::b1da:69ca:57f7:63d8%3]:5678"))
    }

    @Test
    fun testFormatAddress() {
        assertEquals("127.0.0.1:8000", formatAddress("127.0.0.1", 8000))
        assertEquals("mydomain.com:8080", formatAddress("mydomain.com", 8080))
        assertEquals("[::1]:1234", formatAddress("::1", 1234))
        assertEquals(
            expected = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678",
            actual = formatAddress("2001:db8:85a3:8d3:1319:8a2e:370:7348", 5678),
        )
    }

    @Test
    fun testFormatBytes() {
        assertEquals("-1", formatBytes(-1))
        assertEquals("1023 B", formatBytes(1023))
        assertEquals("1 KB", formatBytes(1024))
        assertEquals("1024 KB", formatBytes(1024 * 1024 - 1))
        assertEquals("1 MB", formatBytes(1024 * 1024))
        assertEquals("1.1 MB", formatBytes((1.1 * 1024 * 1024).toLong()))
        assertEquals("10 MB", formatBytes(10 * 1024 * 1024))
    }

    @Test
    @Disabled("Kotlin Migration: join is replaced with built-in function")
    fun testJoin() {
        assertEquals("", join(emptyList<Any>(), ","))
        assertEquals("1", join(mutableListOf("1"), ","))
        assertEquals("1,2,3", join(mutableListOf(1, 2, 3), ","))
    }

    @Test
    fun testMkString() {
        assertEquals("[]", mkString(Stream.empty<Any>(), "[", "]", ","))
        assertEquals("(1)", mkString(Stream.of("1"), "(", ")", ","))
        assertEquals("{1,2,3}", mkString(Stream.of(1, 2, 3), "{", "}", ","))
    }

    @Test
    fun testAbs() {
        assertEquals(0, abs(Int.MIN_VALUE))
        assertEquals(10, abs(-10))
        assertEquals(10, abs(10))
        assertEquals(0, abs(0))
        assertEquals(1, abs(-1))
    }

    @Test
    @Throws(IOException::class)
    fun writeToBuffer() {
        val input = byteArrayOf(0, 1, 2, 3, 4, 5)
        val source = ByteBuffer.wrap(input)
        doTestWriteToByteBuffer(source, ByteBuffer.allocate(input.size))
        doTestWriteToByteBuffer(source, ByteBuffer.allocateDirect(input.size))
        assertEquals(0, source.position())
        source.position(2)
        doTestWriteToByteBuffer(source, ByteBuffer.allocate(input.size))
        doTestWriteToByteBuffer(source, ByteBuffer.allocateDirect(input.size))
    }

    @Throws(IOException::class)
    private fun doTestWriteToByteBuffer(source: ByteBuffer, dest: ByteBuffer) {
        val numBytes = source.remaining()
        val position = source.position()
        val out = DataOutputStream(ByteBufferOutputStream(dest))
        writeTo(out, source, source.remaining())
        dest.flip()
        assertEquals(numBytes, dest.remaining())
        assertEquals(position, source.position())
        assertEquals(source, dest)
    }

    @Test
    fun toArray() {
        val input = byteArrayOf(0, 1, 2, 3, 4)
        val buffer = ByteBuffer.wrap(input)
        assertContentEquals(input, toArray(buffer))
        assertEquals(0, buffer.position())
        assertContentEquals(byteArrayOf(1, 2), toArray(buffer, 1, 2))
        assertEquals(0, buffer.position())
        buffer.position(2)
        assertContentEquals(byteArrayOf(2, 3, 4), toArray(buffer))
        assertEquals(2, buffer.position())
    }

    @Test
    fun toArrayDirectByteBuffer() {
        val input = byteArrayOf(0, 1, 2, 3, 4)
        val buffer = ByteBuffer.allocateDirect(5)
        buffer.put(input)
        buffer.rewind()
        assertContentEquals(input, toArray(buffer))
        assertEquals(0, buffer.position())
        assertContentEquals(byteArrayOf(1, 2), toArray(buffer, 1, 2))
        assertEquals(0, buffer.position())
        buffer.position(2)
        assertContentEquals(byteArrayOf(2, 3, 4), toArray(buffer))
        assertEquals(2, buffer.position())
    }

    @Test
    fun getNullableSizePrefixedArrayExact() {
        val input = byteArrayOf(0, 0, 0, 2, 1, 0)
        val buffer = ByteBuffer.wrap(input)
        val array = getNullableSizePrefixedArray(buffer)
        assertContentEquals(byteArrayOf(1, 0), array)
        assertEquals(6, buffer.position())
        assertFalse(buffer.hasRemaining())
    }

    @Test
    fun getNullableSizePrefixedArrayExactEmpty() {
        val input = byteArrayOf(0, 0, 0, 0)
        val buffer = ByteBuffer.wrap(input)
        val array = getNullableSizePrefixedArray(buffer)
        assertContentEquals(byteArrayOf(), array)
        assertEquals(4, buffer.position())
        assertFalse(buffer.hasRemaining())
    }

    @Test
    fun getNullableSizePrefixedArrayRemainder() {
        val input = byteArrayOf(0, 0, 0, 2, 1, 0, 9)
        val buffer = ByteBuffer.wrap(input)
        val array = getNullableSizePrefixedArray(buffer)
        assertContentEquals(byteArrayOf(1, 0), array)
        assertEquals(6, buffer.position())
        assertTrue(buffer.hasRemaining())
    }

    @Test
    fun getNullableSizePrefixedArrayNull() {
        // -1
        val input = byteArrayOf(-1, -1, -1, -1)
        val buffer = ByteBuffer.wrap(input)
        val array = getNullableSizePrefixedArray(buffer)
        assertNull(array)
        assertEquals(4, buffer.position())
        assertFalse(buffer.hasRemaining())
    }

    @Test
    fun getNullableSizePrefixedArrayInvalid() {
        // -2
        val input = byteArrayOf(-1, -1, -1, -2)
        val buffer = ByteBuffer.wrap(input)
        assertFailsWith<NegativeArraySizeException> { getNullableSizePrefixedArray(buffer) }
    }

    @Test
    fun getNullableSizePrefixedArrayUnderflow() {
        // Integer.MAX_VALUE
        val input = byteArrayOf(127, -1, -1, -1)
        val buffer = ByteBuffer.wrap(input)
        // note, we get a buffer underflow exception instead of an OOME, even though the encoded size
        // would be 2,147,483,647 aka 2.1 GB, probably larger than the available heap
        assertFailsWith<BufferUnderflowException> { getNullableSizePrefixedArray(buffer) }
    }

    @Test
    fun utf8ByteArraySerde() {
        val utf8String = "A\u00ea\u00f1\u00fcC"
        val utf8Bytes = utf8String.toByteArray()
        assertContentEquals(utf8Bytes, utf8(utf8String))
        assertEquals(utf8Bytes.size, utf8Length(utf8String))
        assertEquals(utf8String, utf8(utf8Bytes))
    }

    @Test
    fun utf8ByteBufferSerde() {
        doTestUtf8ByteBuffer(ByteBuffer.allocate(20))
        doTestUtf8ByteBuffer(ByteBuffer.allocateDirect(20))
    }

    private fun doTestUtf8ByteBuffer(utf8Buffer: ByteBuffer) {
        val utf8String = "A\u00ea\u00f1\u00fcC"
        val utf8Bytes = utf8String.toByteArray(StandardCharsets.UTF_8)
        utf8Buffer.position(4)
        utf8Buffer.put(utf8Bytes)
        utf8Buffer.position(4)
        assertEquals(utf8String, utf8(utf8Buffer, utf8Bytes.size))
        assertEquals(4, utf8Buffer.position())
        utf8Buffer.position(0)
        assertEquals(utf8String, utf8(utf8Buffer, 4, utf8Bytes.size))
        assertEquals(0, utf8Buffer.position())
    }

    private fun subTest(buffer: ByteBuffer) {
        // The first byte should be 'A'
        assertEquals('A'.code, readBytes(buffer, 0, 1)[0].toInt())

        // The offset is 2, so the first 2 bytes should be skipped.
        var results = readBytes(buffer, 2, 3)
        assertEquals('y'.code, results[0].toInt())
        assertEquals(' '.code, results[1].toInt())
        assertEquals('S'.code, results[2].toInt())
        assertEquals(3, results.size)

        // test readBytes without offset and length specified.
        results = readBytes(buffer)
        assertEquals('A'.code, results[0].toInt())
        assertEquals('t'.code, results[buffer.limit() - 1].toInt())
        assertEquals(buffer.limit(), results.size)
    }

    @Test
    fun testReadBytes() {
        val myvar = "Any String you want".toByteArray()
        var buffer = ByteBuffer.allocate(myvar.size)
        buffer.put(myvar)
        buffer.rewind()
        subTest(buffer)

        // test readonly buffer, different path
        buffer = ByteBuffer.wrap(myvar).asReadOnlyBuffer()
        subTest(buffer)
    }

    @Test
    @Throws(IOException::class)
    fun testFileAsStringSimpleFile() {
        val tempFile = tempFile()
        try {
            val testContent = "Test Content"
            Files.write(tempFile.toPath(), testContent.toByteArray())
            assertEquals(testContent, readFileAsString(tempFile.path))
        } finally {
            Files.deleteIfExists(tempFile.toPath())
        }
    }

    /**
     * Test to read content of named pipe as string. As reading/writing to a pipe can block,
     * timeout test after a minute (test finishes within 100 ms normally).
     */
    @Timeout(60)
    @Test
    @Throws(Exception::class)
    fun testFileAsStringNamedPipe() {

        // Create a temporary name for named pipe
        var n = Random.nextLong()
        n = if (n == Long.MIN_VALUE) 0 else kotlin.math.abs(n.toDouble()).toLong()

        // Use the name to create a FIFO in tmp directory
        val tmpDir = System.getProperty("java.io.tmpdir")
        val fifoName = "fifo-$n.tmp"
        val fifo = File(tmpDir, fifoName)
        var producerThread: Thread? = null
        try {
            val mkFifoCommand = ProcessBuilder("mkfifo", fifo.getCanonicalPath()).start()
            mkFifoCommand.waitFor()

            // Send some data to fifo and then read it back, but as FIFO blocks if the consumer isn't present,
            // we need to send data in a separate thread.
            val testFileContent = "This is test"
            producerThread = Thread({
                try {
                    Files.write(fifo.toPath(), testFileContent.toByteArray())
                } catch (e: IOException) {
                    fail("Error when producing to fifo : " + e.message)
                }
            }, "FIFO-Producer")
            producerThread.start()
            assertEquals(testFileContent, readFileAsString(fifo.getCanonicalPath()))
        } finally {
            Files.deleteIfExists(fifo.toPath())
            if (producerThread != null) {
                producerThread.join((30 * 1000).toLong()) // Wait for thread to terminate
                assertFalse(producerThread.isAlive)
            }
        }
    }

    @Test
    @Disabled("Kotlin Migration: min replaced with built-in function minOf()")
    fun testMin() {
        assertEquals(1, min(1))
        assertEquals(1, min(1, 2, 3))
        assertEquals(1, min(2, 1, 3))
        assertEquals(1, min(2, 3, 1))
    }

    @Test
    fun testCloseAll() {
        val closeablesWithoutException = TestCloseable.createCloseables(false, false, false)
        try {
            closeAll(*closeablesWithoutException)
            TestCloseable.checkClosed(*closeablesWithoutException)
        } catch (e: IOException) {
            fail("Unexpected exception: $e")
        }
        val closeablesWithException = TestCloseable.createCloseables(true, true, true)
        val error = assertFailsWith<IOException>(message = "Expected exception not thrown") {
            closeAll(*closeablesWithException)
        }

        TestCloseable.checkClosed(*closeablesWithException)
        TestCloseable.checkException(error, *closeablesWithException)

        val singleExceptionCloseables = TestCloseable.createCloseables(false, true, false)
        val error2 = assertFailsWith<IOException>(message = "Expected exception not thrown") {
            closeAll(*singleExceptionCloseables)
        }
        TestCloseable.checkClosed(*singleExceptionCloseables)
        TestCloseable.checkException(error2, singleExceptionCloseables[1])

        val mixedCloseables = TestCloseable.createCloseables(false, true, false, true, true)
        val error3 = assertFailsWith<IOException>(message = "Expected exception not thrown") {
            closeAll(*mixedCloseables)
        }
        TestCloseable.checkClosed(*mixedCloseables)
        TestCloseable.checkException(error3, mixedCloseables[1], mixedCloseables[3], mixedCloseables[4])
    }

    @Test
    @Throws(IOException::class)
    fun testReadFullyOrFailWithRealFile() {
        FileChannel.open(tempFile().toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE).use { channel ->
            // prepare channel
            val msg = "hello, world"
            channel.write(ByteBuffer.wrap(msg.toByteArray()), 0)
            channel.force(true)
            assertEquals(
                expected = channel.size(),
                actual = msg.length.toLong(),
                message = "Message should be written to the file channel",
            )
            val perfectBuffer = ByteBuffer.allocate(msg.length)
            val smallBuffer = ByteBuffer.allocate(5)
            val largeBuffer = ByteBuffer.allocate(msg.length + 1)
            // Scenario 1: test reading into a perfectly-sized buffer
            readFullyOrFail(channel, perfectBuffer, 0, "perfect")
            assertFalse(perfectBuffer.hasRemaining(), "Buffer should be filled up")
            assertEquals(
                expected = msg,
                actual = String(perfectBuffer.array()),
                message = "Buffer should be populated correctly",
            )
            // Scenario 2: test reading into a smaller buffer
            readFullyOrFail(channel, smallBuffer, 0, "small")
            assertFalse(smallBuffer.hasRemaining(), "Buffer should be filled")
            assertEquals(
                expected = "hello",
                actual = String(smallBuffer.array()),
                message = "Buffer should be populated correctly",
            )
            // Scenario 3: test reading starting from a non-zero position
            smallBuffer.clear()
            readFullyOrFail(channel, smallBuffer, 7, "small")
            assertFalse(smallBuffer.hasRemaining(), "Buffer should be filled")
            assertEquals(
                expected = "world",
                actual = String(smallBuffer.array()),
                message = "Buffer should be populated correctly",
            )
            // Scenario 4: test end of stream is reached before buffer is filled up
            assertFailsWith<EOFException>(message = "Expected EOFException to be raised") {
                readFullyOrFail(channel, largeBuffer, 0, "large")
            }
        }
    }

    /**
     * Tests that `readFullyOrFail` behaves correctly if multiple `FileChannel.read` operations are required to fill
     * the destination buffer.
     */
    @Test
    @Throws(IOException::class)
    fun testReadFullyOrFailWithPartialFileChannelReads() {
        val channelMock = mock<FileChannel>()
        val bufferSize = 100
        val buffer = ByteBuffer.allocate(bufferSize)
        val expectedBufferContent = fileChannelMockExpectReadWithRandomBytes(channelMock, bufferSize)
        readFullyOrFail(channelMock, buffer, 0L, "test")
        assertEquals(
            expected = expectedBufferContent,
            actual = String(buffer.array()),
            message = "The buffer should be populated correctly",
        )
        assertFalse(buffer.hasRemaining(), "The buffer should be filled")
        verify(channelMock, atLeastOnce()).read(any(), anyLong())
    }

    /**
     * Tests that `readFullyOrFail` behaves correctly if multiple `FileChannel.read` operations are required to fill
     * the destination buffer.
     */
    @Test
    @Throws(IOException::class)
    fun testReadFullyWithPartialFileChannelReads() {
        val channelMock = mock<FileChannel>()
        val bufferSize = 100
        val expectedBufferContent = fileChannelMockExpectReadWithRandomBytes(channelMock, bufferSize)
        val buffer = ByteBuffer.allocate(bufferSize)
        readFully(channelMock, buffer, 0L)
        assertEquals(
            expected = expectedBufferContent,
            actual = String(buffer.array()),
            message = "The buffer should be populated correctly.",
        )
        assertFalse(buffer.hasRemaining(), "The buffer should be filled")
        verify(channelMock, atLeastOnce()).read(any(), anyLong())
    }

    @Test
    @Throws(IOException::class)
    fun testReadFullyIfEofIsReached() {
        val channelMock = mock<FileChannel>()
        val bufferSize = 100
        val fileChannelContent = "abcdefghkl"
        val buffer = ByteBuffer.allocate(bufferSize)
        `when`(channelMock.read(any(), anyLong())).then { invocation ->
            val bufferArg = invocation.getArgument<ByteBuffer>(0)
            bufferArg.put(fileChannelContent.toByteArray())
            -1
        }
        readFully(channelMock, buffer, 0L)
        assertEquals("abcdefghkl", String(buffer.array(), 0, buffer.position()))
        assertEquals(fileChannelContent.length, buffer.position())
        assertTrue(buffer.hasRemaining())
        verify(channelMock, atLeastOnce()).read(any(), anyLong())
    }

    @Test
    @Throws(IOException::class)
    fun testLoadProps() {
        val tempFile = tempFile()
        try {
            val testContent = "a=1\nb=2\n#a comment\n\nc=3\nd="
            Files.write(tempFile.toPath(), testContent.toByteArray())
            val props = loadProps(tempFile.path)
            assertEquals(4, props.size)
            assertEquals("1", props["a"])
            assertEquals("2", props["b"])
            assertEquals("3", props["c"])
            assertEquals("", props["d"])
            val restrictedProps = loadProps(tempFile.path, mutableListOf("b", "d", "e"))
            assertEquals(2, restrictedProps.size)
            assertEquals("2", restrictedProps["b"])
            assertEquals("", restrictedProps["d"])
        } finally {
            Files.deleteIfExists(tempFile.toPath())
        }
    }

    /**
     * Expectation setter for multiple reads where each one reads random bytes to the buffer.
     *
     * @param channelMock The mocked FileChannel object
     * @param bufferSize The buffer size
     * @return Expected buffer string
     * @throws IOException If an I/O error occurs
     */
    @Throws(IOException::class)
    private fun fileChannelMockExpectReadWithRandomBytes(
        channelMock: FileChannel,
        bufferSize: Int,
    ): String {
        val step = 20
        var remainingBytes = bufferSize
        var `when` = `when`(channelMock.read(any(), anyLong()))
        val expectedBufferContent = StringBuilder()
        while (remainingBytes > 0) {
            val bytesRead = if (remainingBytes < step) remainingBytes else Random.nextInt(step)
            val stringRead = IntStream.range(0, bytesRead).mapToObj { "a" }
                .collect(Collectors.joining())
            expectedBufferContent.append(stringRead)
            `when` = `when`.then { invocation ->
                val buffer = invocation.getArgument<ByteBuffer>(0)
                buffer.put(stringRead.toByteArray())
                bytesRead
            }
            remainingBytes -= bytesRead
        }
        return expectedBufferContent.toString()
    }

    private class TestCloseable(private val id: Int, exceptionOnClose: Boolean) : Closeable {

        private val closeException: IOException?

        private var closed = false

        init {
            closeException = if (exceptionOnClose) IOException("Test close exception $id") else null
        }

        @Throws(IOException::class)
        override fun close() {
            closed = true
            closeException?.let { throw it }
        }

        companion object {
            fun createCloseables(vararg exceptionOnClose: Boolean): Array<TestCloseable> {
                return Array(exceptionOnClose.size) { i -> TestCloseable(i, exceptionOnClose[i]) }
            }

            fun checkClosed(vararg closeables: TestCloseable) {
                for (closeable in closeables)
                    assertTrue(closeable.closed, "Close not invoked for ${closeable.id}")
            }

            fun checkException(e: IOException, vararg closeablesWithException: TestCloseable) {
                assertEquals(closeablesWithException[0].closeException, e)
                val suppressed = e.suppressed
                assertEquals(closeablesWithException.size - 1, suppressed.size)

                for (i in 1 until closeablesWithException.size)
                    assertEquals(closeablesWithException[i].closeException, suppressed[i - 1])
            }
        }
    }

    @Timeout(120)
    @Test
    @Throws(IOException::class)
    fun testRecursiveDelete() {
        delete(null) // delete of null does nothing.

        // Test that deleting a temporary file works.
        val tempFile = tempFile()
        delete(tempFile)
        assertFalse(Files.exists(tempFile.toPath()))

        // Test recursive deletes
        val tempDir = tempDirectory()
        val tempDir2 = tempDirectory(tempDir.toPath(), "a")
        tempDirectory(tempDir.toPath(), "b")
        tempDirectory(tempDir2.toPath(), "c")
        delete(tempDir)
        assertFalse(Files.exists(tempDir.toPath()))
        assertFalse(Files.exists(tempDir2.toPath()))

        // Test that deleting a non-existent directory hierarchy works.
        delete(tempDir)
        assertFalse(Files.exists(tempDir.toPath()))
    }

    @Test
    fun testConvertTo32BitField() {
        var bytes = setOf<Byte>(0, 1, 5, 10, 31)
        var bitField = to32BitField(bytes)
        assertEquals(bytes, from32BitField(bitField))
        bytes = mutableSetOf()
        bitField = to32BitField(bytes)
        assertEquals(bytes, from32BitField(bitField))
        assertFailsWith<IllegalArgumentException> {
            to32BitField(setOf(0, 11, 32))
        }
    }

    @Test
    fun testUnion() {
        val oneSet = setOf("a", "b", "c")
        val anotherSet = setOf("c", "d", "e")
        val union = union({ TreeSet() }, oneSet, anotherSet)
        assertEquals(setOf("a", "b", "c", "d", "e"), union)
        assertIs<TreeSet<String>>(union)
    }

    @Test
    fun testUnionOfOne() {
        val oneSet = setOf("a", "b", "c")
        val union = union({ TreeSet() }, oneSet)
        assertEquals(setOf("a", "b", "c"), union)
        assertIs<TreeSet<String>>(union)
    }

    @Test
    fun testUnionOfMany() {
        val oneSet = setOf("a", "b", "c")
        val twoSet = setOf("c", "d", "e")
        val threeSet = setOf("b", "c", "d")
        val fourSet = setOf("x", "y", "z")
        val union = union({ TreeSet() }, oneSet, twoSet, threeSet, fourSet)
        assertEquals(setOf("a", "b", "c", "d", "e", "x", "y", "z"), union)
        assertIs<TreeSet<String>>(union)
    }

    @Test
    fun testUnionOfNone() {
        val union = union({ TreeSet<String>() })
        assertEquals(emptySet(), union)
        assertIs<TreeSet<String>>(union)
    }

    @Test
    fun testIntersection() {
        val oneSet = setOf("a", "b", "c")
        val anotherSet = setOf("c", "d", "e")
        val intersection = intersection({ TreeSet() }, oneSet, anotherSet)
        assertEquals(setOf("c"), intersection)
        assertIs<TreeSet<String>>(intersection)
    }

    @Test
    fun testIntersectionOfOne() {
        val oneSet = setOf("a", "b", "c")
        val intersection = intersection({ TreeSet() }, oneSet)
        assertEquals(setOf("a", "b", "c"), intersection)
        assertIs<TreeSet<String>>(intersection)
    }

    @Test
    fun testIntersectionOfMany() {
        val oneSet = setOf("a", "b", "c")
        val twoSet = setOf("c", "d", "e")
        val threeSet = setOf("b", "c", "d")
        val intersection = intersection({ TreeSet() }, oneSet, twoSet, threeSet)
        assertEquals(setOf("c"), intersection)
        assertIs<TreeSet<String>>(intersection)
    }

    @Test
    fun testDisjointIntersectionOfMany() {
        val oneSet = setOf("a", "b", "c")
        val twoSet = setOf("c", "d", "e")
        val threeSet = setOf("b", "c", "d")
        val fourSet = setOf("x", "y", "z")
        val intersection = intersection({ TreeSet() }, oneSet, twoSet, threeSet, fourSet)
        assertEquals(emptySet(), intersection)
        assertIs<TreeSet<String>>(intersection)
    }

    @Test
    fun testDiff() {
        val oneSet = setOf("a", "b", "c")
        val anotherSet = setOf("c", "d", "e")
        val diff = diff({ TreeSet() }, oneSet, anotherSet)
        assertEquals(setOf("a", "b"), diff)
        assertIs<TreeSet<String>>(diff)
    }

    @Test
    fun testPropsToMap() {
        assertFailsWith<ConfigException> {
            val props = Properties()
            props[1] = 2
            propsToMap(props)
        }
        assertValue(false)
        assertValue(1)
        assertValue("string")
        assertValue(1.1)
        assertValue(emptySet<Any>())
        assertValue(emptyList<Any>())
        assertValue(emptyMap<Any, Any>())
    }

    @Test
    fun testCloseAllQuietly() {
        val exception = AtomicReference<Throwable?>()
        val msg = "you should fail"
        val count = AtomicInteger(0)
        val c0 = AutoCloseable { throw RuntimeException(msg) }
        val c1 = AutoCloseable { count.incrementAndGet() }
        closeAllQuietly(
            exception,
            "test",
            c0,
            c1,
        )
        assertEquals(msg, exception.get()!!.message)
        assertEquals(1, count.get())
    }

    @Test
    @Throws(ParseException::class)
    fun shouldAcceptValidDateFormats() {
        //check valid formats
        invokeGetDateTimeMethod(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"))
        invokeGetDateTimeMethod(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
        invokeGetDateTimeMethod(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        invokeGetDateTimeMethod(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
        invokeGetDateTimeMethod(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    }

    @Test
    fun shouldThrowOnInvalidDateFormatOrNullTimestamp() {
        // check some invalid formats
        // test null timestamp
        // Kotlin Migration: Null values cannot be passed in Kotlin.
//        val error = assertFailsWith<IllegalArgumentException> { getDateTime(null) }
//        assertContains(error.message!!, "Error parsing timestamp with null value")

        // test pattern: yyyy-MM-dd'T'HH:mm:ss.X
        checkExceptionForGetDateTimeMethod {
            invokeGetDateTimeMethod(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"))
        }

        // test pattern: yyyy-MM-dd HH:mm:ss
        val error = assertFailsWith<ParseException> { invokeGetDateTimeMethod(SimpleDateFormat("yyyy-MM-dd HH:mm:ss")) }
        assertContains(error.message!!, "It does not contain a 'T' according to ISO8601 format")

        // KAFKA-10685: use DateTimeFormatter generate micro/nano second timestamp
        val formatter = DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter()
        val timestampWithNanoSeconds = LocalDateTime.of(2020, 11, 9, 12, 34, 56, 123456789)
        val timestampWithMicroSeconds = timestampWithNanoSeconds.truncatedTo(ChronoUnit.MICROS)
        val timestampWithSeconds = timestampWithNanoSeconds.truncatedTo(ChronoUnit.SECONDS)

        // test pattern: yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS
        checkExceptionForGetDateTimeMethod { getDateTime(formatter.format(timestampWithNanoSeconds)) }

        // test pattern: yyyy-MM-dd'T'HH:mm:ss.SSSSSS
        checkExceptionForGetDateTimeMethod { getDateTime(formatter.format(timestampWithMicroSeconds)) }

        // test pattern: yyyy-MM-dd'T'HH:mm:ss
        checkExceptionForGetDateTimeMethod { getDateTime(formatter.format(timestampWithSeconds)) }
    }

    private fun checkExceptionForGetDateTimeMethod(executable: Executable) {
        val error = assertFailsWith<ParseException> { executable.execute() }
        assertContains(error.message!!, "Unparseable date")
    }

    @Throws(ParseException::class)
    private fun invokeGetDateTimeMethod(format: SimpleDateFormat) {
        val checkpoint = Date()
        val formattedCheckpoint = format.format(checkpoint)
        getDateTime(formattedCheckpoint)
    }

    @Test
    fun testIsBlank() {
        assertTrue(null.isNullOrBlank())
        assertTrue("".isNullOrBlank())
        assertTrue(" ".isNullOrBlank())
        assertFalse("bob".isNullOrBlank())
        assertFalse(" bob ".isNullOrBlank())
    }

    @Test
    fun testCharacterArrayEquality() {
        assertCharacterArraysAreNotEqual(null, "abc")
        assertCharacterArraysAreNotEqual(null, "")
        assertCharacterArraysAreNotEqual("abc", null)
        assertCharacterArraysAreNotEqual("", null)
        assertCharacterArraysAreNotEqual("", "abc")
        assertCharacterArraysAreNotEqual("abc", "abC")
        assertCharacterArraysAreNotEqual("abc", "abcd")
        assertCharacterArraysAreNotEqual("abc", "abcdefg")
        assertCharacterArraysAreNotEqual("abcdefg", "abc")
        assertCharacterArraysAreEqual("abc", "abc")
        assertCharacterArraysAreEqual("a", "a")
        assertCharacterArraysAreEqual("", "")
        assertCharacterArraysAreEqual("", "")
        assertCharacterArraysAreEqual(null, null)
    }

    private fun assertCharacterArraysAreNotEqual(a: String?, b: String?) {
        val first = a?.toCharArray()
        val second = b?.toCharArray()
        if (a == null) assertNotNull(b)
        else assertNotEquals(a, b)
        assertFalse(isEqualConstantTime(first, second))
        assertFalse(isEqualConstantTime(second, first))
    }

    private fun assertCharacterArraysAreEqual(a: String?, b: String?) {
        val first = a?.toCharArray()
        val second = b?.toCharArray()
        if (a == null) assertNull(b)
        else assertEquals(a, b)
        assertTrue(isEqualConstantTime(first, second))
        assertTrue(isEqualConstantTime(second, first))
    }

    @Test
    fun testToLogDateTimeFormat() {
        val timestampWithMilliSeconds = LocalDateTime.of(2020, 11, 9, 12, 34, 5, 123000000)
        val timestampWithSeconds = LocalDateTime.of(2020, 11, 9, 12, 34, 5)
        val offsetFormatter = DateTimeFormatter.ofPattern("XXX")
        val offset = ZoneId.systemDefault().rules.getOffset(timestampWithSeconds)
        val requiredOffsetFormat = offsetFormatter.format(offset)
        assertEquals(
            expected = "2020-11-09 12:34:05,123 $requiredOffsetFormat",
            actual = toLogDateTimeFormat(
                timestampWithMilliSeconds.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
            ),
        )
        assertEquals(
            expected = "2020-11-09 12:34:05,000 $requiredOffsetFormat",
            actual = toLogDateTimeFormat(
                timestampWithSeconds.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
            ),
        )
    }

    companion object {
        private fun assertValue(value: Any) {
            val props = Properties()
            props["key"] = value
            assertEquals(propsToMap(props)["key"], value)
        }
    }
}
