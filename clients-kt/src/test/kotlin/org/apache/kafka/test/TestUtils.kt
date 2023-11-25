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

package org.apache.kafka.test

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.UnalignedRecords
import org.apache.kafka.common.requests.ByteBufferChannel
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.utils.Exit.addShutdownHook
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.Utils.delete
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.regex.Pattern
import kotlin.math.min
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Helper functions for writing unit tests
 */
object TestUtils {

    private val log = LoggerFactory.getLogger(TestUtils::class.java)

    val IO_TMP_DIR = File(System.getProperty("java.io.tmpdir"))

    val LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    val DIGITS = "0123456789"

    val LETTERS_AND_DIGITS = LETTERS + DIGITS

    /* A consistent random number generator to make tests repeatable */
    val SEEDED_RANDOM = Random(192348092834L)

    val RANDOM = Random()

    val DEFAULT_POLL_INTERVAL_MS: Long = 100

    val DEFAULT_MAX_WAIT_MS: Long = 15000

    fun singletonCluster(): Cluster = clusterWith(nodes = 1)

    fun singletonCluster(topic: String, partitions: Int): Cluster = clusterWith(
        nodes = 1,
        topic = topic,
        partitions = partitions,
    )

    fun clusterWith(nodes: Int, topicPartitionCounts: Map<String, Int> = emptyMap()): Cluster {
        val ns = MutableList(nodes) { Node(it, "localhost", 1969) }
        for (i in 0 until nodes) ns[i] = Node(i, "localhost", 1969)
        val parts: MutableList<PartitionInfo> = ArrayList()
        for ((topic, partitions) in topicPartitionCounts) {
            for (i in 0 until partitions) parts.add(
                PartitionInfo(
                    topic = topic,
                    partition = i,
                    leader = ns[i % ns.size],
                    replicas = ns,
                    inSyncReplicas = ns,
                )
            )
        }
        return Cluster(
            clusterId = "kafka-cluster",
            nodes = ns.toList(),
            partitions = parts,
        )
    }

    fun clusterWith(nodes: Int, topic: String, partitions: Int): Cluster =
        clusterWith(nodes, mapOf(topic to partitions))

    /**
     * Generate an array of random bytes
     *
     * @param size The size of the array
     */
    fun randomBytes(size: Int): ByteArray {
        val bytes = ByteArray(size)
        SEEDED_RANDOM.nextBytes(bytes)
        return bytes
    }

    /**
     * Generate a random string of letters and digits of the given length
     *
     * @param len The length of the string
     * @return The random string
     */
    fun randomString(len: Int): String {
        val b = StringBuilder()
        for (i in 0 until len) b.append(
            LETTERS_AND_DIGITS[SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length)]
        )
        return b.toString()
    }
    /**
     * Create an empty file in the default temporary-file directory, using the given prefix
     * (default `kafka`) and suffix (`.tmp`) to generate its name.
     *
     * @throws IOException
     */
    @JvmOverloads
    @Throws(IOException::class)
    fun tempFile(prefix: String? = "kafka", suffix: String? = ".tmp"): File {
        val file = Files.createTempFile(prefix, suffix).toFile()
        file.deleteOnExit()

        // Note that we don't use Exit.addShutdownHook here because it allows for the possibility of
        // accidently overriding the behaviour of this hook leading to leaked files.
        Runtime.getRuntime().addShutdownHook(
            KafkaThread.nonDaemon("delete-temp-file-shutdown-hook") {
                try {
                    delete(file)
                } catch (e: IOException) {
                    log.error("Error deleting {}", file.absolutePath, e)
                }
            }
        )
        return file
    }

    /**
     * Create a file with the given contents in the default temporary-file directory, using `kafka`
     * as the prefix and `tmp` as the suffix to generate its name.
     */
    @Throws(IOException::class)
    @JvmName("tempFileWithContents")
    fun tempFile(contents: String): File {
        val file = tempFile()
        Files.write(file.toPath(), contents.toByteArray(StandardCharsets.UTF_8))
        return file
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the prefix.
     *
     * @param parent The parent folder path name, if `null` using the default temporary-file
     * directory
     * @param prefix The prefix of the temporary directory or "kafka-" if not defined.
     */
    fun tempDirectory(parent: Path? = null, prefix: String = "kafka-"): File {
        val file: File
        try {
            file = if (parent == null) Files.createTempDirectory(prefix).toFile()
            else Files.createTempDirectory(parent, prefix).toFile()
        } catch (ex: IOException) {
            throw RuntimeException("Failed to create a temp dir", ex)
        }
        file.deleteOnExit()
        addShutdownHook("delete-temp-file-shutdown-hook") {
            try {
                delete(file)
            } catch (e: IOException) {
                log.error("Error deleting {}", file.absolutePath, e)
            }
        }
        return file
    }

    fun producerConfig(
        bootstrapServers: String,
        keySerializer: Class<*>,
        valueSerializer: Class<*>,
        additional: Properties = Properties(),
    ): Properties {
        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        properties[ProducerConfig.ACKS_CONFIG] = "all"
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializer
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializer
        properties.putAll(additional)
        return properties
    }

    fun consumerConfig(
        bootstrapServers: String,
        groupId: String,
        keyDeserializer: Class<*>,
        valueDeserializer: Class<*>,
        additional: Properties = Properties(),
    ): Properties {
        val consumerConfig = Properties()
        consumerConfig[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        consumerConfig[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        consumerConfig[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer
        consumerConfig[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer
        consumerConfig.putAll(additional)
        return consumerConfig
    }

    /**
     * returns consumer config with random UUID for the Group ID
     */
    fun consumerConfig(
        bootstrapServers: String,
        keyDeserializer: Class<*>,
        valueDeserializer: Class<*>,
    ): Properties = consumerConfig(
        bootstrapServers = bootstrapServers,
        groupId = UUID.randomUUID().toString(),
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
        additional = Properties(),
    )

    /**
     * Wait for condition to be met for at most `maxWaitMs` and throw assertion failure otherwise.
     * This should be used instead of `Thread.sleep` whenever possible as it allows a longer timeout
     * to be used without unnecessarily increasing test time (as the condition is checked
     * frequently). The longer timeout is needed to avoid transient failures due to slow or
     * overloaded machines.
     */
    @Throws(InterruptedException::class)
    fun waitForCondition(
        testCondition: TestCondition,
        maxWaitMs: Long = DEFAULT_MAX_WAIT_MS,
        conditionDetails: String?,
    ) = waitForCondition(testCondition, maxWaitMs) { conditionDetails }

    /**
     * Wait for condition to be met for at most `maxWaitMs` with a polling interval of
     * `pollIntervalMs` and throw assertion failure otherwise. This should be used instead of
     * `Thread.sleep` whenever possible as it allows a longer timeout to be used without
     * unnecessarily increasing test time (as the condition is checked frequently). The longer
     * timeout is needed to avoid transient failures due to slow or overloaded machines.
     */
    @Throws(InterruptedException::class)
    fun waitForCondition(
        testCondition: TestCondition,
        maxWaitMs: Long = DEFAULT_MAX_WAIT_MS,
        pollIntervalMs: Long = DEFAULT_POLL_INTERVAL_MS,
        conditionDetailsSupplier: Supplier<String?>?,
    ) {
        retryOnExceptionWithTimeout(maxWaitMs, pollIntervalMs) {
            val conditionDetailsSupplied = conditionDetailsSupplier?.get()
            val conditionDetails = conditionDetailsSupplied ?: ""

            assertTrue(
                actual = testCondition.conditionMet(),
                message = "Condition not met within timeout $maxWaitMs. $conditionDetails"
            )
        }
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now [Exception]s or
     * [AssertionError]s, or for the given timeout to expire. If the timeout expires then the last
     * exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param timeoutMs the total time in milliseconds to wait for `runnable` to complete
     * successfully.
     * @param pollIntervalMs the interval in milliseconds to wait between invoking `runnable`.
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for
     * `runnable` to complete successfully.
     */
    @Throws(InterruptedException::class)
    fun retryOnExceptionWithTimeout(
        timeoutMs: Long = DEFAULT_MAX_WAIT_MS,
        pollIntervalMs: Long = DEFAULT_POLL_INTERVAL_MS,
        runnable: ValuelessCallable,
    ) {
        val expectedEnd = System.currentTimeMillis() + timeoutMs
        while (true) {
            try {
                runnable.call()
                return
            } catch (e: NoRetryException) {
                throw e
            } catch (t: AssertionError) {
                if (expectedEnd <= System.currentTimeMillis()) throw t
            } catch (e: Exception) {
                if (expectedEnd <= System.currentTimeMillis()) throw AssertionError(
                    "Assertion failed with an exception after $timeoutMs ms",
                    e
                )
            }
            Thread.sleep(min(pollIntervalMs, timeoutMs))
        }
    }

    /**
     * Checks if a cluster id is valid.
     * @param clusterId
     */
    fun isValidClusterId(clusterId: String) {
        assertNotNull(clusterId)

        // Base 64 encoded value is 22 characters
        assertEquals(clusterId.length, 22)
        val clusterIdPattern = Pattern.compile("[a-zA-Z0-9_\\-]+")
        val matcher = clusterIdPattern.matcher(clusterId)
        assertTrue(matcher.matches())

        // Convert into normal variant and add padding at the end.
        val originalClusterId = "${clusterId.replace("_", "/").replace("-", "+")}=="
        val decodedUuid = Base64.getDecoder().decode(originalClusterId)

        // We expect 16 bytes, same as the input UUID.
        assertEquals(decodedUuid.size, 16)

        //Check if it can be converted back to a UUID.
        try {
            val uuidBuffer = ByteBuffer.wrap(decodedUuid)
            UUID(uuidBuffer.getLong(), uuidBuffer.getLong()).toString()
        } catch (e: Exception) {
            fail("$clusterId cannot be converted back to UUID.")
        }
    }

    /**
     * Checks the two iterables for equality by first converting both to a list.
     */
    fun <T> checkEquals(it1: Iterable<T>, it2: Iterable<T>) {
        assertEquals(it1.toList(), it2.toList())
    }

    fun <T> checkEquals(it1: Iterator<T>?, it2: Iterator<T>?) {
        assertEquals(it1?.asSequence()?.toList(), it2?.asSequence()?.toList())
    }

    fun <T> checkEquals(c1: Set<T>, c2: Set<T>, firstDesc: String?, secondDesc: String?) {
        if (c1 != c2) {
            val missing1: MutableSet<T> = HashSet(c2)
            missing1.removeAll(c1)
            val missing2: MutableSet<T> = HashSet(c1)
            missing2.removeAll(c2)
            fail("Sets not equal, missing $firstDesc=$missing1, missing $secondDesc=$missing2")
        }
    }

    @Deprecated("Use Iterable.toList() instead.")
    fun <T> toList(iterable: Iterable<T>): List<T> {
        val list: MutableList<T> = ArrayList()
        for (item: T in iterable) list.add(item)
        return list
    }

    @Deprecated("Use Collection.toSet() instead.")
    fun <T> toSet(collection: Collection<T>): Set<T> = collection.toSet()

    fun toBuffer(send: Send): ByteBuffer {
        val channel = ByteBufferChannel(send.size())
        try {
            assertEquals(send.size(), send.writeTo(channel))
            channel.close()
            return channel.buffer()
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    fun toBuffer(records: UnalignedRecords): ByteBuffer = toBuffer(records.toSend())

    fun generateRandomTopicPartitions(
        numTopic: Int,
        numPartitionPerTopic: Int
    ): Set<TopicPartition> {
        val tps: MutableSet<TopicPartition> = HashSet()
        for (i in 0 until numTopic) {
            val topic = randomString(32)
            for (j in 0 until numPartitionPerTopic) tps.add(TopicPartition(topic, j))
        }
        return tps
    }

    /**
     * Assert that a future raises an expected exception cause type. Return the exception cause
     * if the assertion succeeds; otherwise raise AssertionError.
     *
     * @param future The future to await
     * @param exceptionCauseClass Class of the expected exception cause
     * @param T Exception cause type parameter
     * @return The caught exception cause
     */
    fun <T : Throwable?> assertFutureThrows(future: Future<*>, exceptionCauseClass: Class<T>): T {
        val exception = assertFailsWith<ExecutionException> { future.get() }
        assertTrue(
            actual = exceptionCauseClass.isInstance(exception.cause),
            message = "Unexpected exception cause " + exception.cause,
        )
        return exceptionCauseClass.cast(exception.cause)
    }

    fun <T : Throwable?> assertFutureThrows(
        future: Future<*>,
        expectedCauseClassApiException: Class<T>,
        expectedMessage: String?
    ) {
        val receivedException = assertFutureThrows(future, expectedCauseClassApiException)
        assertEquals(expectedMessage, receivedException!!.message)
    }

    @Throws(InterruptedException::class)
    fun assertFutureError(future: Future<*>, exceptionClass: Class<out Throwable?>) {
        try {
            future.get()
            fail("Expected a ${exceptionClass.simpleName} exception, but got success.")
        } catch (ee: ExecutionException) {
            val cause = ee.cause
            assertEquals(
                expected = exceptionClass,
                actual = cause!!.javaClass,
                message = "Expected a ${exceptionClass.simpleName} exception, but got ${cause.javaClass.simpleName}"
            )
        }
    }

    @Throws(InterruptedException::class)
    inline fun <reified T : Throwable?> assertFutureError(future: Future<*>) {
        try {
            future.get()
            fail("Expected a ${T::class.simpleName} exception, but got success.")
        } catch (ee: ExecutionException) {
            val cause = ee.cause
            assertIs<T>(
                value = cause!!,
                message = "Expected a ${T::class.simpleName} exception, but got ${cause.javaClass.simpleName}",
            )
        }
    }

    fun apiKeyFrom(networkReceive: NetworkReceive): ApiKeys {
        return RequestHeader.parse(networkReceive.payload()!!.duplicate()).apiKey
    }

    @Deprecated("Use assertNullable() instead.")
    fun <T> assertOptional(optional: Optional<T>, assertion: Consumer<T>) {
        if (optional.isPresent) assertion.accept(optional.get())
        else fail("Missing value from Optional")
    }

    fun <T> assertNullable(nullable: T?, assertion: (T) -> Unit) {
        nullable?.let { assertion(it) } ?: fail("Missing value from Optional")
    }

    fun <T> fieldValue(o: Any, clazz: Class<*>, fieldName: String): T {
        try {
            val field = clazz.getDeclaredField(fieldName)
            field.isAccessible = true
            return field[o] as T
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    @Throws(Exception::class)
    fun setFieldValue(obj: Any, fieldName: String, value: Any?) {
        val field = obj.javaClass.getDeclaredField(fieldName)
        field.isAccessible = true
        field[obj] = value
    }

    /**
     * Returns true if both iterators have same elements in the same order.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param T type of element in the iterators.
     */
    fun <T> sameElementsWithOrder(
        iterator1: Iterator<T>,
        iterator2: Iterator<T>,
    ): Boolean {
        while (iterator1.hasNext()) {
            if (!iterator2.hasNext()) return false
            if (iterator1.next() != iterator2.next()) return false
        }
        return !iterator2.hasNext()
    }

    /**
     * Returns true if both the iterators have same set of elements irrespective of order and
     * duplicates.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param T type of element in the iterators.
     */
    fun <T> sameElementsWithoutOrder(
        iterator1: Iterator<T>,
        iterator2: Iterator<T>,
    ): Boolean {
        // Check both the iterators have the same set of elements irrespective of order and
        // duplicates.
        val allSegmentsSet = iterator1.asSequence().toHashSet()
        val expectedSegmentsSet = iterator2.asSequence().toHashSet()
        return (allSegmentsSet == expectedSegmentsSet)
    }

    fun <T> assertNotFails(block: () -> T) : T {
        try {
            return block()
        } catch (e: Exception) {
            fail("Unexpected exception thrown: ${e.message}")
        }
    }
}
