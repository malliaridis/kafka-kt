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
import java.io.DataOutput
import java.io.EOFException
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.PrintWriter
import java.io.StringWriter
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.text.ParseException
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.AbstractMap
import java.util.Collections
import java.util.EnumSet
import java.util.Locale
import java.util.Objects
import java.util.Properties
import java.util.SortedSet
import java.util.TreeSet
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BiConsumer
import java.util.function.BinaryOperator
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate
import java.util.function.Supplier
import java.util.regex.Pattern
import java.util.stream.Collector
import java.util.stream.Collectors
import java.util.stream.Stream
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.TransferableChannel
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object Utils {
    // This matches URIs of formats: host:port and protocol:\\host:port
    // IPv6 is supported with [ip] pattern
    private val HOST_PORT_PATTERN = Pattern.compile(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)")

    private val VALID_HOST_CHARACTERS = Pattern.compile("([0-9a-zA-Z\\-%._:]*)")

    // Prints up to 2 decimal digits. Used for human-readable printing
    private val TWO_DIGIT_FORMAT = DecimalFormat(
        "0.##",
        DecimalFormatSymbols.getInstance(Locale.ENGLISH)
    )

    private val BYTE_SCALE_SUFFIXES = arrayOf("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

    val NL = System.getProperty("line.separator")

    private val log = LoggerFactory.getLogger(Utils::class.java)

    /**
     * Get a sorted list representation of a collection.
     * @param collection The collection to sort
     * @param T The class of objects in the collection
     * @return An unmodifiable sorted list with the contents of the collection
     */
    @Deprecated("Use Kotlin sorted collection function instead.")
    fun <T : Comparable<T>> sorted(collection: Collection<T>): List<T> {
        return collection.sorted()
    }

    /**
     * Turn the given UTF8 byte array into a string
     *
     * @param bytes The byte array
     * @return The string
     */
    fun utf8(bytes: ByteArray): String = String(bytes)

    /**
     * Read a UTF8 string from a byte buffer. Note that the position of the byte buffer is not
     * affected by this method.
     *
     * @param buffer The buffer to read from
     * @param length The length of the string in bytes to read. Leave undefined to read the
     * remaining bytes.
     * @return The UTF8 string
     */
    fun utf8(buffer: ByteBuffer, length: Int = buffer.remaining()): String {
        return utf8(buffer, 0, length)
    }

    /**
     * Read a UTF8 string from a byte buffer at a given offset. Note that the position of the byte buffer
     * is not affected by this method.
     *
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position in the buffer
     * @param length The length of the string in bytes
     * @return The UTF8 string
     */
    fun utf8(buffer: ByteBuffer, offset: Int, length: Int): String {
        return if (buffer.hasArray()) String(
            bytes = buffer.array(),
            offset = buffer.arrayOffset() + buffer.position() + offset,
            length = length,
            charset = StandardCharsets.UTF_8
        ) else utf8(toArray(buffer, offset, length))
    }

    /**
     * Turn a string into an utf8 byte[].
     *
     * @param string The string
     * @return The byte[]
     */
    @Deprecated("Use Kotlin's String.toByteArray() instead.")
    fun utf8(string: String): ByteArray {
        return string.toByteArray(StandardCharsets.UTF_8)
    }

    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0. This is
     * different from kotlin.math.abs, java.lang.Math.abs or scala.math.abs in that they return
     * Int.MinValue (!).
     */
    fun abs(n: Int): Int {
        return if (n == Int.MIN_VALUE) 0 else kotlin.math.abs(n)
    }

    /**
     * Get the minimum of some long values.
     *
     * @param first Used to ensure at least one value
     * @param rest The remaining values to compare
     * @return The minimum of all passed values
     */
    @Deprecated("Use  kotlin.comparisons.minOf instead.")
    fun min(first: Long, vararg rest: Long): Long {
        var min = first
        for (r in rest) {
            if (r < min) min = r
        }
        return min
    }

    /**
     * Get the maximum of some long values.
     * @param first Used to ensure at least one value
     * @param rest The remaining values to compare
     * @return The maximum of all passed values
     */
    @Deprecated("Use  kotlin.comparisons.maxOf instead.")
    fun max(first: Long, vararg rest: Long): Long {
        var max = first
        for (r in rest) {
            if (r > max) max = r
        }
        return max
    }

    @Deprecated("Use  kotlin.comparisons.minOf instead.")
    fun min(first: Short, second: Short): Short {
        return Math.min(first.toInt(), second.toInt()).toShort()
    }

    /**
     * Get the length for UTF8-encoding a string without encoding it first
     *
     * @param s The string to calculate the length for
     * @return The length when serialized
     */
    fun utf8Length(s: CharSequence): Int {
        var count = 0
        var i = 0
        val len = s.length
        while (i < len) {
            val ch = s[i]
            if (ch.code <= 0x7F) {
                count++
            } else if (ch.code <= 0x7FF) {
                count += 2
            } else if (Character.isHighSurrogate(ch)) {
                count += 4
                ++i
            } else {
                count += 3
            }
            i++
        }
        return count
    }

    /**
     * Read a byte array from its current position given the size in the buffer
     * @param buffer The buffer to read from
     * @param size The number of bytes to read into the array
     */
    fun toArray(buffer: ByteBuffer, size: Int): ByteArray = toArray(buffer, 0, size)

    /**
     * Convert a ByteBuffer to a nullable array.
     * @param buffer The buffer to convert
     * @return The resulting array or null if the buffer is null
     */
    fun toNullableArray(buffer: ByteBuffer?): ByteArray? = buffer?.let { toArray(it) }

    /**
     * Wrap an array as a nullable ByteBuffer.
     * @param array The nullable array to wrap
     * @return The wrapping ByteBuffer or null if array is null
     */
    fun wrapNullable(array: ByteArray?): ByteBuffer? = array?.let { ByteBuffer.wrap(array) }

    /**
     * Read a byte array from the given offset and size in the buffer
     *
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position of the buffer.
     * @param size The number of bytes to read into the array. Reads till the limit if not defined.
     */
    fun toArray(buffer: ByteBuffer, offset: Int = 0, size: Int = buffer.remaining()): ByteArray {
        val dest = ByteArray(size)

        if (buffer.hasArray()) {
            System.arraycopy(
                buffer.array(),
                buffer.position() + buffer.arrayOffset() + offset,
                dest,
                0,
                size
            )
        } else {
            val pos = buffer.position()
            buffer.position(pos + offset)
            buffer[dest]
            buffer.position(pos)
        }

        return dest
    }

    /**
     * Starting from the current position, read an integer indicating the size of the byte array to read,
     * then read the array. Consumes the buffer: upon returning, the buffer's position is after the array
     * that is returned.
     * @param buffer The buffer to read a size-prefixed array from
     * @return The array
     */
    fun getNullableSizePrefixedArray(buffer: ByteBuffer): ByteArray? {
        val size = buffer.getInt()
        return getNullableArray(buffer, size)
    }

    /**
     * Read a byte array of the given size. Consumes the buffer: upon returning, the buffer's
     * position is after the array that is returned.
     *
     * @param buffer The buffer to read a size-prefixed array from
     * @param size The number of bytes to read out of the buffer
     * @return The array
     */
    fun getNullableArray(buffer: ByteBuffer, size: Int): ByteArray? {
        if (size > buffer.remaining()) {
            // preemptively throw this when the read is doomed to fail, so we don't have to allocate the array.
            throw BufferUnderflowException()
        }

        val oldBytes = if (size == -1) null else ByteArray(size)
        if (oldBytes != null) buffer[oldBytes]

        return oldBytes
    }

    /**
     * Returns a copy of src byte array
     * @param src The byte array to copy
     * @return The copy
     */
    @Deprecated("Use ByteArray.copyOf() instead.")
    fun copyArray(src: ByteArray): ByteArray = src.copyOf()

    /**
     * Compares two character arrays for equality using a constant-time algorithm, which is needed
     * for comparing passwords. Two arrays are equal if they have the same length and all
     * characters at corresponding positions are equal.
     *
     * All characters in the first array are examined to determine equality.
     * The calculation time depends only on the length of this first character array; it does not
     * depend on the length of the second character array or the contents of either array.
     *
     * @param first the first array to compare
     * @param second the second array to compare
     * @return true if the arrays are equal, or false otherwise
     */
    fun isEqualConstantTime(first: CharArray?, second: CharArray?): Boolean {
        @Suppress("ReplaceArrayEqualityOpWithArraysEquals")
        if (first == second) return true
        if (first == null || second == null) return false
        if (second.isEmpty()) return first.isEmpty()

        // time-constant comparison that always compares all characters in first array
        var matches = first.size == second.size
        for (i in first.indices) {
            val j = if (i < second.size) i else 0
            if (first[i] != second[j]) {
                matches = false
            }
        }
        return matches
    }

    /**
     * Sleep for a bit
     * @param ms The duration of the sleep
     */
    fun sleep(ms: Long) {
        try {
            Thread.sleep(ms)
        } catch (e: InterruptedException) {
            // this is okay, we just wake up early
            Thread.currentThread().interrupt()
        }
    }

    /**
     * Instantiate the class
     */
    fun <T> newInstance(c: Class<T>): T {
        return try {
            c.getDeclaredConstructor().newInstance()
        } catch (e: NoSuchMethodException) {
            throw KafkaException("Could not find a public no-argument constructor for ${c.name}", e)
        } catch (e: ReflectiveOperationException) {
            throw KafkaException("Could not instantiate class ${c.name}", e)
        } catch (e: RuntimeException) {
            throw KafkaException("Could not instantiate class ${c.name}", e)
        }
    }

    /**
     * Look up the class by name and instantiate it.
     * @param klass class name
     * @param base super class of the class to be instantiated
     * @param T the type of the base class
     * @return the new instance
     */
    @Throws(ClassNotFoundException::class)
    fun <T> newInstance(klass: String, base: Class<T>): T {
        return newInstance(loadClass(klass, base))
    }

    /**
     * Look up a class by name.
     *
     * @param klass class name
     * @param base super class of the class for verification
     * @param T the type of the base class
     * @return the new class
     */
    @Throws(ClassNotFoundException::class)
    fun <T> loadClass(klass: String, base: Class<T>): Class<out T> {
        return Class.forName(klass, true, contextOrKafkaClassLoader).asSubclass(base)
    }

    /**
     * Cast `klass` to `base` and instantiate it.
     *
     * @param klass The class to instantiate
     * @param base A know baseclass of klass.
     * @param T the type of the base class
     * @throws ClassCastException If `klass` is not a subclass of `base`.
     * @return the new instance.
     */
    fun <T> newInstance(klass: Class<*>, base: Class<T>): T {
        return newInstance(klass.asSubclass(base))
    }

    /**
     * Construct a new object using a class name and parameters.
     *
     * @param className The full name of the class to construct.
     * @param params A sequence of (type, object) elements.
     * @param T The type of object to construct.
     * @return The new object.
     * @throws ClassNotFoundException If there was a problem constructing the object.
     */
    @Throws(ClassNotFoundException::class)
    fun <T> newParameterizedInstance(className: String, vararg params: Any): T {
        val argTypes: Array<Class<*>?> = arrayOfNulls(params.size / 2)
        val args = arrayOfNulls<Any>(params.size / 2)

        return try {
            val c = Class.forName(
                className,
                true,
                contextOrKafkaClassLoader
            )
            for (i in 0 until params.size / 2) {
                argTypes[i] = params[2 * i] as Class<*>
                args[i] = params[2 * i + 1]
            }
            @Suppress("UNCHECKED_CAST")
            val constructor = c.getConstructor(*argTypes) as Constructor<T>
            constructor.newInstance(*args)
        } catch (exception: NoSuchMethodException) {
            throw ClassNotFoundException(
                String.format(
                    "Failed to find constructor with %s for %s",
                    argTypes.joinToString(", "),
                    className,
                ),
                exception
            )
        } catch (exception: InstantiationException) {
            throw ClassNotFoundException(
                String.format("Failed to instantiate %s", className),
                exception
            )
        } catch (exception: IllegalAccessException) {
            throw ClassNotFoundException(
                String.format("Unable to access constructor of %s", className),
                exception
            )
        } catch (exception: InvocationTargetException) {
            throw KafkaException(
                String.format("The constructor of %s threw an exception", className),
                exception.cause
            )
        }
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data byte array to hash
     * @return 32-bit hash of the given array
     */
    fun murmur2(data: ByteArray): Int {
        val length = data.size
        val seed = -0x68b84d74
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        val m = 0x5bd1e995
        val r = 24

        // Initialize the hash to a random value
        var h = seed xor length
        val length4 = length / 4
        for (i in 0 until length4) {
            val i4 = i * 4
            var k =
                (data[i4 + 0].toInt() and 0xff) + (data[i4 + 1].toInt() and 0xff shl 8) + (data[i4 + 2].toInt() and 0xff shl 16) + (data[i4 + 3].toInt() and 0xff shl 24)
            k *= m
            k = k xor (k ushr r)
            k *= m
            h *= m
            h = h xor k
        }
        when (length % 4) {
            3 -> {
                h = h xor (data[(length and 3.inv()) + 2].toInt() and 0xff shl 16)
                h = h xor (data[(length and 3.inv()) + 1].toInt() and 0xff shl 8)
                h = h xor (data[length and 3.inv()].toInt() and 0xff)
                h *= m
            }

            2 -> {
                h = h xor (data[(length and 3.inv()) + 1].toInt() and 0xff shl 8)
                h = h xor (data[length and 3.inv()].toInt() and 0xff)
                h *= m
            }

            1 -> {
                h = h xor (data[length and 3.inv()].toInt() and 0xff)
                h *= m
            }
        }
        h = h xor (h ushr 13)
        h *= m
        h = h xor (h ushr 15)
        return h
    }

    /**
     * Extracts the hostname from a "host:port" address string.
     *
     * @param address address string to parse
     * @return hostname or `null` if the given `address` is incorrect
     */
    fun getHost(address: String): String? {
        val matcher = HOST_PORT_PATTERN.matcher(address)
        return if (matcher.matches()) matcher.group(1) else null
    }

    /**
     * Extracts the port number from a "host:port" address string.
     *
     * @param address address string to parse
     * @return port number or `null` if the given address is incorrect
     */
    fun getPort(address: String): Int? {
        val matcher = HOST_PORT_PATTERN.matcher(address)
        return if (matcher.matches()) matcher.group(2).toInt() else null
    }

    /**
     * Basic validation of the supplied address. checks for valid characters
     *
     * @param address hostname string to validate
     * @return true if address contains valid characters
     */
    fun validHostPattern(address: String): Boolean {
        return VALID_HOST_CHARACTERS.matcher(address).matches()
    }

    /**
     * Formats hostname and port number as a "host:port" address string,
     * surrounding IPv6 addresses with braces '[', ']'
     *
     * @param host hostname
     * @param port port number
     * @return address string
     */
    fun formatAddress(host: String, port: Int): String {
        return if (host.contains(":")) "[$host]:$port" // IPv6
        else "$host:$port"
    }

    /**
     * Formats a byte number as a human-readable String ("3.2 MB")
     *
     * @param bytes some size in bytes
     * @return
     */
    fun formatBytes(bytes: Long): String {
        if (bytes < 0) {
            return bytes.toString()
        }
        val asDouble = bytes.toDouble()
        val ordinal = Math.floor(Math.log(asDouble) / Math.log(1024.0)).toInt()
        val scale = Math.pow(1024.0, ordinal.toDouble())
        val scaled = asDouble / scale
        val formatted = TWO_DIGIT_FORMAT.format(scaled)
        return try {
            formatted + " " + BYTE_SCALE_SUFFIXES[ordinal]
        } catch (e: IndexOutOfBoundsException) {
            //huge number?
            asDouble.toString()
        }
    }

    /**
     * Create a string representation of an array joined by the given separator
     *
     * @param strs The array of items
     * @param separator The separator
     * @return The string representation.
     */
    @Deprecated("Use Kotlin collection operator.")
    fun <T> join(strs: Array<T>, separator: String): String {
        return strs.joinToString(separator)
    }

    /**
     * Create a string representation of a collection joined by the given separator
     *
     * @param collection The list of items
     * @param separator The separator
     * @return The string representation.
     */
    @Deprecated("Use Kotlin collection operator.")
    fun <T> join(collection: Collection<T>, separator: String): String {
        Objects.requireNonNull(collection)
        return mkString(collection.stream(), "", "", separator)
    }

    /**
     * Create a string representation of a stream surrounded by `begin` and `end` and joined by `separator`.
     *
     * @return The string representation.
     */
    fun <T> mkString(stream: Stream<T>, begin: String, end: String, separator: String): String {
        Objects.requireNonNull(stream)
        val sb = StringBuilder()
        sb.append(begin)
        val iter = stream.iterator()
        while (iter.hasNext()) {
            sb.append(iter.next())
            if (iter.hasNext()) sb.append(separator)
        }
        sb.append(end)
        return sb.toString()
    }

    /**
     * Converts a `Map` class into a string, concatenating keys and values
     * Example:
     * `mkString({ key: "hello", keyTwo: "hi" }, "|START|", "|END|", "=", ",")
     * => "|START|key=hello,keyTwo=hi|END|"`
     */
    fun <K, V> mkString(
        map: Map<K, V>,
        begin: String = "",
        end: String = "",
        keyValueSeparator: String,
        elementSeparator: String,
    ): String {
        val bld = StringBuilder()
        bld.append(begin)
        var prefix: String? = ""
        map.forEach { (key, value) ->
            bld.append(prefix).append(key).append(keyValueSeparator).append(value)
            prefix = elementSeparator
        }
        bld.append(end)
        return bld.toString()
    }

    /**
     * Converts an extensions string into a `Map<String, String>`.
     *
     * Example:
     * `parseMap("key=hey,keyTwo=hi,keyThree=hello", "=", ",") => { key: "hey", keyTwo: "hi", keyThree: "hello" }`
     *
     */
    fun parseMap(
        mapStr: String,
        keyValueSeparator: String,
        elementSeparator: String
    ): Map<String, String> {
        val map: MutableMap<String, String> = HashMap()
        if (mapStr.isNotEmpty()) {
            val attrvals = mapStr.split(elementSeparator.toRegex())
                .dropLastWhile { it.isEmpty() }
                .toTypedArray()

            attrvals.forEach { attrval ->
                val array = attrval.split(keyValueSeparator.toRegex(), limit = 2).toTypedArray()
                map[array[0]] = array[1]
            }
        }
        return map
    }
    /**
     * Read a properties file from the given path
     *
     * @param filename The path of the file to read
     * @param onlyIncludeKeys When non-null, only return values associated with these keys and
     * ignore all others
     * @return the loaded properties
     */
    @Throws(IOException::class)
    fun loadProps(filename: String?, onlyIncludeKeys: List<String>? = null): Properties {
        val props = Properties()

        if (filename != null) {
            Files.newInputStream(Paths.get(filename)).use { propStream -> props.load(propStream) }
        } else println("Did not load any properties since the property file is not specified")

        if (onlyIncludeKeys.isNullOrEmpty()) return props

        val requestedProps = Properties()
        onlyIncludeKeys.forEach(Consumer { key: String? ->
            val value = props.getProperty(key)
            if (value != null) requestedProps.setProperty(key, value)
        })

        return requestedProps
    }

    /**
     * Converts a Properties object to a Map<String></String>, String>, calling [toString] to
     * ensure all keys and values are Strings.
     */
    fun propsToStringMap(props: Properties): Map<String, String> {
        return props.map { (key, value) -> key.toString() to value.toString() }.toMap()
    }

    /**
     * Get the stack trace from an exception as a string
     */
    fun stackTrace(e: Throwable): String {
        val sw = StringWriter()
        val pw = PrintWriter(sw)
        e.printStackTrace(pw)
        return sw.toString()
    }

    /**
     * Read a buffer into a Byte array for the given offset and length
     */
    fun readBytes(buffer: ByteBuffer, offset: Int, length: Int): ByteArray {
        val dest = ByteArray(length)
        if (buffer.hasArray()) System.arraycopy(
            buffer.array(),
            buffer.arrayOffset() + offset,
            dest,
            0,
            length
        )
        else {
            buffer.mark()
            buffer.position(offset)
            buffer[dest]
            buffer.reset()
        }
        return dest
    }

    /**
     * Read the given byte buffer into a Byte array
     */
    fun readBytes(buffer: ByteBuffer): ByteArray {
        return readBytes(buffer, 0, buffer.limit())
    }

    /**
     * Read a file as string and return the content. The file is treated as a stream and no seek is
     * performed. This allows the program to read from a regular file as well as from a pipe/fifo.
     */
    @Throws(IOException::class)
    fun readFileAsString(path: String): String {
        return try {
            val allBytes = Files.readAllBytes(Paths.get(path))
            String(allBytes, StandardCharsets.UTF_8)
        } catch (ex: IOException) {
            throw IOException("Unable to read file $path", ex)
        }
    }

    /**
     * Check if the given ByteBuffer capacity
     *
     * @param existingBuffer ByteBuffer capacity to check
     * @param newLength new length for the ByteBuffer.
     * returns ByteBuffer
     */
    fun ensureCapacity(existingBuffer: ByteBuffer, newLength: Int): ByteBuffer {
        if (newLength > existingBuffer.capacity()) {
            val newBuffer = ByteBuffer.allocate(newLength)
            existingBuffer.flip()
            newBuffer.put(existingBuffer)
            return newBuffer
        }
        return existingBuffer
    }

    /**
     * Creates a set
     *
     * @param elems the elements
     * @param T the type of element
     * @return Set
     */
    @Deprecated("Use Kotlin built-in function setOf() instead")
    @SafeVarargs
    fun <T> mkSet(vararg elems: T): Set<T> {
        val result: MutableSet<T> = HashSet((elems.size / 0.75).toInt() + 1)
        for (elem in elems) result.add(elem)
        return result
    }

    /**
     * Creates a sorted set
     *
     * @param elems the elements
     * @param T the type of element, must be comparable
     * @return SortedSet
     */
    @SafeVarargs
    fun <T : Comparable<T>?> mkSortedSet(vararg elems: T): SortedSet<T> {
        val result: SortedSet<T> = TreeSet()
        for (elem in elems) result.add(elem)
        return result
    }

    /**
     * Creates a map entry (for use with [Utils.mkMap])
     *
     * @param k The key
     * @param v The value
     * @param K The key type
     * @param V The value type
     * @return An entry
     */
    @Deprecated("Use Kotlin map functions instead")
    fun <K, V> mkEntry(k: K, v: V): Map.Entry<K, V> {
        return AbstractMap.SimpleEntry(k, v)
    }

    /**
     * Creates a map from a sequence of entries
     *
     * @param entries The entries to map
     * @param K The key type
     * @param V The value type
     * @return A map
     */
    @Deprecated("Use Kotlin map functions instead")
    @SafeVarargs
    fun <K, V> mkMap(vararg entries: Map.Entry<K, V>): Map<K, V> {
        val result = LinkedHashMap<K, V>()
        for ((key, value) in entries) result[key] = value
        return result
    }

    /**
     * Creates a [Properties] from a map
     *
     * @param properties A map of properties to add
     * @return The properties object
     */
    fun mkProperties(properties: Map<String?, String?>): Properties {
        val result = Properties()
        for ((key, value) in properties) {
            result.setProperty(key, value)
        }
        return result
    }

    /**
     * Creates a [Properties] from a map
     *
     * @param properties A map of properties to add
     * @return The properties object
     */
    fun mkObjectProperties(properties: Map<String, Any>): Properties {
        val result = Properties()
        for ((key, value) in properties) {
            result[key] = value
        }
        return result
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param rootFile The root file at which to begin deleting
     */
    @Throws(IOException::class)
    fun delete(rootFile: File?) {
        if (rootFile == null) return
        Files.walkFileTree(rootFile.toPath(), object : SimpleFileVisitor<Path>() {
            @Throws(IOException::class)
            override fun visitFileFailed(path: Path, exc: IOException): FileVisitResult {
                // If the root path did not exist, ignore the error; otherwise throw it.
                if (exc is NoSuchFileException && path.toFile() == rootFile) return FileVisitResult.TERMINATE
                throw exc
            }

            @Throws(IOException::class)
            override fun visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult {
                Files.delete(path)
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun postVisitDirectory(path: Path, exc: IOException): FileVisitResult {
                // KAFKA-8999: if there's an exception thrown previously already, we should throw it
                if (exc != null) {
                    throw exc
                }
                Files.delete(path)
                return FileVisitResult.CONTINUE
            }
        })
    }

    /**
     * Returns an empty collection if this list is null
     * @param other
     * @return
     */
    fun <T> safe(other: List<T>?): List<T> {
        return other ?: emptyList()
    }

    val kafkaClassLoader: ClassLoader
        /**
         * Get the ClassLoader which loaded Kafka.
         */
        get() = Utils::class.java.classLoader
    val contextOrKafkaClassLoader: ClassLoader
        /**
         * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
         * loaded Kafka.
         *
         * This should be used whenever passing a ClassLoader to Class.forName
         */
        get() {
            val cl = Thread.currentThread().contextClassLoader
            return cl ?: kafkaClassLoader
        }
    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it fails.
     * This function allows callers to decide whether to flush the parent directory. This is needed
     * when a sequence of atomicMoveWithFallback is called for the same directory and we don't want
     * to repeatedly flush the same parent directory.
     *
     * @throws IOException if both atomic and non-atomic moves fail,
     * or parent dir flush fails if needFlushParentDir is true.
     */
    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it fails.
     * This function also flushes the parent directory to guarantee crash consistency.
     *
     * @throws IOException if both atomic and non-atomic moves fail, or parent dir flush fails.
     */
    @JvmOverloads
    @Throws(IOException::class)
    fun atomicMoveWithFallback(source: Path?, target: Path, needFlushParentDir: Boolean = true) {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE)
        } catch (outer: IOException) {
            try {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
                log.debug(
                    "Non-atomic move of {} to {} succeeded after atomic move failed due to {}",
                    source,
                    target,
                    outer.message
                )
            } catch (inner: IOException) {
                inner.addSuppressed(outer)
                throw inner
            }
        } finally {
            if (needFlushParentDir) {
                flushDir(target.toAbsolutePath().normalize().parent)
            }
        }
    }

    /**
     * Flushes dirty directories to guarantee crash consistency.
     *
     * Note: We don't fsync directories on Windows OS because otherwise it'll throw AccessDeniedException (KAFKA-13391)
     *
     * @throws IOException if flushing the directory fails.
     */
    @Throws(IOException::class)
    fun flushDir(path: Path?) {
        if (path != null && !OperatingSystem.IS_WINDOWS && !OperatingSystem.IS_ZOS) {
            FileChannel.open(path, StandardOpenOption.READ).use { dir ->
                dir.force(
                    true
                )
            }
        }
    }

    /**
     * Closes all the provided closeables.
     * @throws IOException if any of the close methods throws an IOException.
     * The first IOException is thrown with subsequent exceptions
     * added as suppressed exceptions.
     */
    @Throws(IOException::class)
    fun closeAll(vararg closeables: Closeable?) {
        var exception: IOException? = null
        for (closeable in closeables) {
            try {
                closeable?.close()
            } catch (e: IOException) {
                if (exception != null) exception.addSuppressed(e) else exception = e
            }
        }
        if (exception != null) throw exception
    }

    fun swallow(
        log: Logger,
        what: String?,
        runnable: Runnable
    ) {
        try {
            runnable.run()
        } catch (e: Throwable) {
            log.warn("{} error", what, e)
        }
    }

    /**
     * Closes `closeable` and if an exception is thrown, it is logged at the WARN level.
     * **Be cautious when passing method references as an argument.** For example:
     *
     *
     * `closeQuietly(task::stop, "source task");`
     *
     *
     * Although this method gracefully handles null [AutoCloseable] objects, attempts to take a method
     * reference from a null object will result in a [NullPointerException]. In the example code above,
     * it would be the caller's responsibility to ensure that `task` was non-null before attempting to
     * use a method reference from it.
     */
    fun closeQuietly(closeable: AutoCloseable?, name: String?) {
        if (closeable != null) {
            try {
                closeable.close()
            } catch (t: Throwable) {
                log.warn("Failed to close {} with type {}", name, closeable.javaClass.name, t)
            }
        }
    }

    /**
     * Closes `closeable` and if an exception is thrown, it is registered to the firstException parameter.
     * **Be cautious when passing method references as an argument.** For example:
     *
     *
     * `closeQuietly(task::stop, "source task");`
     *
     *
     * Although this method gracefully handles null [AutoCloseable] objects, attempts to take a method
     * reference from a null object will result in a [NullPointerException]. In the example code above,
     * it would be the caller's responsibility to ensure that `task` was non-null before attempting to
     * use a method reference from it.
     */
    fun closeQuietly(
        closeable: AutoCloseable?,
        name: String?,
        firstException: AtomicReference<Throwable?>
    ) {
        if (closeable != null) {
            try {
                closeable.close()
            } catch (t: Throwable) {
                firstException.compareAndSet(null, t)
                log.error("Failed to close {} with type {}", name, closeable.javaClass.name, t)
            }
        }
    }

    /**
     * close all closable objects even if one of them throws exception.
     * @param firstException keeps the first exception
     * @param name message of closing those objects
     * @param closeables closable objects
     */
    fun closeAllQuietly(
        firstException: AtomicReference<Throwable?>,
        name: String?,
        vararg closeables: AutoCloseable?
    ) {
        for (closeable in closeables) closeQuietly(closeable, name, firstException)
    }

    /**
     * close all closable objects even if one of them throws exception.
     * @param firstException keeps the first exception
     * @param name message of closing those objects
     * @param closeables closable objects
     */
    fun closeAllQuietly(
        firstException: AtomicReference<Throwable?>,
        name: String?,
        closeables: List<AutoCloseable?>
    ) {
        for (closeable in closeables) closeQuietly(closeable, name, firstException)
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolute
     * value.
     *
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition since it is used
     * in producer's partition selection logic [org.apache.kafka.clients.producer.KafkaProducer]
     *
     * @param number a given number
     * @return a positive number.
     */
    fun toPositive(number: Int): Int {
        return number and 0x7fffffff
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset.
     * @param buffer Buffer containing the size and data
     * @param start Offset in the buffer to read from
     * @return A slice of the buffer containing only the delimited data (excluding the size)
     */
    fun sizeDelimited(buffer: ByteBuffer, start: Int): ByteBuffer? {
        val size = buffer.getInt(start)
        return if (size < 0) null
        else {
            var b = buffer.duplicate()
            b.position(start + 4)
            b = b.slice()
            b.limit(size)
            b.rewind()
            b
        }
    }

    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the buffer. If the end
     * of the file is reached while there are bytes remaining in the buffer, an EOFException is thrown.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; it must be non-negative
     * @param description A description of what is being read, this will be included in the EOFException if it is thrown
     *
     * @throws IllegalArgumentException If position is negative
     * @throws EOFException If the end of the file is reached while there are remaining bytes in the destination buffer
     * @throws IOException If an I/O error occurs, see [FileChannel.read] for details on the
     * possible exceptions
     */
    @Throws(IOException::class)
    fun readFullyOrFail(
        channel: FileChannel, destinationBuffer: ByteBuffer, position: Long,
        description: String?
    ) {
        require(position >= 0) { "The file channel position cannot be negative, but it is $position" }
        val expectedReadBytes = destinationBuffer.remaining()
        readFully(channel, destinationBuffer, position)
        if (destinationBuffer.hasRemaining()) {
            throw EOFException(
                String.format(
                    "Failed to read `%s` from file channel `%s`. Expected to read %d bytes, " +
                            "but reached end of file after reading %d bytes. Started read from position %d.",
                    description,
                    channel,
                    expectedReadBytes,
                    expectedReadBytes - destinationBuffer.remaining(),
                    position
                )
            )
        }
    }

    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the buffer or the end
     * of the file has been reached.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; it must be non-negative
     *
     * @throws IllegalArgumentException If position is negative
     * @throws IOException If an I/O error occurs, see [FileChannel.read] for details on the
     * possible exceptions
     */
    @Throws(IOException::class)
    fun readFully(channel: FileChannel, destinationBuffer: ByteBuffer, position: Long) {
        require(position >= 0) { "The file channel position cannot be negative, but it is $position" }
        var currentPosition = position
        var bytesRead: Int
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition)
            currentPosition += bytesRead.toLong()
        } while (bytesRead != -1 && destinationBuffer.hasRemaining())
    }

    /**
     * Read data from the input stream to the given byte buffer until there are no bytes remaining in the buffer or the
     * end of the stream has been reached.
     *
     * @param inputStream Input stream to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred (it must be backed by an array)
     *
     * @throws IOException If an I/O error occurs
     */
    @Throws(IOException::class)
    fun readFully(inputStream: InputStream, destinationBuffer: ByteBuffer) {
        require(destinationBuffer.hasArray()) { "destinationBuffer must be backed by an array" }
        val initialOffset = destinationBuffer.arrayOffset() + destinationBuffer.position()
        val array = destinationBuffer.array()
        val length = destinationBuffer.remaining()
        var totalBytesRead = 0
        do {
            val bytesRead =
                inputStream.read(array, initialOffset + totalBytesRead, length - totalBytesRead)
            if (bytesRead == -1) break
            totalBytesRead += bytesRead
        } while (length > totalBytesRead)
        destinationBuffer.position(destinationBuffer.position() + totalBytesRead)
    }

    @Throws(IOException::class)
    fun writeFully(channel: FileChannel, sourceBuffer: ByteBuffer) {
        while (sourceBuffer.hasRemaining()) channel.write(sourceBuffer)
    }

    /**
     * Trying to write data in source buffer to a [TransferableChannel], we may need to call this method multiple
     * times since this method doesn't ensure the data in the source buffer can be fully written to the destination channel.
     *
     * @param destChannel The destination channel
     * @param position From which the source buffer will be written
     * @param length The max size of bytes can be written
     * @param sourceBuffer The source buffer
     *
     * @return The length of the actual written data
     * @throws IOException If an I/O error occurs
     */
    @Throws(IOException::class)
    fun tryWriteTo(
        destChannel: TransferableChannel,
        position: Int,
        length: Int,
        sourceBuffer: ByteBuffer
    ): Long {
        val dup = sourceBuffer.duplicate()
        dup.position(position)
        dup.limit(position + length)
        return destChannel.write(dup).toLong()
    }

    /**
     * Write the contents of a buffer to an output stream. The bytes are copied from the current position
     * in the buffer.
     * @param out The output to write to
     * @param buffer The buffer to write from
     * @param length The number of bytes to write
     * @throws IOException For any errors writing to the output
     */
    @Throws(IOException::class)
    fun writeTo(out: DataOutput, buffer: ByteBuffer, length: Int) {
        if (buffer.hasArray()) {
            out.write(buffer.array(), buffer.position() + buffer.arrayOffset(), length)
        } else {
            val pos = buffer.position()
            for (i in pos until length + pos) out.writeByte(buffer[i].toInt())
        }
    }

    @Deprecated("Use Kotlin function .toList() instead")
    fun <T> toList(iterable: Iterable<T>): List<T> = iterable.toList()

    @Deprecated("Use Kotlin function .asSequence().toList() instead")
    fun <T> toList(iterator: Iterator<T>): List<T> = iterator.asSequence().toList()

    @Deprecated("Use Kotlin function .filter(predicate) instead")
    fun <T> toList(iterator: Iterator<T>, predicate: Predicate<T>): List<T> {
        val res: MutableList<T> = ArrayList()
        while (iterator.hasNext()) {
            val e = iterator.next()
            if (predicate.test(e)) res.add(e)
        }
        return res
    }

    @Deprecated("Use Kotlin operator for list concatenation.")
    fun <T> concatListsUnmodifiable(left: List<T>, right: List<T>): List<T> {
        return concatLists(
            left, right
        ) { list: List<T>? -> Collections.unmodifiableList(list) }
    }

    @Deprecated("Use Kotlin operator for list concatenation.")
    fun <T> concatLists(
        left: List<T>,
        right: List<T>,
        finisher: (List<T>) -> List<T>
    ): List<T> = finisher(left + right)

    fun to32BitField(bytes: Set<Byte>): Int {
        var value = 0
        for (b in bytes) value = value or (1 shl checkRange(b).toInt())
        return value
    }

    private fun checkRange(i: Byte): Byte {
        require(i <= 31) { "out of range: i>31, i = $i" }
        require(i >= 0) { "out of range: i<0, i = $i" }
        return i
    }

    fun from32BitField(intValue: Int): Set<Byte> {
        val result: MutableSet<Byte> = HashSet()
        var itr = intValue
        var count = 0
        while (itr != 0) {
            if (itr and 1 != 0) result.add(count.toByte())
            count++
            itr = itr ushr 1
        }
        return result
    }

    @Deprecated("Use Kotlin collection functions.")
    fun <K1, V1, K2, V2> transformMap(
        map: Map<out K1, V1>,
        keyMapper: Function<K1, K2>,
        valueMapper: Function<V1, V2>
    ): Map<K2, V2> {
        return map.entries.stream().collect(
            Collectors.toMap(
                { (key): Map.Entry<K1, V1> -> keyMapper.apply(key) },
                { (_, value): Map.Entry<K1, V1> -> valueMapper.apply(value) }
            )
        )
    }

    /**
     * A Collector that offers two kinds of convenience:
     * 1. You can specify the concrete type of the returned Map
     * 2. You can turn a stream of Entries directly into a Map without having to mess with a key function
     * and a value function. In particular, this is handy if all you need to do is apply a filter to a Map's entries.
     *
     *
     * One thing to be wary of: These types are too "distant" for IDE type checkers to warn you if you
     * try to do something like build a TreeMap of non-Comparable elements. You'd get a runtime exception for that.
     *
     * @param mapSupplier The constructor for your concrete map type.
     * @param K The Map key type
     * @param V The Map value type
     * @param M The type of the Map itself.
     * @return new Collector<Map.Entry<K, V>, M, M>
     */
    @Deprecated("Use Kotlin operators instead.")
    fun <K, V, M : MutableMap<K, V>> entriesToMap(mapSupplier: Supplier<M>): Collector<Map.Entry<K, V>, M, M> {

        return object : Collector<Map.Entry<K, V>, M, M> {
            override fun supplier(): Supplier<M> {
                return mapSupplier
            }

            override fun accumulator(): BiConsumer<M, Map.Entry<K, V>> {
                return BiConsumer<M, Map.Entry<K, V>> { map: M, (key, value): Map.Entry<K, V> ->
                    map[key] = value
                }
            }

            override fun combiner(): BinaryOperator<M> {
                return BinaryOperator { map: M, map2: M ->
                    map.putAll(map2)
                    map
                }
            }

            override fun finisher(): Function<M, M> {
                return Function { map: M -> map }
            }

            override fun characteristics(): Set<Collector.Characteristics> {
                return EnumSet.of(
                    Collector.Characteristics.UNORDERED,
                    Collector.Characteristics.IDENTITY_FINISH
                )
            }
        }
    }

    @SafeVarargs
    fun <E> union(constructor: Supplier<MutableSet<E>>, vararg set: Set<E>): Set<E> {
        val result = constructor.get()
        set.forEach { result.addAll(it) }

        return result
    }

    @SafeVarargs
    fun <E> intersection(
        constructor: Supplier<MutableSet<E>>,
        first: Set<E>,
        vararg set: Set<E>
    ): Set<E> {
        val result = constructor.get()
        result.addAll(first)

        set.forEach { result.retainAll(it) }

        return result
    }

    fun <E> diff(constructor: Supplier<MutableSet<E>>, left: Set<E>?, right: Set<E>?): Set<E> {
        val result = constructor.get()
        result.addAll(left!!)
        result.removeAll(right!!)
        return result
    }

    fun <K, V> filterMap(map: Map<K, V>, filterPredicate: Predicate<Map.Entry<K, V>?>?): Map<K, V> {
        return map.entries.stream().filter(filterPredicate).collect(
            Collectors.toMap(
                { (key): Map.Entry<K, V> -> key },
                { (_, value): Map.Entry<K, V> -> value })
        )
    }

    /**
     * Convert a properties to map. All keys in properties must be string type. Otherwise, a
     * ConfigException is thrown.
     *
     * @param properties to be converted
     * @return a map including all elements in properties
     */
    fun propsToMap(properties: Properties): Map<String, Any?> {
        return properties.map { (key, value) ->
            if (key !is String) throw ConfigException(key.toString(), value, "Key must be a string.")
            key to value
        }.toMap()
    }

    /**
     * Convert timestamp to an epoch value
     *
     * @param timestamp the timestamp to be converted, the accepted formats are:
     * (1) yyyy-MM-dd'T'HH:mm:ss.SSS, ex: 2020-11-10T16:51:38.198
     * (2) yyyy-MM-dd'T'HH:mm:ss.SSSZ, ex: 2020-11-10T16:51:38.198+0800
     * (3) yyyy-MM-dd'T'HH:mm:ss.SSSX, ex: 2020-11-10T16:51:38.198+08
     * (4) yyyy-MM-dd'T'HH:mm:ss.SSSXX, ex: 2020-11-10T16:51:38.198+0800
     * (5) yyyy-MM-dd'T'HH:mm:ss.SSSXXX, ex: 2020-11-10T16:51:38.198+08:00
     *
     * @return epoch value of a given timestamp (i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT)
     * @throws ParseException for timestamp that doesn't follow ISO8601 format or the format is not expected
     */
    @Throws(ParseException::class, IllegalArgumentException::class)
    fun getDateTime(timestamp: String): Long {
        var timestampCopy = timestamp

        val timestampParts = timestampCopy.split("T".toRegex())
            .dropLastWhile { it.isEmpty() }
            .toTypedArray()

        if (timestampParts.size < 2) throw ParseException(
            "Error parsing timestamp. It does not contain a 'T' according to ISO8601 format",
            timestampCopy.length
        )

        val secondPart = timestampParts[1]
        if (!(secondPart.contains("+")
                    || secondPart.contains("-")
                    || secondPart.contains("Z"))
        ) timestampCopy += "Z"

        val simpleDateFormat = SimpleDateFormat()
        // strictly parsing the date/time format
        simpleDateFormat.isLenient = false

        return try {
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
            val date = simpleDateFormat.parse(timestampCopy)
            date.time
        } catch (e: ParseException) {
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
            val date = simpleDateFormat.parse(timestampCopy)
            date.time
        }
    }

    fun <S> covariantCast(iterator: Iterator<S>?): Iterator<S>? {
        return iterator
    }

    /**
     * Checks if a string is null, empty or whitespace only.
     * @param str a string to be checked
     * @return true if the string is null, empty or whitespace only; otherwise, return false.
     */
    fun isBlank(str: String?): Boolean {
        return str == null || str.trim { it <= ' ' }.isEmpty()
    }

    fun <K, V> initializeMap(keys: Collection<K>, valueSupplier: Supplier<V>): Map<K, V> {
        val res: MutableMap<K, V> = HashMap(keys.size)
        keys.forEach(Consumer { key: K -> res[key] = valueSupplier.get() })
        return res
    }

    /**
     * Get an array containing all of the [string representations][Object.toString] of a given enumerable type.
     * @param enumClass the enum class; may not be null
     * @return an array with the names of every value for the enum class; never null, but may be empty
     * if there are no values defined for the enum
     */
    fun enumOptions(enumClass: Class<out Enum<*>>): Array<String> {
        require(enumClass.isEnum) { "Class $enumClass is not an enumerable type" }

        return enumClass.enumConstants.map { it.toString() }.toTypedArray()
    }

    /**
     * Convert time instant to readable string for logging
     * @param timestamp the timestamp of the instant to be converted.
     *
     * @return string value of a given timestamp in the format "yyyy-MM-dd HH:mm:ss,SSS"
     */
    fun toLogDateTimeFormat(timestamp: Long): String {
        val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS XXX")
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault())
            .format(dateTimeFormatter)
    }

    /**
     * An [AutoCloseable] interface without a throws clause in the signature
     *
     * This is used with lambda expressions in try-with-resources clauses
     * to avoid casting un-checked exceptions to checked exceptions unnecessarily.
     */
    @FunctionalInterface
    interface UncheckedCloseable : AutoCloseable {
        override fun close()
    }
}
