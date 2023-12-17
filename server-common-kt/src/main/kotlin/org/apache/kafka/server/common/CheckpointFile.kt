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

package org.apache.kafka.server.common

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import org.apache.kafka.common.utils.Utils.atomicMoveWithFallback
import org.apache.kafka.server.common.CheckpointFile.EntryFormatter

/**
 * This class represents a utility to capture a checkpoint in a file. It writes down to the file in the below format.
 *
 *```
 * ========= File beginning =========
 * version: int
 * entries-count: int
 * entry-as-string-on-each-line
 * ========= File end ===============
 *```
 *
 * Each entry is represented as a string on each line in the checkpoint file. [EntryFormatter] is used
 * to convert the entry into a string and vice versa.
 *
 * @param T entry type.
 */
class CheckpointFile<T>(
    file: File,
    private val version: Int,
    private val formatter: EntryFormatter<T>,
) {

    private val lock = Any()

    private val absolutePath: Path

    private val tempPath: Path

    init {
        try {
            // Create the file if it does not exist.
            Files.createFile(file.toPath())
        } catch (_: FileAlreadyExistsException) {
            // Ignore if file already exists.
        }
        absolutePath = file.toPath().toAbsolutePath()
        tempPath = Paths.get("$absolutePath.tmp")
    }

    @Throws(IOException::class)
    fun write(entries: Collection<T>) {
        synchronized(lock) {
            FileOutputStream(tempPath.toFile()).use { fileOutputStream ->
                BufferedWriter(OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)).use { writer ->
                    val checkpointWriteBuffer = CheckpointWriteBuffer(
                        writer = writer,
                        version = version,
                        formatter = formatter,
                    )
                    checkpointWriteBuffer.write(entries)
                    writer.flush()
                    fileOutputStream.getFD().sync()
                }
            }
            atomicMoveWithFallback(tempPath, absolutePath)
        }
    }

    @Throws(IOException::class)
    fun read(): List<T> {
        synchronized(lock) {
            Files.newBufferedReader(absolutePath).use { reader ->
                val checkpointBuffer = CheckpointReadBuffer(
                    location = absolutePath.toString(),
                    reader = reader,
                    version = version,
                    formatter = formatter,
                )
                return checkpointBuffer.read()
            }
        }
    }

    class CheckpointWriteBuffer<T>(
        private val writer: BufferedWriter,
        private val version: Int,
        private val formatter: EntryFormatter<T>,
    ) {

        @Throws(IOException::class)
        fun write(entries: Collection<T>) {
            // Write the version
            writer.write(version.toString())
            writer.newLine()

            // Write the entries count
            writer.write(entries.size.toString())
            writer.newLine()

            // Write each entry on a new line.
            for (entry: T in entries) {
                writer.write(formatter.toString(entry))
                writer.newLine()
            }
        }
    }

    class CheckpointReadBuffer<T>(
        private val location: String,
        private val reader: BufferedReader,
        private val version: Int,
        private val formatter: EntryFormatter<T>,
    ) {

        @Throws(IOException::class)
        fun read(): List<T> {
            var line: String? = reader.readLine() ?: return emptyList()
            val readVersion = toInt(line!!)
            if (readVersion != version) throw IOException(
                "Unrecognised version:$readVersion, expected version: $version in checkpoint file at: $location"
            )
            line = reader.readLine()
            if (line == null) return emptyList()

            val expectedSize = toInt(line)
            val entries = ArrayList<T>(expectedSize)
            line = reader.readLine()
            while (line != null) {
                val maybeEntry = formatter.fromString(line) ?: throw buildMalformedLineException(line)
                entries.add(maybeEntry)
                line = reader.readLine()
            }

            if (entries.size != expectedSize) throw IOException(
                "Expected [$expectedSize] entries in checkpoint file [$location], but found only [${entries.size}]"
            )

            return entries
        }

        @Throws(IOException::class)
        private fun toInt(line: String): Int {
            try {
                return line.toInt()
            } catch (e: NumberFormatException) {
                throw buildMalformedLineException(line)
            }
        }

        private fun buildMalformedLineException(line: String): IOException {
            return IOException("Malformed line in checkpoint file [$location]: $line")
        }
    }

    /**
     * This is used to convert the given entry of type [T] into a string and vice versa.
     *
     * @param T entry type
     */
    interface EntryFormatter<T> {

        /**
         * @param entry entry to be converted into string.
         * @return String representation of the given entry.
         */
        fun toString(entry: T): String

        /**
         * @param value string representation of an entry.
         * @return entry converted from the given string representation if possible. `null` represents
         * that the given string representation could not be converted into an entry.
         */
        fun fromString(value: String): T?
    }
}
