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

package org.apache.kafka.trogdor.fault

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.TreeMap
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString


object Kibosh {

    private val processes = TreeMap<String, KiboshProcess>()

    @Deprecated(
        message = "Use Kibosh directly.",
        replaceWith = ReplaceWith("Kibosh"),
    )
    val INSTANCE = this

    const val KIBOSH_CONTROL = "kibosh_control"

    /**
     * Get or create a KiboshProcess object to manage the Kibosh process at a given path.
     */
    @Synchronized
    private fun findProcessObject(mountPath: String): KiboshProcess {
        val path = Paths.get(mountPath).normalize().toString()
        var process = processes[path]
        if (process == null) {
            process = KiboshProcess(mountPath)
            processes[path] = process
        }
        return process
    }

    /**
     * Add a new Kibosh fault.
     */
    @Throws(IOException::class)
    fun addFault(mountPath: String, spec: KiboshFaultSpec) {
        val process = findProcessObject(mountPath)
        process.addFault(spec)
    }

    /**
     * Remove a Kibosh fault.
     */
    @Throws(IOException::class)
    fun removeFault(mountPath: String, spec: KiboshFaultSpec) {
        val process = findProcessObject(mountPath)
        process.removeFault(spec)
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes(
        JsonSubTypes.Type(value = KiboshFilesUnreadableFaultSpec::class, name = "unreadable"),
    )
    abstract class KiboshFaultSpec {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            return if (other == null || javaClass != other.javaClass) false
            else toString() == other.toString()
        }

        override fun hashCode(): Int = toString().hashCode()

        override fun toString(): String = toJsonString(this)
    }

    class KiboshFilesUnreadableFaultSpec @JsonCreator constructor(
        @param:JsonProperty("prefix") private val prefix: String,
        @param:JsonProperty("errorCode") private val errorCode: Int,
    ) : KiboshFaultSpec() {

        @JsonProperty
        fun prefix(): String = prefix

        @JsonProperty
        fun errorCode(): Int = errorCode
    }

    private class KiboshProcess(mountPath: String?) {

        private val controlPath: Path = Paths.get(mountPath, KIBOSH_CONTROL)

        init {
            if (!Files.exists(controlPath)) throw RuntimeException("Can't find file $controlPath")
        }

        @Synchronized
        @Throws(IOException::class)
        fun addFault(toAdd: KiboshFaultSpec) {
            val file = KiboshControlFile.read(controlPath)
            val faults = file.faults() + toAdd
            KiboshControlFile(faults).write(controlPath)
        }

        @Synchronized
        @Throws(IOException::class)
        fun removeFault(toRemove: KiboshFaultSpec) {
            val file = KiboshControlFile.read(controlPath)
            val faults = mutableListOf<KiboshFaultSpec>()
            var foundToRemove = false
            for (fault in file.faults()) {
                if (fault == toRemove) {
                    foundToRemove = true
                } else faults.add(fault)
            }
            if (!foundToRemove) throw RuntimeException("Failed to find fault $toRemove. ")

            KiboshControlFile(faults).write(controlPath)
        }
    }

    class KiboshControlFile @JsonCreator constructor(
        @JsonProperty("faults") faults: List<KiboshFaultSpec>?,
    ) {

        private val faults: List<KiboshFaultSpec> = faults ?: emptyList()

        @JsonProperty
        fun faults(): List<KiboshFaultSpec> = faults

        @Throws(IOException::class)
        fun write(controlPath: Path?) {
            Files.write(controlPath, JsonUtil.JSON_SERDE.writeValueAsBytes(this))
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            return if (other == null || javaClass != other.javaClass) false
            else toString() == other.toString()
        }

        override fun hashCode(): Int = toString().hashCode()

        override fun toString(): String = toJsonString(this)

        companion object {

            val EMPTY = KiboshControlFile(emptyList())

            @Throws(IOException::class)
            fun read(controlPath: Path?): KiboshControlFile {
                val controlFileBytes = Files.readAllBytes(controlPath)
                return JsonUtil.JSON_SERDE.readValue(controlFileBytes, KiboshControlFile::class.java)
            }
        }
    }
}
