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

package org.apache.kafka.server.record

import org.apache.kafka.common.record.CompressionType

enum class BrokerCompressionType(val altName: String) {

    UNCOMPRESSED("uncompressed") {
        override fun targetCompressionType(
            producerCompressionType: CompressionType,
        ): CompressionType = CompressionType.NONE
    },
    ZSTD("zstd") {
        override fun targetCompressionType(
            producerCompressionType: CompressionType,
        ): CompressionType = CompressionType.ZSTD
    },
    LZ4("lz4") {
        override fun targetCompressionType(
            producerCompressionType: CompressionType,
        ): CompressionType = CompressionType.LZ4
    },
    SNAPPY("snappy") {
        override fun targetCompressionType(
            producerCompressionType: CompressionType,
        ): CompressionType = CompressionType.SNAPPY
    },
    GZIP("gzip") {
        override fun targetCompressionType(
            producerCompressionType: CompressionType,
        ): CompressionType = CompressionType.GZIP
    },
    PRODUCER("producer") {
        override fun targetCompressionType(
            producerCompressionType: CompressionType,
        ): CompressionType = producerCompressionType
    };

    abstract fun targetCompressionType(producerCompressionType: CompressionType): CompressionType

    companion object {

        fun names(): List<String> = entries.map { v -> v.altName }

        fun forName(n: String): BrokerCompressionType {
            val name = n.lowercase()
            return requireNotNull(entries.firstOrNull { it.altName == name }) {
                "Unknown broker compression type name: $name"
            }
        }
    }
}
