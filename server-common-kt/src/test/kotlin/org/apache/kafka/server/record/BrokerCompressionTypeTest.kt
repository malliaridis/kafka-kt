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
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BrokerCompressionTypeTest {

    @Test
    fun testTargetCompressionType() {
        assertEquals(CompressionType.GZIP, BrokerCompressionType.GZIP.targetCompressionType(CompressionType.ZSTD))
        assertEquals(CompressionType.SNAPPY, BrokerCompressionType.SNAPPY.targetCompressionType(CompressionType.LZ4))
        assertEquals(CompressionType.LZ4, BrokerCompressionType.LZ4.targetCompressionType(CompressionType.ZSTD))
        assertEquals(CompressionType.ZSTD, BrokerCompressionType.ZSTD.targetCompressionType(CompressionType.GZIP))
        assertEquals(CompressionType.LZ4, BrokerCompressionType.PRODUCER.targetCompressionType(CompressionType.LZ4))
        assertEquals(CompressionType.ZSTD, BrokerCompressionType.PRODUCER.targetCompressionType(CompressionType.ZSTD))
    }
}
