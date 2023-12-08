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

import java.nio.channels.FileChannel
import org.apache.kafka.common.utils.ByteBufferUnmapper.unmap
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.Test

class ByteBufferUnmapperTest {

    /**
     * Checks that unmap doesn't throw exceptions.
     */
    @Test
    @Throws(Exception::class)
    fun testUnmap() {
        val file = tempFile()
        FileChannel.open(file.toPath()).use { channel ->
            val map = channel.map(FileChannel.MapMode.READ_ONLY, 0, 0)
            unmap(file.absolutePath, map)
        }
    }
}
