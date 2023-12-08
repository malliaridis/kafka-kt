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

package org.apache.kafka.common.record

import org.apache.kafka.common.utils.BufferSupplier.GrowableBufferSupplier
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class BufferSupplierTest {
    
    @Test
    fun testGrowableBuffer() {
        val supplier = GrowableBufferSupplier()
        val buffer = supplier[1024]
        assertEquals(0, buffer.position())
        assertEquals(1024, buffer.capacity())
        supplier.release(buffer)
        val cached = supplier[512]
        assertEquals(0, cached.position())
        assertSame(buffer, cached)
        val increased = supplier[2048]
        assertEquals(2048, increased.capacity())
        assertEquals(0, increased.position())
    }
}
