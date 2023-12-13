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

package org.apache.kafka.common.config.provider

import java.io.IOException
import java.io.Reader
import java.io.StringReader
import java.util.Collections
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class MockFileConfigProvider : FileConfigProvider() {

    private var id: String? = null

    private var closed = false

    override fun configure(configs: Map<String, Any?>) {
        val id = configs["testId"] ?: throw RuntimeException("${javaClass.getName()} missing 'testId' config")
        if (this.id != null) throw RuntimeException("${javaClass.getName()} instance was configured twice")

        this.id = id.toString()
        INSTANCES[id.toString()] = this
    }

    @Throws(IOException::class)
    override fun reader(path: String): Reader {
        return StringReader("key=testKey\npassword=randomPassword")
    }

    @Synchronized
    override fun close() {
        closed = true
    }

    companion object {

        private val INSTANCES = Collections.synchronizedMap(HashMap<String, MockFileConfigProvider>())

        fun assertClosed(id: String) {
            val instance = INSTANCES.remove(id)
            assertNotNull(instance)
            synchronized(instance) { assertTrue(instance.closed) }
        }
    }
}
