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

class MockVaultConfigProvider : FileConfigProvider() {

    var vaultConfigs: Map<String, *>? = null

    private var configured = false

    @Throws(IOException::class)
    override fun reader(path: String): Reader {
        val vaultLocation = vaultConfigs!![LOCATION] as String?
        return StringReader("truststoreKey=testTruststoreKey\ntruststorePassword=randomtruststorePassword\ntruststoreLocation=$vaultLocation\n")
    }

    override fun configure(configs: Map<String, Any?>) {
        vaultConfigs = configs
        configured = true
    }

    fun configured(): Boolean {
        return configured
    }

    companion object {
        private const val LOCATION = "location"
    }
}
