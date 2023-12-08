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

package org.apache.kafka.common.security.ssl.mock

import java.security.Provider

class TestProvider internal constructor(
    name: String? = "TestProvider",
    version: Double = 0.1,
    info: String? = "provider for test cases",
) : Provider(name, version, info) {

    init {
        super.put(KEY_MANAGER_FACTORY, TestKeyManagerFactory::class.java.getName())
        super.put(TRUST_MANAGER_FACTORY, TestTrustManagerFactory::class.java.getName())
    }

    companion object {

        private const val KEY_MANAGER_FACTORY = "KeyManagerFactory.${TestKeyManagerFactory.ALGORITHM}"

        private const val TRUST_MANAGER_FACTORY = "TrustManagerFactory.${TestTrustManagerFactory.ALGORITHM}"
    }
}
