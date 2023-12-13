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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import java.io.IOException
import java.nio.file.Path
import org.apache.kafka.common.utils.Utils.readFileAsString

/**
 * `FileTokenRetriever` is an [AccessTokenRetriever] that will load the contents,
 * interpreting them as a JWT access key in the serialized form.
 *
 * @see AccessTokenRetriever
 */
class FileTokenRetriever(private val accessTokenFile: Path) : AccessTokenRetriever {

    private var accessToken: String? = null

    @Throws(IOException::class)
    override fun init() {
        // always non-null; to remove any newline chars or backend will report err
        accessToken = readFileAsString(accessTokenFile.toFile().path).trim { it <= ' ' }
    }

    @Throws(IOException::class)
    override fun retrieve(): String = checkNotNull(accessToken) {
        "Access token is null; please call init() first"
    }
}
