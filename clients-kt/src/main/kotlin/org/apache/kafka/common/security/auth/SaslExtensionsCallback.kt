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

package org.apache.kafka.common.security.auth

import javax.security.auth.callback.Callback

/**
 * Optional callback used for SASL mechanisms if any extensions need to be set in the SASL exchange.
 */
class SaslExtensionsCallback : Callback {

    /**
     * [SaslExtensions] consisting of the extension names and values that are sent by the client to
     * the server in the initial client SASL authentication message. The default value is
     * [SaslExtensions.empty] so that if this callback is unhandled the client will see a non-null
     * value.
     */
    var extensions = SaslExtensions.empty()

    /**
     * Returns always non-null [SaslExtensions] consisting of the extension names and values that
     * are sent by the client to the server in the initial client SASL authentication message. The
     * default value is [SaslExtensions.empty] so that if this callback is unhandled the client will
     * see a non-null value.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("extensions"),
    )
    fun extensions(): SaslExtensions = extensions

    /**
     * Sets the SASL extensions on this callback.
     *
     * @param extensions
     * the mandatory extensions to set
     */
    @Deprecated("Use property instead")
    fun extensions(extensions: SaslExtensions) {
        this.extensions = extensions
    }
}
