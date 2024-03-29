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

package org.apache.kafka.common.errors

import javax.security.sasl.SaslServer

/**
 * This exception indicates that SASL authentication has failed. The error message
 * in the exception indicates the actual cause of failure.
 *
 *
 * SASL authentication failures typically indicate invalid credentials, but
 * could also include other failures specific to the SASL mechanism used
 * for authentication.
 *
 *
 * **Note:**If [SaslServer.evaluateResponse] throws this exception during
 * authentication, the message from the exception will be sent to clients in the SaslAuthenticate
 * response. Custom [SaslServer] implementations may throw this exception in order to
 * provide custom error messages to clients, but should take care not to include any
 * security-critical information in the message that should not be leaked to unauthenticated clients.
 *
 */
class SaslAuthenticationException : AuthenticationException {

    constructor(message: String?) : super(message)

    constructor(message : String?, cause: Throwable?) : super(message, cause)

    companion object {
        private const val serialVersionUID = 1L
    }
}
