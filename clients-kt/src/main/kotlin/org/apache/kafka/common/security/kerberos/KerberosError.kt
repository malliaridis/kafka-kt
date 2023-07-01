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

package org.apache.kafka.common.security.kerberos

import java.lang.reflect.Method
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator
import org.apache.kafka.common.utils.Java
import org.ietf.jgss.GSSException
import org.slf4j.LoggerFactory

/**
 * Kerberos exceptions that may require special handling. The standard Kerberos error codes for
 * these errors are retrieved using KrbException#errorCode() from the underlying Kerberos exception
 * thrown during [SaslClient.evaluateChallenge].
 */
enum class KerberosError(private val errorCode: Int, private val retriable: Boolean) {

    // (Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)
    // This is retriable, but included here to add extra logging for this case.
    SERVER_NOT_FOUND(7, false),

    // (Mechanism level: Client not yet valid - try again later (21))
    CLIENT_NOT_YET_VALID(21, true),

    // (Mechanism level: Ticket not yet valid (33) - Ticket not yet valid)])
    // This could be a small timing window.
    TICKET_NOT_YET_VALID(33, true),

    // (Mechanism level: Request is a replay (34) - Request is a replay)
    // Replay detection used to prevent DoS attacks can result in false positives, so retry on error.
    REPLAY(34, true);

    fun retriable(): Boolean {
        return retriable
    }

    companion object {

        private val log = LoggerFactory.getLogger(SaslClientAuthenticator::class.java)

        private val KRB_EXCEPTION_CLASS: Class<*>

        private val KRB_EXCEPTION_RETURN_CODE_METHOD: Method

        private const val IBM_KERBEROS_EXCEPTION_CLASS = "com.ibm.security.krb5.KrbException"

        private const val IBM_INTERNAL_KERBEROS_EXCEPTION_CLASS =
            "com.ibm.security.krb5.internal.KrbException"

        private const val SUN_KERBEROS_EXCEPTION_CLASS = "sun.security.krb5.KrbException"

        init {
            try {
                // different IBM JDKs versions include different security implementations
                KRB_EXCEPTION_CLASS = if (Java.isIbmJdk && canLoad(IBM_KERBEROS_EXCEPTION_CLASS))
                    Class.forName(IBM_KERBEROS_EXCEPTION_CLASS)
                else if (Java.isIbmJdk && canLoad(IBM_INTERNAL_KERBEROS_EXCEPTION_CLASS))
                    Class.forName(IBM_INTERNAL_KERBEROS_EXCEPTION_CLASS)
                else Class.forName(SUN_KERBEROS_EXCEPTION_CLASS)

                KRB_EXCEPTION_RETURN_CODE_METHOD = KRB_EXCEPTION_CLASS.getMethod("returnCode")
            } catch (e: Exception) {
                throw KafkaException("Kerberos exceptions could not be initialized", e)
            }
        }

        private fun canLoad(clazz: String): Boolean {
            return try {
                Class.forName(clazz)
                true
            } catch (e: Exception) {
                false
            }
        }

        fun fromException(exception: Exception): KerberosError? {
            var cause = exception.cause

            while (cause != null && !KRB_EXCEPTION_CLASS.isInstance(cause)) cause = cause.cause

            return cause?.let {
                try {
                    val errorCode = KRB_EXCEPTION_RETURN_CODE_METHOD.invoke(cause) as Int
                    fromErrorCode(errorCode)
                } catch (e: Exception) {
                    log.trace(
                        "Kerberos return code could not be determined from {} due to {}",
                        exception,
                        e
                    )
                    null
                }
            }
        }

        private fun fromErrorCode(errorCode: Int): KerberosError? {
            return values().firstOrNull { it.errorCode == errorCode }
        }

        /**
         * Returns true if the exception should be handled as a transient failure on clients. We
         * handle GSSException.NO_CRED as retriable on the client-side since this may occur during
         * re-login if a clients attempts to authentication after logout, but before the subsequent
         * login.
         */
        fun isRetriableClientGssException(exception: Exception): Boolean {
            var cause = exception.cause
            while (cause != null && cause !is GSSException) cause = cause.cause

            if (cause != null) {
                val gssException = cause as GSSException
                return gssException.major == GSSException.NO_CRED
            }
            return false
        }
    }
}
