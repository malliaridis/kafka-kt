package org.apache.kafka.common.network

import java.io.Closeable
import java.io.IOException
import java.util.*
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde

/**
 * Authentication for Channel
 */
interface Authenticator : Closeable {

    /**
     * Implements any authentication mechanism. Use transportLayer to read or write tokens.
     * For security protocols PLAINTEXT and SSL, this is a no-op since no further authentication
     * needs to be done. For SASL_PLAINTEXT and SASL_SSL, this performs the SASL authentication.
     *
     * @throws AuthenticationException if authentication fails due to invalid credentials or
     * other security configuration errors
     * @throws IOException if read/write fails due to an I/O error
     */
    @Throws(AuthenticationException::class, IOException::class)
    fun authenticate()

    /**
     * Perform any processing related to authentication failure. This is invoked when the channel is about to be closed
     * because of an [AuthenticationException] thrown from a prior [.authenticate] call.
     * @throws IOException if read/write fails due to an I/O error
     */
    @Throws(IOException::class)
    fun handleAuthenticationFailure() {
    }

    /**
     * Returns Principal using PrincipalBuilder
     */
    fun principal(): KafkaPrincipal?

    /**
     * Returns the serializer/deserializer interface for principal
     */
    fun principalSerde(): KafkaPrincipalSerde?

    /**
     * returns true if authentication is complete otherwise returns false;
     */
    fun complete(): Boolean

    /**
     * Begins re-authentication. Uses transportLayer to read or write tokens as is
     * done for [.authenticate]. For security protocols PLAINTEXT and SSL,
     * this is a no-op since re-authentication does not apply/is not supported,
     * respectively. For SASL_PLAINTEXT and SASL_SSL, this performs a SASL
     * authentication. Any in-flight responses from prior requests can/will be read
     * and collected for later processing as required. There must not be partially
     * written requests; any request queued for writing (for which zero bytes have
     * been written) remains queued until after re-authentication succeeds.
     *
     * @param reauthenticationContext
     * the context in which this re-authentication is occurring. This
     * instance is responsible for closing the previous Authenticator
     * returned by
     * [ReauthenticationContext.previousAuthenticator].
     * @throws AuthenticationException
     * if authentication fails due to invalid credentials or other
     * security configuration errors
     * @throws IOException
     * if read/write fails due to an I/O error
     */
    @Throws(IOException::class)
    fun reauthenticate(reauthenticationContext: ReauthenticationContext) = Unit

    /**
     * Return the session expiration time, if any, otherwise null. The value is in
     * nanoseconds as per `System.nanoTime()` and is therefore only useful
     * when compared to such a value -- it's absolute value is meaningless. This
     * value may be non-null only on the server-side. It represents the time after
     * which, in the absence of re-authentication, the broker will close the session
     * if it receives a request unrelated to authentication. We store nanoseconds
     * here to avoid having to invoke the more expensive `milliseconds()` call
     * on the broker for every request
     *
     * @return the session expiration time, if any, otherwise null
     */
    fun serverSessionExpirationTimeNanos(): Long? = null

    /**
     * Return the time on or after which a client should re-authenticate this
     * session, if any, otherwise null. The value is in nanoseconds as per
     * `System.nanoTime()` and is therefore only useful when compared to such
     * a value -- it's absolute value is meaningless. This value may be non-null
     * only on the client-side. It will be a random time between 85% and 95% of the
     * full session lifetime to account for latency between client and server and to
     * avoid re-authentication storms that could be caused by many sessions
     * re-authenticating simultaneously.
     *
     * @return the time on or after which a client should re-authenticate this
     * session, if any, otherwise null
     */
    fun clientSessionReauthenticationTimeNanos(): Long? = null

    /**
     * Return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable, otherwise null.
     * The server-side perspective will yield a lower value than the client-side
     * perspective of the same re-authentication because the client-side observes an
     * additional network round-trip.
     *
     * @return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable,
     * otherwise null
     */
    fun reauthenticationLatencyMs(): Long? = null

    /**
     * Return the next (always non-null but possibly empty) client-side
     * [NetworkReceive] response that arrived during re-authentication that
     * is unrelated to re-authentication, if any. These correspond to requests sent
     * prior to the beginning of re-authentication; the requests were made when the
     * channel was successfully authenticated, and the responses arrived during the
     * re-authentication process. The response returned is removed from the authenticator's
     * queue. Responses of requests sent after completion of re-authentication are
     * processed only when the authenticator response queue is empty.
     *
     * @return the (always non-null but possibly empty) client-side
     * [NetworkReceive] response that arrived during
     * re-authentication that is unrelated to re-authentication, if any
     */
    fun pollResponseReceivedDuringReauthentication(): NetworkReceive? = null

    /**
     * Return true if this is a server-side authenticator and the connected client
     * has indicated that it supports re-authentication, otherwise false
     *
     * @return true if this is a server-side authenticator and the connected client
     * has indicated that it supports re-authentication, otherwise false
     */
    fun connectedClientSupportsReauthentication(): Boolean = false
}
