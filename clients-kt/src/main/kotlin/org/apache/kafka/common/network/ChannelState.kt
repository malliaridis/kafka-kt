package org.apache.kafka.common.network

import org.apache.kafka.common.errors.AuthenticationException

/**
 * States for KafkaChannel:
 *
 *  * NOT_CONNECTED: Connections are created in NOT_CONNECTED state. State is updated
 * on [TransportLayer.finishConnect] when socket connection is established.
 * PLAINTEXT channels transition from NOT_CONNECTED to READY, others transition
 * to AUTHENTICATE. Failures in NOT_CONNECTED state typically indicate that the
 * remote endpoint is unavailable, which may be due to misconfigured endpoints.
 *  * AUTHENTICATE: SSL, SASL_SSL and SASL_PLAINTEXT channels are in AUTHENTICATE state during SSL and
 * SASL handshake. Disconnections in AUTHENTICATE state may indicate that authentication failed with
 * SSL or SASL (broker version < 1.0.0). Channels transition to READY state when authentication completes
 * successfully.
 *  * READY: Connected, authenticated channels are in READY state. Channels may transition from
 * READY to EXPIRED, FAILED_SEND or LOCAL_CLOSE.
 *  * EXPIRED: Idle connections are moved to EXPIRED state on idle timeout and the channel is closed.
 *  * FAILED_SEND: Channels transition from READY to FAILED_SEND state if the channel is closed due
 * to a send failure.
 *  * AUTHENTICATION_FAILED: Channels are moved to this state if the requested SASL mechanism is not
 * enabled in the broker or when brokers with versions 1.0.0 and above provide an error response
 * during SASL authentication. [exception] gives the reason provided by the broker for
 * authentication failure.
 *  * LOCAL_CLOSE: Channels are moved to LOCAL_CLOSE state if close() is initiated locally.
 *
 * If the remote endpoint closes a channel, the state of the channel reflects the state the channel
 * was in at the time of disconnection. This state may be useful to identify the reason for disconnection.
 *
 *
 * Typical transitions:
 *
 *  * PLAINTEXT Good path: NOT_CONNECTED => READY => LOCAL_CLOSE
 *  * SASL/SSL Good path: NOT_CONNECTED => AUTHENTICATE => READY => LOCAL_CLOSE
 *  * Bootstrap server misconfiguration: NOT_CONNECTED, disconnected in NOT_CONNECTED state
 *  * Security misconfiguration: NOT_CONNECTED => AUTHENTICATE => AUTHENTICATION_FAILED, disconnected in
 *    AUTHENTICATION_FAILED state
 *  * Security misconfiguration with older broker: NOT_CONNECTED => AUTHENTICATE, disconnected in AUTHENTICATE state
 *
 */
class ChannelState(
    private val state: State?,
    private val remoteAddress: String? = null,
    private val exception: AuthenticationException? = null
) {
    enum class State {
        NOT_CONNECTED,
        AUTHENTICATE,
        READY,
        EXPIRED,
        FAILED_SEND,
        AUTHENTICATION_FAILED,
        LOCAL_CLOSE
    }

    fun state(): State? {
        return state
    }

    fun exception(): AuthenticationException? {
        return exception
    }

    fun remoteAddress(): String? {
        return remoteAddress
    }

    companion object {
        // AUTHENTICATION_FAILED has a custom exception. For other states,
        // create a reusable `ChannelState` instance per-state.
        val NOT_CONNECTED = ChannelState(State.NOT_CONNECTED)
        val AUTHENTICATE = ChannelState(State.AUTHENTICATE)
        val READY = ChannelState(State.READY)
        val EXPIRED = ChannelState(State.EXPIRED)
        val FAILED_SEND = ChannelState(State.FAILED_SEND)
        val LOCAL_CLOSE = ChannelState(State.LOCAL_CLOSE)
    }
}
