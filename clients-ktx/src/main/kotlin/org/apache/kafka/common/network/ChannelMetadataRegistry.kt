package org.apache.kafka.common.network

import java.io.Closeable

/**
 * Metadata about a channel is provided in various places in the network stack. This
 * registry is used as a common place to collect them.
 */
interface ChannelMetadataRegistry : Closeable {

    /**
     * Register information about the SSL cipher we are using.
     * Re-registering the information will overwrite the previous one.
     */
    fun registerCipherInformation(cipherInformation: CipherInformation?)

    /**
     * Get the currently registered cipher information.
     */
    fun cipherInformation(): CipherInformation?

    /**
     * Register information about the client client we are using.
     * Depending on the clients, the ApiVersionsRequest could be received
     * multiple times or not at all. Re-registering the information will
     * overwrite the previous one.
     */
    fun registerClientInformation(clientInformation: ClientInformation?)

    /**
     * Get the currently registered client information.
     */
    fun clientInformation(): ClientInformation?

    /**
     * Unregister everything that has been registered and close the registry.
     */
    override fun close()
}
