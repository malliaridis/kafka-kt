package org.apache.kafka.clients

import java.net.InetAddress

internal class AddressChangeHostResolver(
    private val initialAddresses: Array<InetAddress>,
    private val newAddresses: Array<InetAddress>
) : HostResolver {

    private var useNewAddresses = false
    private var resolutionCount = 0

    override fun resolve(host: String?): Array<InetAddress> {
        ++resolutionCount
        return if (useNewAddresses) newAddresses else initialAddresses
    }

    fun changeAddresses() {
        useNewAddresses = true
    }

    fun useNewAddresses(): Boolean {
        return useNewAddresses
    }

    fun resolutionCount(): Int {
        return resolutionCount
    }
}
