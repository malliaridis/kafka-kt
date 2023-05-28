package org.apache.kafka.clients

import java.net.InetAddress
import java.net.UnknownHostException

class DefaultHostResolver : HostResolver {
    @Throws(UnknownHostException::class)
    override fun resolve(host: String?): Array<InetAddress> {

        return InetAddress.getAllByName(host)
    }
}
