package org.apache.kafka.clients

import java.net.InetAddress
import java.net.UnknownHostException
import java.util.*

interface HostResolver {

    @Throws(UnknownHostException::class)
    fun resolve(host: String?): Array<InetAddress>
}
