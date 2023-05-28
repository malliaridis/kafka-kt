package org.apache.kafka.common.network

import org.apache.kafka.common.Reconfigurable

/**
 * Interface for reconfigurable entities associated with a listener.
 */
interface ListenerReconfigurable : Reconfigurable {
    /**
     * Returns the listener name associated with this reconfigurable. Listener-specific
     * configs corresponding to this listener name are provided for reconfiguration.
     */
    fun listenerName(): ListenerName?
}
