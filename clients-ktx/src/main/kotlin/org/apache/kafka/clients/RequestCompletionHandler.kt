package org.apache.kafka.clients

/**
 * A callback interface for attaching an action to be executed when a request is complete and the corresponding response
 * has been received. This handler will also be invoked if there is a disconnection while handling the request.
 */
interface RequestCompletionHandler {
    fun onComplete(response: ClientResponse?)
}
