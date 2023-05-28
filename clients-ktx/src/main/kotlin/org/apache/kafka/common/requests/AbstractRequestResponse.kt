package org.apache.kafka.common.requests

import org.apache.kafka.common.protocol.ApiMessage

interface AbstractRequestResponse {
    fun data(): ApiMessage
}
