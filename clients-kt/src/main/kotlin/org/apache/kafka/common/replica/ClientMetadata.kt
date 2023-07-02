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

package org.apache.kafka.common.replica

import java.net.InetAddress
import org.apache.kafka.common.security.auth.KafkaPrincipal

/**
 * Holder for all the client metadata required to determine a preferred replica.
 */
interface ClientMetadata {

    /**
     * Rack ID sent by the client
     */
    fun rackId(): String

    /**
     * Client ID sent by the client
     */
    fun clientId(): String

    /**
     * Incoming address of the client
     */
    fun clientAddress(): InetAddress

    /**
     * Security principal of the client
     */
    fun principal(): KafkaPrincipal

    /**
     * Listener name for the client
     */
    fun listenerName(): String

    data class DefaultClientMetadata(
        private val rackId: String,
        private val clientId: String,
        private val clientAddress: InetAddress,
        private val principal: KafkaPrincipal,
        private val listenerName: String
    ) : ClientMetadata {

        override fun rackId(): String = rackId

        override fun clientId(): String = clientId

        override fun clientAddress(): InetAddress = clientAddress

        override fun principal(): KafkaPrincipal = principal

        override fun listenerName(): String = listenerName

        override fun toString(): String {
            return "DefaultClientMetadata{" +
                    "rackId='$rackId'" +
                    ", clientId='$clientId'" +
                    ", clientAddress=$clientAddress" +
                    ", principal=$principal" +
                    ", listenerName='$listenerName'" +
                    '}'
        }
    }
}
