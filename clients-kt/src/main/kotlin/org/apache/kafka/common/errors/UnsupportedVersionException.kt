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

package org.apache.kafka.common.errors

/**
 * Indicates that a request API or version needed by the client is not supported by the broker. This is
 * typically a fatal error as Kafka clients will downgrade request versions as needed except in cases where
 * a needed feature is not available in old versions. Fatal errors can generally only be handled by closing
 * the client instance, although in some cases it may be possible to continue without relying on the
 * underlying feature. For example, when the producer is used with idempotence enabled, this error is fatal
 * since the producer does not support reverting to weaker semantics. On the other hand, if this error
 * is raised from [org.apache.kafka.clients.consumer.KafkaConsumer.offsetsForTimes], it would
 * be possible to revert to alternative logic to set the consumer's position.
 */
open class UnsupportedVersionException : ApiException {

    constructor(message: String?) : super(message)

    constructor(message : String?, cause: Throwable?) : super(message, cause)

    companion object {
        private const val serialVersionUID = 1L
    }
}
