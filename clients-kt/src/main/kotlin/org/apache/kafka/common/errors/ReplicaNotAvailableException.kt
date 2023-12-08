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
 * The replica is not available for the requested topic partition. This may be
 * a transient exception during reassignments. From version 2.6 onwards, Fetch requests
 * and other requests intended only for the leader or follower of the topic partition return
 * [NotLeaderOrFollowerException] if the broker is a not a replica of the partition.
 */
class ReplicaNotAvailableException : InvalidMetadataException {

    constructor(message: String?) : super(message)

    constructor(cause: Throwable?) : super(cause)

    constructor(message : String?, cause: Throwable?) : super(message, cause)

    companion object {
        private const val serialVersionUID = 1L
    }
}
