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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.KafkaFuture

/**
 * The result of the [Admin.updateFeatures] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 *
 * @property futures A map from feature name to future, which can be used to check the status of
 * individual feature updates.
 */
class UpdateFeaturesResult internal constructor(val futures: Map<String, KafkaFuture<Unit>>) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("futures")
    )
    fun values(): Map<String, KafkaFuture<Unit>> {
        return futures
    }

    /**
     * Return a future which succeeds if all the feature updates succeed.
     */
    fun all(): KafkaFuture<Unit> {
        return KafkaFuture.allOf(*futures.values.toTypedArray())
    }
}
