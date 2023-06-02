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

import java.util.Properties

/**
 * The base class for in-built admin clients.
 *
 * Client code should use the newer [Admin] interface in preference to this class.
 *
 * This class may be removed in a later release, but has not be marked as deprecated to avoid
 * unnecessary noise.
 */
abstract class AdminClient : Admin {

    companion object {

        /**
         * Create a new Admin with the given configuration.
         *
         * @param props The configuration.
         * @return The new KafkaAdminClient.
         */
        fun create(props: Properties): AdminClient {
            return Admin.create(props) as AdminClient
        }

        /**
         * Create a new Admin with the given configuration.
         *
         * @param conf The configuration.
         * @return The new KafkaAdminClient.
         */
        fun create(conf: Map<String, Any?>): AdminClient {
            return Admin.create(conf) as AdminClient
        }
    }
}
