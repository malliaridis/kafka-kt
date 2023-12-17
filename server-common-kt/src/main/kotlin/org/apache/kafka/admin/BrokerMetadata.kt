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

package org.apache.kafka.admin

import java.util.Objects
import java.util.Optional

/**
 * Broker metadata used by admin tools.
 *
 * @property id an integer that uniquely identifies this broker
 * @property rack the rack of the broker, which is used to in rack aware partition assignment for fault tolerance.
 * Examples: "RACK1", "us-east-1d"
 */
data class BrokerMetadata(
    val id: Int,
    val rack: String?,
)
