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
 * This fatal exception indicates that another producer with the same `transactional.id` has been
 * started. It is only possible to have one producer instance with a `transactional.id` at any
 * given time, and the latest one to be started "fences" the previous instances so that they can no longer
 * make transactional requests. When you encounter this exception, you must close the producer instance.
 */
class ProducerFencedException(message: String? = null) : ApiException(message = message)
