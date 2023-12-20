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

package org.apache.kafka.log4jappender

import java.util.Properties
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.test.MockSerializer
import org.apache.log4j.spi.LoggingEvent

class MockKafkaLog4jAppender : KafkaLog4jAppender() {

    private var mockProducer = MockProducer(
        autoComplete = false,
        keySerializer = MockSerializer(),
        valueSerializer = MockSerializer(),
    )

    var producerProperties: Properties? = null
        private set

    override fun getKafkaProducer(props: Properties): Producer<ByteArray?, ByteArray?> {
        producerProperties = props
        return mockProducer
    }

    fun setKafkaProducer(producer: MockProducer<ByteArray?, ByteArray?>) {
        mockProducer = producer
    }

    override fun append(event: LoggingEvent) {
        if (super.producer == null) activateOptions()
        super.append(event)
    }

    val history: List<ProducerRecord<ByteArray?, ByteArray?>>
        get() = mockProducer.history()
}
