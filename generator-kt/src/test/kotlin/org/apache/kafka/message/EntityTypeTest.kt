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

package org.apache.kafka.message

import org.junit.Assert.assertThrows
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import java.util.concurrent.TimeUnit

class EntityTypeTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)
    
    @Test
    fun testUnknownEntityType() {
        for (type in arrayOf(
            FieldType.StringFieldType.INSTANCE,
            FieldType.Int8FieldType.INSTANCE,
            FieldType.Int16FieldType.INSTANCE,
            FieldType.Int32FieldType.INSTANCE,
            FieldType.Int64FieldType.INSTANCE,
            FieldType.ArrayType(FieldType.StringFieldType.INSTANCE),
        )) EntityType.UNKNOWN.verifyTypeMatches("unknown", type)
    }

    @Test
    fun testVerifyTypeMatches() {
        EntityType.TRANSACTIONAL_ID.verifyTypeMatches(
            fieldName = "transactionalIdField",
            type = FieldType.StringFieldType.INSTANCE,
        )
        EntityType.TRANSACTIONAL_ID.verifyTypeMatches(
            fieldName = "transactionalIdField",
            type = FieldType.ArrayType(FieldType.StringFieldType.INSTANCE),
        )
        EntityType.PRODUCER_ID.verifyTypeMatches(
            fieldName = "producerIdField",
            type = FieldType.Int64FieldType.INSTANCE,
        )
        EntityType.PRODUCER_ID.verifyTypeMatches(
            fieldName = "producerIdField",
            type = FieldType.ArrayType(FieldType.Int64FieldType.INSTANCE),
        )
        EntityType.GROUP_ID.verifyTypeMatches(
            fieldName = "groupIdField",
            type = FieldType.StringFieldType.INSTANCE,
        )
        EntityType.GROUP_ID.verifyTypeMatches(
            fieldName = "groupIdField",
            type = FieldType.ArrayType(FieldType.StringFieldType.INSTANCE),
        )
        EntityType.TOPIC_NAME.verifyTypeMatches(
            fieldName = "topicNameField",
            type = FieldType.StringFieldType.INSTANCE,
        )
        EntityType.TOPIC_NAME.verifyTypeMatches(
            fieldName = "topicNameField",
            type = FieldType.ArrayType(FieldType.StringFieldType.INSTANCE),
        )
        EntityType.BROKER_ID.verifyTypeMatches(
            fieldName = "brokerIdField",
            type = FieldType.Int32FieldType.INSTANCE,
        )
        EntityType.BROKER_ID.verifyTypeMatches(
            fieldName = "brokerIdField",
            type = FieldType.ArrayType(FieldType.Int32FieldType.INSTANCE),
        )
    }

    @Test
    fun testVerifyTypeMismatches() {
        expectException {
            EntityType.TRANSACTIONAL_ID.verifyTypeMatches(
                fieldName = "transactionalIdField",
                type = FieldType.Int32FieldType.INSTANCE,
            )
        }
        expectException {
            EntityType.PRODUCER_ID.verifyTypeMatches(
                fieldName = "producerIdField",
                type = FieldType.StringFieldType.INSTANCE,
            )
        }
        expectException {
            EntityType.GROUP_ID.verifyTypeMatches(
                fieldName = "groupIdField",
                type = FieldType.Int8FieldType.INSTANCE,
            )
        }
        expectException {
            EntityType.TOPIC_NAME.verifyTypeMatches(
                fieldName = "topicNameField",
                type = FieldType.ArrayType(FieldType.Int64FieldType.INSTANCE),
            )
        }
        expectException {
            EntityType.BROKER_ID.verifyTypeMatches(
                fieldName = "brokerIdField",
                type = FieldType.Int64FieldType.INSTANCE,
            )
        }
    }

    companion object {
        private fun expectException(r: Runnable) {
            assertThrows(RuntimeException::class.java){ r.run() }
        }
    }
}
