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

package org.apache.kafka.common

import org.apache.kafka.common.utils.Serializer
import java.io.IOException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * This test ensures TopicPartition class is serializable and is serialization compatible.
 * Note: this ensures that the current code can deserialize data serialized with older versions of the code,
 * but not the reverse. That is, older code won't necessarily be able to deserialize data serialized with newer code.
 */
class TopicPartitionTest {

    private val topicName = "mytopic"

    private val fileName = "serializedData/topicPartitionSerializedfile"

    private val partNum = 5

    private fun checkValues(deSerTP: TopicPartition) {
        //assert deserialized values are same as original
        assertEquals(
            expected = partNum,
            actual = deSerTP.partition,
            message = "partition number should be $partNum but got ${deSerTP.partition}"
        )
        assertEquals(
            expected = topicName,
            actual = deSerTP.topic,
            message = "topic should be $topicName but got ${deSerTP.topic}"
        )
    }

    @Test
    @Throws(IOException::class, ClassNotFoundException::class)
    fun testSerializationRoundtrip() {
        //assert TopicPartition is serializable and deserialization renders the clone of original properly
        val origTp = TopicPartition(topicName, partNum)
        val byteArray = Serializer.serialize(origTp)

        //deserialize the byteArray and check if the values are same as original
        val deserializedObject = Serializer.deserialize(byteArray)
        assertIs<TopicPartition>(deserializedObject)
        checkValues(deserializedObject)
    }

    @Test
    @Throws(IOException::class, ClassNotFoundException::class)
    fun testTopiPartitionSerializationCompatibility() {
        // assert serialized TopicPartition object in file (serializedData/topicPartitionSerializedfile) is
        // deserializable into TopicPartition and is compatible
        val deserializedObject = Serializer.deserialize(fileName)
        assertIs<TopicPartition>(deserializedObject)
        checkValues(deserializedObject)
    }
}
