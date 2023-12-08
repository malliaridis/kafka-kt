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

package org.apache.kafka.common.utils

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

object Serializer {

    @Throws(IOException::class)
    fun serialize(toSerialize: Any?): ByteArray {
        val arrayOutputStream = ByteArrayOutputStream()
        ObjectOutputStream(arrayOutputStream).use { ooStream ->
            ooStream.writeObject(toSerialize)
            return arrayOutputStream.toByteArray()
        }
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    fun deserialize(inputStream: InputStream?): Any {
        ObjectInputStream(inputStream).use { objectInputStream -> return objectInputStream.readObject() }
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    fun deserialize(byteArray: ByteArray?): Any {
        val arrayInputStream = ByteArrayInputStream(byteArray)
        return deserialize(arrayInputStream)
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    fun deserialize(fileName: String?): Any {
        val classLoader = Serializer::class.java.getClassLoader()
        val fileStream = classLoader.getResourceAsStream(fileName)
        return deserialize(fileStream)
    }
}
