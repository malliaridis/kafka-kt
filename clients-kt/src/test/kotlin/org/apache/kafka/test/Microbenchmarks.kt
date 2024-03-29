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

package org.apache.kafka.test

import org.apache.kafka.common.utils.CopyOnWriteMap
import org.apache.kafka.common.utils.Time
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.sqrt
import kotlin.random.Random

object Microbenchmarks {

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val iters = args[0].toInt()
        var x = 0.0
        var start = System.nanoTime()
        for (i in 0..<iters) x += sqrt(x)
        println(x)
        println("sqrt: " + (System.nanoTime() - start) / iters.toDouble())

        // test clocks
        systemMillis(iters)
        systemNanos(iters)
        var total: Long = 0
        start = System.nanoTime()
        total += systemMillis(iters)
        println("System.currentTimeMillis(): " + (System.nanoTime() - start) / iters)
        start = System.nanoTime()
        total += systemNanos(iters)
        println("System.nanoTime(): " + (System.nanoTime() - start) / iters)
        println(total)

        // test random
        var n = 0
        start = System.nanoTime()
        for (i in 0..<iters) n += Random.nextInt()
        println(n)
        println("random: " + (System.nanoTime() - start) / iters)
        val floats = FloatArray(1024)
        for (i in floats.indices) floats[i] = Random.nextFloat()
        floats.sort()
        var loc = 0
        start = System.nanoTime()

        for (i in 0..<iters) loc += floats.binarySearch(floats[i % floats.size])
        println(loc)
        println("binary search: " + (System.nanoTime() - start) / iters)
        val time = Time.SYSTEM
        val done = AtomicBoolean(false)
        val lock = Any()
        val t1 = object : Thread() {
            override fun run() {
                time.sleep(1)
                var counter = 0
                val start = time.nanoseconds()
                for (i in 0..<iters) synchronized(lock) { counter++ }
                println("synchronized: " + (time.nanoseconds() - start) / iters)
                println(counter)
                done.set(true)
            }
        }
        val t2 = object : Thread() {
            override fun run() {
                var counter = 0
                while (!done.get()) {
                    time.sleep(1)
                    synchronized(lock) { counter += 1 }
                }
                println("Counter: $counter")
            }
        }
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        println("Testing locks")
        done.set(false)
        val lock2 = ReentrantLock()
        val t3 = object : Thread() {
            override fun run() {
                time.sleep(1)
                var counter = 0
                val start = time.nanoseconds()
                for (i in 0..<iters) {
                    lock2.lock()
                    counter++
                    lock2.unlock()
                }
                println("lock: " + (time.nanoseconds() - start) / iters)
                println(counter)
                done.set(true)
            }
        }
        val t4: Thread = object : Thread() {
            override fun run() {
                var counter = 0
                while (!done.get()) {
                    time.sleep(1)
                    lock2.lock()
                    counter++
                    lock2.unlock()
                }
                println("Counter: $counter")
            }
        }
        t3.start()
        t4.start()
        t3.join()
        t4.join()

        val values = mutableMapOf<String, Int>()
        for (i in 0..99) values[i.toString()] = i
        println("HashMap:")
        benchMap(2, 1000000, values)
        println("ConcurrentHashMap:")
        benchMap(2, 1000000, ConcurrentHashMap(values))
        println("CopyOnWriteMap:")
        benchMap(2, 1000000, CopyOnWriteMap(values))
    }

    @Throws(Exception::class)
    private fun benchMap(numThreads: Int, iters: Int, map: Map<String, Int>) {
        val keys: List<String> = ArrayList(map.keys)
        val threads: MutableList<Thread> = ArrayList()
        for (i in 0..<numThreads) threads.add(object : Thread() {
            override fun run() {
                val start = System.nanoTime()
                for (j in 0..<iters) map[keys[j % threads.size]]
                println("Map access time: " + (System.nanoTime() - start) / iters.toDouble())
            }
        })
        for (thread in threads) thread.start()
        for (thread in threads) thread.join()
    }

    private fun systemMillis(iters: Int): Long {
        var total: Long = 0
        for (i in 0..<iters) total += System.currentTimeMillis()
        return total
    }

    private fun systemNanos(iters: Int): Long {
        var total: Long = 0
        for (i in 0..<iters) total += System.currentTimeMillis()
        return total
    }
}
