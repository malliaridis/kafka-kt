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

import org.apache.kafka.common.utils.Exit.Procedure
import org.apache.kafka.common.utils.Exit.ShutdownHookAdder
import kotlin.system.exitProcess

/**
 * Internal class that should be used instead of `System.exit()` and `Runtime.getRuntime().halt()`
 * so that tests can easily change the behaviour.
 */
object Exit {

    private val DEFAULT_HALT_PROCEDURE = Procedure { statusCode, _ -> Runtime.getRuntime().halt(statusCode) }

    private val DEFAULT_EXIT_PROCEDURE = Procedure { statusCode, _ -> exitProcess(statusCode) }

    private val DEFAULT_SHUTDOWN_HOOK_ADDER = ShutdownHookAdder { name, runnable ->
            if (name != null) Runtime.getRuntime().addShutdownHook(KafkaThread.nonDaemon(name, runnable))
            else Runtime.getRuntime().addShutdownHook(Thread(runnable))
        }

    @Volatile
    private var exitProcedure = DEFAULT_EXIT_PROCEDURE

    @Volatile
    private var haltProcedure = DEFAULT_HALT_PROCEDURE

    @Volatile
    private var shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER

    fun exit(statusCode: Int, message: String? = null) = exitProcedure.execute(statusCode, message)

    fun halt(statusCode: Int, message: String? = null) =
        haltProcedure.execute(statusCode, message)

    fun addShutdownHook(name: String?, runnable: Runnable?) =
        shutdownHookAdder.addShutdownHook(name, runnable)

    fun setExitProcedure(procedure: Procedure) {
        exitProcedure = procedure
    }

    fun setHaltProcedure(procedure: Procedure) {
        haltProcedure = procedure
    }

    fun setShutdownHookAdder(shutdownHookAdder: ShutdownHookAdder) {
        Exit.shutdownHookAdder = shutdownHookAdder
    }

    fun resetExitProcedure() {
        exitProcedure = DEFAULT_EXIT_PROCEDURE
    }

    fun resetHaltProcedure() {
        haltProcedure = DEFAULT_HALT_PROCEDURE
    }

    fun resetShutdownHookAdder() {
        shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER
    }

    fun interface Procedure {
        fun execute(statusCode: Int, message: String?)
    }

    fun interface ShutdownHookAdder {
        fun addShutdownHook(name: String?, runnable: Runnable?)
    }
}
