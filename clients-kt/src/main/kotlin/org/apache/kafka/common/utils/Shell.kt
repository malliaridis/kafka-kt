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

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.atomic.AtomicBoolean
import org.slf4j.LoggerFactory

/**
 * A base class for running a Unix command.
 *
 * `Shell` can be used to run unix commands like `du` or `df`.
 *
 * @property timeout Specifies the time in milliseconds, after which the command will be killed. -1
 * means no timeout.
 */
abstract class Shell(private val timeout: Long) {

    /**
     * Return an array containing the command name and its parameters.
     */
    protected abstract fun execString(): Array<String>

    /**
     * Parse the execution result.
     */
    @Throws(IOException::class)
    protected abstract fun parseExecResult(lines: BufferedReader)

    var exitCode = 0
        private set

    /**
     * Sub process used to execute the command.
     */
    var process: Process? = null
        private set

    /**
     * Whether script finished executing.
     */
    @Volatile
    private lateinit var completed: AtomicBoolean

    /**
     * Get the exit code.
     *
     * @return the exit code of the process
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("exitCode")
    )
    fun exitCode(): Int = exitCode

    /**
     * Get the current sub-process executing the given command.
     *
     * @return process executing the command
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("process")
    )
    fun process(): Process? = process

    @Throws(IOException::class)
    protected fun run() {
        exitCode = 0 // reset for next run
        runCommand()
    }

    /**
     * Run a command.
     */
    @Throws(IOException::class)
    private fun runCommand() {
        val builder = ProcessBuilder(*execString())
        var timeoutTimer: Timer? = null
        completed = AtomicBoolean(false)

        val process = builder.start().also { this.process = it }

        if (timeout > -1) {
            timeoutTimer = Timer()
            //One time scheduling.
            timeoutTimer.schedule(ShellTimeoutTimerTask(this), timeout)
        }

        val errReader = BufferedReader(
            InputStreamReader(process.errorStream, StandardCharsets.UTF_8)
        )

        val inReader = BufferedReader(
            InputStreamReader(process.inputStream, StandardCharsets.UTF_8)
        )

        val errMsg = StringBuffer()

        // read error and input streams as this would free up the buffers
        // free the error stream buffer
        val errThread: Thread = KafkaThread.nonDaemon("kafka-shell-thread") {
            try {
                var line = errReader.readLine()
                while (line != null && !Thread.currentThread().isInterrupted) {
                    errMsg.append(line)
                    errMsg.append(System.getProperty("line.separator"))
                    line = errReader.readLine()
                }
            } catch (ioe: IOException) {
                log.warn("Error reading the error stream", ioe)
            }
        }

        errThread.start()

        try {
            parseExecResult(inReader) // parse the output
            // wait for the process to finish and check the exit code
            exitCode = process.waitFor()
            try {
                // make sure that the error thread exits
                errThread.join()
            } catch (ie: InterruptedException) {
                log.warn("Interrupted while reading the error stream", ie)
            }
            completed.set(true)
            //the timeout thread handling
            //taken care in finally block
            if (exitCode != 0) {
                throw ExitCodeException(exitCode, errMsg.toString())
            }
        } catch (ie: InterruptedException) {
            throw IOException(ie.toString())
        } finally {
            timeoutTimer?.cancel()

            // close the input stream
            try {
                inReader.close()
            } catch (ioe: IOException) {
                log.warn("Error while closing the input stream", ioe)
            }

            if (!completed.get()) errThread.interrupt()
            try {
                errReader.close()
            } catch (ioe: IOException) {
                log.warn("Error while closing the error stream", ioe)
            }
            process.destroy()
        }
    }

    /**
     * This is an IOException with exit code added.
     */
    class ExitCodeException(var exitCode: Int, message: String?) : IOException(message)

    /**
     * A simple shell command executor.
     *
     * `ShellCommandExecutor`should be used in cases where the output of the command needs no
     * explicit parsing and where the command, working directory and the environment remains
     * unchanged. The output of the command is stored as-is and is expected to be small.
     *
     * @constructor Create a new instance of the ShellCommandExecutor to execute a command.
     * @param execString The command to execute with arguments
     * @param timeout Specifies the time in milliseconds, after which the command will be killed.
     * -1 means no timeout.
     */
    class ShellCommandExecutor(
        execString: Array<String>,
        timeout: Long,
    ) : Shell(timeout) {

        private val command: Array<String> = execString.clone()

        var output: StringBuffer? = null
            private set

        /**
         * Execute the shell command.
         */
        @Throws(IOException::class)
        fun execute() {
            this.run()
        }

        override fun execString(): Array<String> = command

        @Throws(IOException::class)
        override fun parseExecResult(lines: BufferedReader) {
            val output = StringBuffer().also { output = it }
            val buf = CharArray(512)
            var nRead: Int
            while (lines.read(buf, 0, buf.size).also { nRead = it } > 0) {
                output.append(buf, 0, nRead)
            }
        }

        /**
         * Get the output of the shell command.
         */
        fun output(): String = output?.toString() ?: ""

        /**
         * Returns the commands of this instance. Arguments with spaces in are presented with quotes
         * round; other arguments are presented raw.
         *
         * @return a string representation of the object.
         */
        override fun toString(): String {
            val builder = StringBuilder()
            val args = execString()
            for (s in args) {
                if (s.indexOf(' ') >= 0) builder.append('"').append(s).append('"')
                else builder.append(s)
                builder.append(' ')
            }
            return builder.toString()
        }
    }

    /**
     * Timer which is used to timeout scripts spawned off by shell.
     */
    private class ShellTimeoutTimerTask(private val shell: Shell) : TimerTask() {
        override fun run() {
            val p = shell.process
            try {
                p!!.exitValue()
            } catch (e: Exception) {
                //Process has not terminated, so check if it has completed, if not just destroy it.
                if (p != null && !shell.completed.get()) p.destroy()
            }
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(Shell::class.java)

        /**
         * Static method to execute a shell command. Covers most of the simple cases without
         * requiring the user to implement the `Shell` interface.
         *
         * @param cmd shell command to execute.
         * @return the output of the executed command.
         */
        @Throws(IOException::class)
        fun execCommand(vararg cmd: String): String = execCommand(cmd = arrayOf(*cmd), timeout = -1)

        /**
         * Static method to execute a shell command. Covers most of the simple cases without
         * requiring the user to implement the `Shell` interface.
         *
         * @param cmd shell command to execute.
         * @param timeout time in milliseconds after which script should be killed. -1 means no
         * timeout.
         * @return the output of the executed command.
         */
        @Throws(IOException::class)
        fun execCommand(cmd: Array<String>, timeout: Long): String {
            val exec = ShellCommandExecutor(cmd, timeout)
            exec.execute()
            return exec.output()
        }
    }
}
