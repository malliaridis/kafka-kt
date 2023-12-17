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

package org.apache.kafka.server.util

import java.io.IOException
import java.util.Optional
import java.util.Properties
import java.util.function.Consumer
import joptsimple.OptionParser
import joptsimple.OptionSet
import joptsimple.OptionSpec
import org.apache.kafka.common.utils.AppInfoParser.version
import org.apache.kafka.common.utils.Exit.exit

/**
 * Helper functions for dealing with command line utilities.
 */
object CommandLineUtils {

    /**
     * Check if there are no options or `--help` option from command line.
     *
     * @param commandOpts Acceptable options for a command
     * @return true on matching the help check condition
     */
    fun isPrintHelpNeeded(commandOpts: CommandDefaultOptions): Boolean {
        return commandOpts.args.isEmpty() || commandOpts.options!!.has(commandOpts.helpOpt)
    }

    /**
     * Check if there is `--version` option from command line.
     *
     * @param commandOpts Acceptable options for a command
     * @return true on matching the help check condition
     */
    fun isPrintVersionNeeded(commandOpts: CommandDefaultOptions): Boolean {
        return commandOpts.options!!.has(commandOpts.versionOpt)
    }

    /**
     * Check and print help message if there is no options or `--help` option
     * from command line, if `--version` is specified on the command line
     * print version information and exit.
     *
     * @param commandOpts Acceptable options for a command
     * @param message     Message to display on successful check
     */
    fun maybePrintHelpOrVersion(commandOpts: CommandDefaultOptions, message: String?) {
        if (isPrintHelpNeeded(commandOpts)) printUsageAndExit(commandOpts.parser, message)
        if (isPrintVersionNeeded(commandOpts)) printVersionAndExit()
    }

    /**
     * Check that all the listed options are present.
     */
    fun checkRequiredArgs(parser: OptionParser, options: OptionSet, vararg requiredList: OptionSpec<*>?) {
        for (arg in requiredList) {
            if (!options.has(arg)) printUsageAndExit(parser, """Missing required argument "$arg"""")
        }
    }

    /**
     * Check that none of the listed options are present.
     */
    @Deprecated(
        message = "Use checkInvalidArgs with invalidOptions as Set instead.",
        replaceWith = ReplaceWith("checkInvalidArgs(parser, options, usedOption, invalidOptions.toSet())")
    )
    fun checkInvalidArgs(
        parser: OptionParser,
        options: OptionSet,
        usedOption: OptionSpec<*>,
        vararg invalidOptions: OptionSpec<*>,
    ) {
        if (options.has(usedOption)) {
            for (arg in invalidOptions) {
                if (options.has(arg)) printUsageAndExit(
                    parser = parser,
                    message = """Option "$usedOption" can't be used with option "$arg"""",
                )
            }
        }
    }

    /**
     * Check that none of the listed options are present.
     */
    fun checkInvalidArgs(
        parser: OptionParser,
        options: OptionSet,
        usedOption: OptionSpec<*>,
        invalidOptions: Set<OptionSpec<*>>,
    ) {
        if (options.has(usedOption)) {
            for (arg in invalidOptions) {
                if (options.has(arg)) printUsageAndExit(
                    parser = parser,
                    message = """Option "$usedOption" can't be used with option "$arg"""",
                )
            }
        }
    }

    /**
     * Check that none of the listed options are present with the combination of used options.
     */
    fun checkInvalidArgsSet(
        parser: OptionParser,
        options: OptionSet,
        usedOptions: Set<OptionSpec<*>>,
        invalidOptions: Set<OptionSpec<*>>,
        trailingAdditionalMessage: String?,
    ) {
        if (usedOptions.count { option -> options.has(option) } == usedOptions.size) {
            for (arg in invalidOptions) {
                if (options.has(arg)) printUsageAndExit(
                    parser = parser,
                    message = """Option combination "$usedOptions" can't be used with option "$arg"${trailingAdditionalMessage ?: ""}"""
                )
            }
        }
    }

    fun printUsageAndExit(parser: OptionParser, message: String?) {
        System.err.println(message)
        try {
            parser.printHelpOn(System.err)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
        exit(1, message)
    }

    fun printVersionAndExit() {
        println(version)
        exit(0)
    }

    /**
     * Parse key-value pairs in the form key=value.
     * Value may contain equals sign.
     */
    fun parseKeyValueArgs(args: List<String>, acceptMissingValue: Boolean = true): Properties {
        val props = Properties()
        val splits = mutableListOf<List<String?>>()

        args.forEach { arg ->
            val split = arg.split("=".toRegex(), limit = 2)
            if (split.isNotEmpty()) splits.add(split)
        }

        splits.forEach { split ->
            if (
                split.size == 1
                || split.size == 2
                && (split[1] == null || split[1]!!.isEmpty())
            ) {
                require(acceptMissingValue) { "Missing value for key ${split[0]}}" }
                props[split[0]] = ""
            } else props[split[0]] = split[1]
        }
        return props
    }

    /**
     * Merge the options into `props` for key `key`, with the following precedence, from high to low:
     * 1) if `spec` is specified on `options` explicitly, use the value;
     * 2) if `props` already has `key` set, keep it;
     * 3) otherwise, use the default value of `spec`.
     * A `null` value means to remove `key` from the `props`.
     */
    fun <T> maybeMergeOptions(props: Properties, key: String?, options: OptionSet, spec: OptionSpec<T>?) {
        if (options.has(spec) || !props.containsKey(key)) {
            val value = options.valueOf(spec)

            if (value == null) props.remove(key)
            else props[key] = value.toString()
        }
    }
}
