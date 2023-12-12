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

package org.apache.kafka.common.config

import java.util.LinkedList
import java.util.function.Function
import java.util.regex.Pattern
import java.util.stream.Collectors
import org.apache.kafka.common.config.ConfigDef.Recommender
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.utils.Utils.contextOrKafkaClassLoader
import org.apache.kafka.common.utils.Utils.join

/**
 * This class is used for specifying the set of expected configurations. For each configuration, you
 * can specify the name, the type, the default value, the documentation, the group information, the
 * order in the group, the width of the configuration value and the name suitable for display in the
 * UI.
 *
 * You can provide special validation logic used for single configuration validation by overriding
 * [Validator].
 *
 * Moreover, you can specify the dependents of a configuration. The valid values and visibility of a
 * configuration may change according to the values of other configurations. You can override
 * [Recommender] to get valid values and set visibility of a configuration given the current
 * configuration values.
 *
 * To use the class:
 *
 * ```java
 * ConfigDef defs = new ConfigDef();
 *
 * defs.define(
 *     "config_with_default",
 *     Type.STRING,
 *     "default string value",
 *     Importance.High,
 *     "Configuration with default value."
 * );
 * defs.define(
 *     "config_with_validator",
 *     Type.INT,
 *     42,
 *     Range.atLeast(0),
 *     Importance.High,
 *     "Configuration with user provided validator."
 * );
 * defs.define(
 *     "config_with_dependents",
 *     Type.INT,
 *     Importance.LOW,
 *     "Configuration with dependents.",
 *     "group",
 *     1,
 *     "Config With Dependents",
 *     Arrays.asList("config_with_default","config_with_validator")
 * );
 *
 * Map<String, String> props = new HashMap<>();
 * props.put("config_with_default", "some value");
 * props.put("config_with_dependents", "some other value");
 *
 * Map<String, Object> configs = defs.parse(props);
 * // will return "some value"
 * String someConfig = (String) configs.get("config_with_default");
 * // will return default value of 42
 * int anotherConfig = (Integer) configs.get("config_with_validator");
 *
 * To validate the full configuration, use:
 * List<ConfigValue> configValues = defs.validate(props);
 * The [ConfigValue] contains updated configuration information given the current configuration values.
 * ```
 *
 * This class can be used standalone or in combination with [AbstractConfig] which provides some
 * additional functionality for accessing configs.
 */
class ConfigDef {

    val configKeys: MutableMap<String, ConfigKey>

    val groups: MutableList<String?>

    private var configsWithNoParent: Set<String>? = null

    constructor() {
        configKeys = LinkedHashMap()
        groups = LinkedList()
    }

    constructor(base: ConfigDef) {
        configKeys = base.configKeys.toMutableMap()
        groups = base.groups.toMutableList()
        // It is not safe to copy this from the parent because we may subsequently add to the set of
        // configs and invalidate this
    }

    /**
     * Returns unmodifiable set of properties names defined in this [ConfigDef]
     *
     * @return new unmodifiable [Set] instance containing the keys
     */
    fun names(): Set<String> = configKeys.keys

    fun defaultValues(): Map<String, Any?> {
        val defaultValues = mutableMapOf<String, Any?>()

        configKeys.forEach { (_, key) ->
            if (key.defaultValue !== NO_DEFAULT_VALUE) defaultValues[key.name] = key.defaultValue
        }
        return defaultValues
    }

    fun define(key: ConfigKey): ConfigDef {
        if (configKeys.containsKey(key.name))
            throw ConfigException("Configuration ${key.name} is defined twice.")

        if (key.group != null && !groups.contains(key.group)) groups.add(key.group)
        configKeys[key.name] = key

        return this
    }

    /**
     * Define a new configuration with no dependents and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param validator the validator to use in checking the correctness of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param dependents the configurations that are dependents of this configuration
     * @param recommender the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    fun define(
        name: String,
        type: Type,
        defaultValue: Any? = NO_DEFAULT_VALUE,
        validator: Validator? = null,
        importance: Importance,
        documentation: String?,
        group: String? = null,
        orderInGroup: Int = -1,
        width: Width? = Width.NONE,
        displayName: String? = name,
        dependents: List<String> = emptyList(),
        recommender: Recommender? = null,
    ): ConfigDef {
        return define(
            ConfigKey(
                name = name,
                type = type,
                defaultValue = defaultValue,
                validator = validator,
                importance = importance,
                documentation = documentation,
                group = group,
                orderInGroup = orderInGroup,
                width = width,
                displayName = displayName,
                dependents = dependents,
                recommender = recommender,
                internalConfig = false,
            )
        )
    }

    /**
     * Define a new internal configuration. Internal configuration won't show up in the docs and
     * aren't intended for general use.
     *
     * @param name The name of the config parameter
     * @param type The type of the config
     * @param defaultValue The default value to use if this config isn't present
     * @param importance The importance of this config (i.e. is this something you will likely need
     * to change?)
     * @return This ConfigDef so you can chain calls
     */
    fun defineInternal(
        name: String,
        type: Type,
        defaultValue: Any?,
        importance: Importance,
    ): ConfigDef {
        return define(
            ConfigKey(
                name = name,
                type = type,
                defaultValue = defaultValue,
                validator = null,
                importance = importance,
                documentation = "",
                group = "",
                orderInGroup = -1,
                width = Width.NONE,
                displayName = name,
                dependents = emptyList(),
                recommender = null,
                internalConfig = true
            )
        )
    }

    /**
     * Define a new internal configuration. Internal configuration won't show up in the docs and aren't
     * intended for general use.
     * @param name              The name of the config parameter
     * @param type              The type of the config
     * @param defaultValue      The default value to use if this config isn't present
     * @param validator         The validator to use in checking the correctness of the config
     * @param importance        The importance of this config (i.e. is this something you will likely need to change?)
     * @param documentation     The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    fun defineInternal(
        name: String,
        type: Type,
        defaultValue: Any?,
        validator: Validator?,
        importance: Importance,
        documentation: String?,
    ): ConfigDef {
        return define(
            ConfigKey(
                name = name,
                type = type,
                defaultValue = defaultValue,
                validator = validator,
                importance = importance,
                documentation = documentation,
                group = "",
                orderInGroup = -1,
                width = Width.NONE,
                displayName = name,
                dependents = emptyList(),
                recommender = null,
                internalConfig = true
            )
        )
    }

    /**
     * Get the configuration keys
     * @return a map containing all configuration keys
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("configKeys")
    )
    fun configKeys(): Map<String, ConfigKey> = configKeys

    /**
     * Get the groups for the configuration
     * @return a list of group names
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("groups")
    )
    fun groups(): List<String?> = groups

    /**
     * Add standard SSL client configuration options.
     * @return this
     */
    fun withClientSslSupport(): ConfigDef {
        SslConfigs.addClientSslSupport(this)
        return this
    }

    /**
     * Add standard SASL client configuration options.
     * @return this
     */
    fun withClientSaslSupport(): ConfigDef {
        SaslConfigs.addClientSaslSupport(this)
        return this
    }

    /**
     * Parse and validate configs against this configuration definition. The input is a map of
     * configs. It is expected that the keys of the map are strings, but the values can either be
     * strings or they may already be of the appropriate type (int, string, etc). This will work
     * equally well with either java.util.Properties instances or a programmatically constructed
     * map.
     *
     * @param props The configs to parse and validate.
     * @return Parsed and validated configs. The key will be the config name and the value will be
     * the value parsed into the appropriate type (int, string, etc).
     */
    fun parse(props: Map<*, *>): Map<String, Any?> {
        // Check all configurations are defined
        val undefinedConfigKeys = undefinedDependentConfigs()
        if (undefinedConfigKeys.isNotEmpty()) {
            throw ConfigException(
                "Some configurations in are referred in the dependents, but not defined: " +
                        undefinedConfigKeys.joinToString(",")
            )
        }

        // parse all known keys
        return configKeys.values.associate { key ->
            key.name to parseValue(key, props[key.name], props.containsKey(key.name))
        }
    }

    fun parseValue(key: ConfigKey, value: Any?, isSet: Boolean): Any? {

        val parsedValue = if (isSet) parseType(key.name, value, key.type)
        // props map doesn't contain setting, the key is required because no default value specified - its an error
        else if (NO_DEFAULT_VALUE == key.defaultValue) throw ConfigException(
            "Missing required configuration \"${key.name}\" which has no default value."
        )
        else key.defaultValue // otherwise assign setting its default value

        if (key.validator != null) key.validator.ensureValid(key.name, parsedValue)

        return parsedValue
    }

    /**
     * Validate the current configuration values with the configuration definition.
     *
     * @param props the current configuration values
     * @return List of Config, each Config contains the updated configuration information given
     * the current configuration values.
     */
    fun validate(props: Map<String, String?>): List<ConfigValue?> {
        return ArrayList(validateAll(props).values)
    }

    fun validateAll(props: Map<String, String?>): Map<String, ConfigValue?> {
        val configValues: MutableMap<String, ConfigValue?> = HashMap()
        for (name: String in configKeys.keys) {
            configValues[name] = ConfigValue(name)
        }
        val undefinedConfigKeys = undefinedDependentConfigs()
        for (undefinedConfigKey: String in undefinedConfigKeys) {
            val undefinedConfigValue = ConfigValue(undefinedConfigKey)
            undefinedConfigValue.addErrorMessage("$undefinedConfigKey is referred in the dependents, but not defined.")
            undefinedConfigValue.visible(false)
            configValues[undefinedConfigKey] = undefinedConfigValue
        }
        val parsed = parseForValidate(props, configValues)
        return validate(parsed, configValues)
    }

    // package accessible for testing
    fun parseForValidate(
        props: Map<String, String?>,
        configValues: Map<String, ConfigValue?>,
    ): Map<String, Any?> {
        val parsed: MutableMap<String, Any?> = HashMap()
        val configsWithNoParent = getConfigsWithNoParent()

        configsWithNoParent.forEach { name ->
            parseForValidate(name, props, parsed, configValues)
        }
        return parsed
    }

    private fun validate(
        parsed: Map<String, Any?>,
        configValues: MutableMap<String, ConfigValue?>,
    ): Map<String, ConfigValue?> {
        val configsWithNoParent = getConfigsWithNoParent()

        configsWithNoParent.forEach { name ->
            validate(name, parsed, configValues)
        }

        return configValues
    }

    private fun undefinedDependentConfigs(): List<String> {
        val undefinedConfigKeys: MutableSet<String> = hashSetOf()

        configKeys.values.forEach { configKey ->
            configKey.dependents.forEach { dependent ->
                if (!configKeys.containsKey(dependent)) {
                    undefinedConfigKeys.add(dependent)
                }
            }
        }

        return ArrayList(undefinedConfigKeys)
    }

    // package accessible for testing
    fun getConfigsWithNoParent(): Set<String> {
        configsWithNoParent?.let { return it }

        val configsWithParent = mutableSetOf<String>()
        configKeys.values.forEach { configKey ->
            configsWithParent.addAll(configKey.dependents)
        }

        val configs: MutableSet<String> = configKeys.keys.toHashSet()

        configs.removeAll(configsWithParent)
        configsWithNoParent = configs

        return configs
    }

    private fun parseForValidate(
        name: String,
        props: Map<String, String?>,
        parsed: MutableMap<String, Any?>,
        configs: Map<String, ConfigValue?>,
    ) {
        if (!configKeys.containsKey(name)) return

        val key = configKeys[name]!!
        val config = configs[name]!!
        var value: Any? = null

        if (props.containsKey(key.name)) {
            try {
                value = parseType(key.name, props[key.name]!!, key.type)
            } catch (e: ConfigException) {
                config.addErrorMessage(e.message)
            }
        } else if (NO_DEFAULT_VALUE == key.defaultValue) {
            config.addErrorMessage("Missing required configuration \"${key.name}\" which has no default value.")
        } else {
            value = key.defaultValue
        }
        if (key.validator != null) {
            try {
                key.validator.ensureValid(key.name, value)
            } catch (e: ConfigException) {
                config.addErrorMessage(e.message)
            }
        }
        config.value(value)
        parsed[name] = value

        key.dependents.forEach { dependent ->
            parseForValidate(dependent, props, parsed, configs)
        }
    }

    private fun validate(
        name: String,
        parsed: Map<String, Any?>,
        configs: MutableMap<String, ConfigValue?>,
    ) {
        if (!configKeys.containsKey(name)) return

        val key = configKeys[name]!!
        val value = configs[name]!!

        if (key.recommender != null) {
            try {
                val recommendedValues = key.recommender.validValues(name, parsed)
                val originalRecommendedValues = value.recommendedValues

                if (originalRecommendedValues.isNotEmpty()) {
                    val originalRecommendedValueSet = originalRecommendedValues.toHashSet()
                    recommendedValues.filter { originalRecommendedValueSet.contains(it) }
                }

                value.recommendedValues = recommendedValues
                value.visible = key.recommender.visible(name, parsed)
            } catch (e: ConfigException) {
                value.addErrorMessage(e.message)
            }
        }
        configs[name] = value

        key.dependents.forEach { dependent ->
            validate(dependent, parsed, configs)
        }
    }

    /**
     * The config types
     */
    enum class Type {
        BOOLEAN,
        STRING,
        INT,
        SHORT,
        LONG,
        DOUBLE,
        LIST,
        CLASS,
        PASSWORD;

        val isSensitive: Boolean
            get() = this == PASSWORD
    }

    /**
     * The importance level for a configuration
     */
    enum class Importance {
        HIGH,
        MEDIUM,
        LOW
    }

    /**
     * The width of a configuration value
     */
    enum class Width {
        NONE,
        SHORT,
        MEDIUM,
        LONG
    }

    /**
     * This is used by the [validate] to get valid values for a configuration given the current
     * configuration values in order to perform full configuration validation and visibility modification.
     * In case that there are dependencies between configurations, the valid values and visibility
     * for a configuration may change given the values of other configurations.
     */
    interface Recommender {
        /**
         * The valid values for the configuration given the current configuration values.
         *
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The list of valid values. To function properly, the returned objects should have
         * the type defined for the configuration using the recommender.
         */
        fun validValues(name: String, parsedConfig: Map<String, Any?>): List<Any>

        /**
         * Set the visibility of the configuration given the current configuration values.
         *
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The visibility of the configuration
         */
        fun visible(name: String, parsedConfig: Map<String, Any?>): Boolean
    }

    /**
     * Validation logic the user may provide to perform single configuration validation.
     */
    interface Validator {

        /**
         * Perform single configuration validation.
         *
         * @param name The name of the configuration
         * @param value The value of the configuration
         * @throws ConfigException if the value is invalid.
         */
        fun ensureValid(name: String, value: Any?)
    }

    /**
     * Validation logic for numeric ranges.
     *
     * This is a numeric range with inclusive upper bound and inclusive lower bound.
     *
     * @property min the lower bound
     * @property max the upper bound
     */
    class Range private constructor(
        private val min: Number?,
        private val max: Number?,
    ) : Validator {

        override fun ensureValid(name: String, value: Any?) {
            if (value == null) throw ConfigException(name = name, message = "Value must be non-null")
            val n = value as Number
            if (min != null && n.toDouble() < min.toDouble()) throw ConfigException(
                name = name,
                value = value,
                message = "Value must be at least $min",
            )
            if (max != null && n.toDouble() > max.toDouble()) throw ConfigException(
                name = name,
                value = value,
                message = "Value must be no more than $max",
            )
        }

        override fun toString(): String {
            return if (min == null && max == null) "[..]"
            else if (min == null) "[..,$max]"
            else if (max == null) "[$min,...]"
            else "[$min,...,$max]"
        }

        companion object {

            /**
             * A numeric range that checks only the lower bound
             *
             * @param min The minimum acceptable value
             */
            fun atLeast(min: Number): Range {
                return Range(min, null)
            }

            /**
             * A numeric range that checks both the upper (inclusive) and lower bound
             */
            fun between(min: Number, max: Number?): Range {
                return Range(min, max)
            }
        }
    }

    class ValidList private constructor(validStrings: List<String>) : Validator {

        val validString: ValidString = ValidString(validStrings)

        override fun ensureValid(name: String, value: Any?) {
            val values = value as List<String?>
            for (string: String? in values) {
                validString.ensureValid(name, string)
            }
        }

        override fun toString(): String {
            return validString.toString()
        }

        companion object {

            fun `in`(vararg validStrings: String): ValidList {
                return ValidList(listOf(*validStrings))
            }
        }
    }

    class ValidString internal constructor(val validStrings: List<String?>) : Validator {

        override fun ensureValid(name: String, value: Any?) {
            val s = value as String?

            if (!validStrings.contains(s)) throw ConfigException(
                name = name,
                value = value,
                message = "String must be one of: ${validStrings.joinToString(", ")}"
            )
        }

        override fun toString(): String = "[${validStrings.joinToString(", ")}]"

        companion object {

            fun `in`(vararg validStrings: String): ValidString {
                return ValidString(listOf(*validStrings))
            }
        }
    }

    class CaseInsensitiveValidString private constructor(validStrings: List<String>) : Validator {

        val validStrings: Set<String>

        init {
            this.validStrings = validStrings.stream()
                .map { s: String -> s.uppercase() }
                .collect(Collectors.toSet())
        }

        override fun ensureValid(name: String, value: Any?) {
            val s = value as String?

            if (s == null || !validStrings.contains(s.uppercase())) {
                throw ConfigException(
                    name = name,
                    value = value,
                    message = "String must be one of (case insensitive): ${validStrings.joinToString(", ")}"
                )
            }
        }

        override fun toString(): String {
            return "(case insensitive) [" + join(validStrings, ", ") + "]"
        }

        companion object {

            fun `in`(vararg validStrings: String): CaseInsensitiveValidString {
                return CaseInsensitiveValidString(listOf(*validStrings))
            }
        }
    }

    class NonNullValidator : Validator {

        override fun ensureValid(name: String, value: Any?) {
            if (value == null) {
                // Pass in the string null to avoid the spotbugs warning
                throw ConfigException(
                    name = name,
                    value = "null",
                    message = "entry must be non null"
                )
            }
        }

        override fun toString(): String {
            return "non-null string"
        }
    }

    class LambdaValidator private constructor(
        var ensureValid: (String, Any?) -> Unit,
        var toStringFunction: () -> String,
    ) : Validator {

        override fun ensureValid(name: String, value: Any?) {
            ensureValid(name, value)
        }

        override fun toString(): String {
            return toStringFunction()
        }

        companion object {

            fun with(
                ensureValid: (String, Any?) -> Unit,
                toStringFunction: () -> String,
            ): LambdaValidator {
                return LambdaValidator(ensureValid, toStringFunction)
            }
        }
    }

    class CompositeValidator private constructor(
        private val validators: List<Validator>,
    ) : Validator {

        override fun ensureValid(name: String, value: Any?) {
            validators.forEach { validator -> validator.ensureValid(name, value) }
        }

        override fun toString(): String {
            val desc = StringBuilder()

            validators.forEach { validator ->
                if (desc.isNotEmpty()) desc.append(',').append(' ')
                desc.append(validator)
            }

            return desc.toString()
        }

        companion object {

            fun of(vararg validators: Validator): CompositeValidator {
                return CompositeValidator(listOf(*validators))
            }
        }
    }

    class NonEmptyString : Validator {

        override fun ensureValid(name: String, value: Any?) {
            val s = value as String?

            if (s != null && s.isEmpty()) {
                throw ConfigException(
                    name = name,
                    value = value,
                    message = "String must be non-empty"
                )
            }
        }

        override fun toString(): String {
            return "non-empty string"
        }
    }

    class NonEmptyStringWithoutControlChars : Validator {
        override fun ensureValid(name: String, value: Any?) {
            val s = value as String?
            if (s == null) {
                // This can happen during creation of the config object due to no default value being defined for the
                // name configuration - a missing name parameter is caught when checking for mandatory parameters,
                // thus we can ok a null value here
                return
            } else if (s.isEmpty()) throw ConfigException(
                name = name,
                value = value,
                message = "String may not be empty"
            )

            // Check name string for illegal characters
            val foundIllegalCharacters = ArrayList<Int>()
            for (i in 0 until s.length) {
                if (Character.isISOControl(s.codePointAt(i))) {
                    foundIllegalCharacters.add(s.codePointAt(i))
                }
            }
            if (foundIllegalCharacters.isNotEmpty()) throw ConfigException(
                name = name,
                value = value,
                message = "String may not contain control sequences but had the following ASCII chars: " +
                        foundIllegalCharacters.joinToString(", "),
            )
        }

        override fun toString(): String {
            return "non-empty string without ISO control characters"
        }

        companion object {
            fun nonEmptyStringWithoutControlChars(): NonEmptyStringWithoutControlChars {
                return NonEmptyStringWithoutControlChars()
            }
        }
    }

    class ListSize private constructor(val maxSize: Int) : Validator {

        override fun ensureValid(name: String, value: Any?) {
            val values = value as List<String>

            if (values.size > maxSize) throw ConfigException(
                name = name,
                value = value,
                message = "exceeds maximum list size of [$maxSize]."
            )
        }

        override fun toString(): String {
            return "List containing maximum of $maxSize elements"
        }

        companion object {

            fun atMostOfSize(maxSize: Int): ListSize {
                return ListSize(maxSize)
            }
        }
    }

    class ConfigKey(
        val name: String,
        val type: Type,
        defaultValue: Any?,
        val validator: Validator?,
        val importance: Importance,
        val documentation: String?,
        val group: String?,
        val orderInGroup: Int,
        val width: Width?,
        val displayName: String?,
        val dependents: List<String>,
        val recommender: Recommender?,
        val internalConfig: Boolean,
    ) {
        val defaultValue: Any?

        init {
            this.defaultValue = if (NO_DEFAULT_VALUE == defaultValue) NO_DEFAULT_VALUE
            else parseType(name, defaultValue, type)

            if (validator != null && hasDefault()) validator.ensureValid(name, this.defaultValue)
        }

        fun hasDefault(): Boolean = NO_DEFAULT_VALUE != defaultValue

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("type")
        )
        fun type(): Type = type
    }

    internal fun headers(): List<String> {
        return mutableListOf("Name", "Description", "Type", "Default", "Valid Values", "Importance")
    }

    internal fun getConfigValue(key: ConfigKey, headerName: String): String? {
        when (headerName) {
            "Name" -> return key.name
            "Description" -> return key.documentation
            "Type" -> return key.type.toString().lowercase()
            "Default" -> if (key.hasDefault()) {
                if (key.defaultValue == null) return "null"
                val defaultValueStr = convertToString(key.defaultValue, key.type)
                return if (defaultValueStr!!.isEmpty()) "\"\"" else {
                    var suffix = ""
                    if (key.name.endsWith(".bytes"))
                        suffix = niceMemoryUnits((key.defaultValue as Number).toLong())
                    else if (key.name.endsWith(".ms"))
                        suffix = niceTimeUnits((key.defaultValue as Number).toLong())
                    defaultValueStr + suffix
                }
            } else return ""

            "Valid Values" -> return if (key.validator != null) key.validator.toString() else ""
            "Importance" -> return key.importance.toString().lowercase()
            else -> throw RuntimeException("Can't find value for header '$headerName' in ${key.name}")
        }
    }

    private fun addHeader(builder: StringBuilder, headerName: String) {
        builder.append("<th>")
        builder.append(headerName)
        builder.append("</th>\n")
    }

    private fun addColumnValue(builder: StringBuilder, value: String?) {
        builder.append("<td>")
        builder.append(value)
        builder.append("</td>")
    }

    /**
     * Converts this config into an HTML table that can be embedded into docs.
     * If `dynamicUpdateModes` is non-empty, a "Dynamic Update Mode" column
     * will be included n the table with the value of the update mode. Default
     * mode is "read-only".
     * @param dynamicUpdateModes Config name -> update mode mapping
     */
    @JvmOverloads
    fun toHtmlTable(dynamicUpdateModes: Map<String?, String?> = emptyMap<String?, String>()): String {
        val hasUpdateModes = !dynamicUpdateModes.isEmpty()
        val configs = sortedConfigs()
        val b = StringBuilder()
        b.append("<table class=\"data-table\"><tbody>\n")
        b.append("<tr>\n")
        // print column headers
        for (headerName: String in headers()) {
            addHeader(b, headerName)
        }
        if (hasUpdateModes) addHeader(b, "Dynamic Update Mode")
        b.append("</tr>\n")
        for (key: ConfigKey in configs) {
            if (key.internalConfig) {
                continue
            }
            b.append("<tr>\n")
            // print column values
            for (headerName: String in headers()) {
                addColumnValue(b, getConfigValue(key, headerName))
                b.append("</td>")
            }
            if (hasUpdateModes) {
                var updateMode = dynamicUpdateModes[key.name]
                if (updateMode == null) updateMode = "read-only"
                addColumnValue(b, updateMode)
            }
            b.append("</tr>\n")
        }
        b.append("</tbody></table>")
        return b.toString()
    }

    /**
     * Get the configs formatted with reStructuredText, suitable for embedding in Sphinx
     * documentation.
     */
    fun toRst(): String {
        val b = StringBuilder()
        for (key: ConfigKey in sortedConfigs()) {
            if (key.internalConfig) {
                continue
            }
            getConfigKeyRst(key, b)
            b.append("\n")
        }
        return b.toString()
    }

    /**
     * Configs with new metadata (group, orderInGroup, dependents) formatted with reStructuredText,
     * suitable for embedding in Sphinx documentation.
     */
    fun toEnrichedRst(): String {
        val b = StringBuilder()
        var lastKeyGroupName = ""

        sortedConfigs().forEach { key ->
            if (key.internalConfig) return@forEach

            if (key.group != null) {
                if (!lastKeyGroupName.equals(key.group, ignoreCase = true)) {
                    b.append(key.group).append("\n")
                    val underLine = CharArray(key.group.length) { '^' }
                    b.append(String(underLine)).append("\n\n")
                }
                lastKeyGroupName = key.group
            }
            getConfigKeyRst(key, b)
            if (key.dependents.isNotEmpty()) {
                var j = 0
                b.append("  * Dependents: ")

                key.dependents.forEach { dependent ->
                    b.append("``")
                    b.append(dependent)
                    if (++j == key.dependents.size) b.append("``") else b.append("``, ")
                }
                b.append("\n")
            }
            b.append("\n")
        }
        return b.toString()
    }

    /**
     * Shared content on Rst and Enriched Rst.
     */
    private fun getConfigKeyRst(key: ConfigKey, b: StringBuilder) {
        b.append("``").append(key.name).append("``").append("\n")
        if (key.documentation != null) key.documentation.split("\n".toRegex())
            .dropLastWhile { it.isEmpty() }
            .forEach { docLine ->
                if (docLine.isEmpty()) return@forEach
                b.append("  ")
                    .append(docLine)
                    .append("\n\n")
            }
        else b.append("\n")

        b.append("  * Type: ")
            .append(getConfigValue(key, "Type"))
            .append("\n")

        if (key.hasDefault()) {
            b.append("  * Default: ")
                .append(getConfigValue(key, "Default"))
                .append("\n")
        }

        if (key.validator != null) {
            b.append("  * Valid Values: ")
                .append(getConfigValue(key, "Valid Values"))
                .append("\n")
        }
        b.append("  * Importance: ")
            .append(getConfigValue(key, "Importance"))
            .append("\n")
    }

    /**
     * Get a list of configs sorted taking the 'group' and 'orderInGroup' into account.
     *
     * If grouping is not specified, the result will reflect "natural" order: listing required
     * fields first, then ordering by importance, and finally by name.
     */
    private fun sortedConfigs(): List<ConfigKey> {
        val groupOrd: MutableMap<String?, Int> = HashMap(groups.size)
        var ord = 0

        groups.forEach { group -> groupOrd[group] = ord++ }

        return configKeys.values.sortedWith { k1, k2 -> compare(k1, k2, groupOrd) }
    }

    private fun compare(k1: ConfigKey, k2: ConfigKey, groupOrd: Map<String?, Int>): Int {
        var cmp = if (k1.group == null) (if (k2.group == null) 0 else -1)
        else if (k2.group == null) 1
        else groupOrd[k1.group]!!.compareTo(groupOrd[k2.group]!!)

        if (cmp == 0) {
            cmp = k1.orderInGroup.compareTo(k2.orderInGroup)
            if (cmp == 0) {
                // first take anything with no default value
                if (!k1.hasDefault() && k2.hasDefault()) cmp = -1
                else if (!k2.hasDefault() && k1.hasDefault()) cmp = 1
                else {
                    cmp = k1.importance.compareTo(k2.importance)
                    if (cmp == 0) return k1.name.compareTo(k2.name)
                }
            }
        }

        return cmp
    }

    fun embed(keyPrefix: String, groupPrefix: String, startingOrd: Int, child: ConfigDef) {
        var orderInGroup = startingOrd
        for (key: ConfigKey in child.sortedConfigs()) {
            define(
                ConfigKey(
                    keyPrefix + key.name,
                    key.type,
                    key.defaultValue,
                    embeddedValidator(keyPrefix, key.validator),
                    key.importance,
                    key.documentation,
                    groupPrefix + if (key.group == null) "" else ": " + key.group,
                    orderInGroup++,
                    key.width,
                    key.displayName,
                    embeddedDependents(keyPrefix, key.dependents),
                    embeddedRecommender(keyPrefix, key.recommender),
                    key.internalConfig
                )
            )
        }
    }

    /**
     * Converts this config into an HTML list that can be embedded into docs.
     * If `dynamicUpdateModes` is non-empty, a "Dynamic Update Mode" label
     * will be included in the config details with the value of the update mode. Default
     * mode is "read-only".
     * @param dynamicUpdateModes Config name -> update mode mapping.
     */
    fun toHtml(dynamicUpdateModes: Map<String, String> = emptyMap()): String {
        return toHtml(4, Function.identity(), dynamicUpdateModes)
    }

    /**
     * Converts this config into an HTML list that can be embedded into docs.
     * If `dynamicUpdateModes` is non-empty, a "Dynamic Update Mode" label
     * will be included in the config details with the value of the update mode. Default
     * mode is "read-only".
     * @param headerDepth The top level header depth in the generated HTML.
     * @param idGenerator A function for computing the HTML id attribute in the generated HTML from
     * a given config name.
     * @param dynamicUpdateModes Config name -> update mode mapping.
     *
     * Converts this config into an HTML list that can be embedded into docs.
     * @param headerDepth The top level header depth in the generated HTML.
     * @param idGenerator A function for computing the HTML id attribute in the generated HTML from
     * a given config name.
     */
    fun toHtml(
        headerDepth: Int,
        idGenerator: Function<String, String>,
        dynamicUpdateModes: Map<String, String> = emptyMap(),
    ): String {
        val hasUpdateModes = dynamicUpdateModes.isNotEmpty()
        val configs = sortedConfigs()
        val b = StringBuilder()
        b.append("<ul class=\"config-list\">\n")
        for (key: ConfigKey in configs) {
            if (key.internalConfig) continue

            b.append("<li>\n")
            b.append(
                String.format(
                    "<h%1\$d><a id=\"%3\$s\"></a><a id=\"%2\$s\" href=\"#%2\$s\">%3\$s</a>" +
                            "</h%1\$d>%n", headerDepth, idGenerator.apply(key.name), key.name
                )
            )
            b.append("<p>")

            if (key.documentation != null) {
                b.append(key.documentation.replace("\n".toRegex(), "<br>"))
            }

            b.append("</p>\n")
            b.append("<table><tbody>\n")

            for (detail: String in headers()) {
                if ((detail == "Name") || (detail == "Description")) continue
                addConfigDetail(b, detail, getConfigValue(key, detail))
            }
            if (hasUpdateModes) {
                var updateMode = dynamicUpdateModes[key.name]
                if (updateMode == null) updateMode = "read-only"
                addConfigDetail(b, "Update Mode", updateMode)
            }
            b.append("</tbody></table>\n")
            b.append("</li>\n")
        }
        b.append("</ul>\n")
        return b.toString()
    }

    companion object {
        private val COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*")

        /**
         * A unique Java object which represents the lack of a default value.
         */
        val NO_DEFAULT_VALUE = Any()

        /**
         * Parse a value according to its expected type.
         * @param name  The config name
         * @param value The config value
         * @param type  The expected type
         * @return The parsed object
         */
        fun parseType(name: String?, value: Any?, type: Type): Any? {
            try {
                if (value == null) return null
                var trimmed: String? = null

                if (value is String) trimmed = value.trim { it <= ' ' }

                when (type) {
                    Type.BOOLEAN -> return if (value is String) {
                        if (trimmed.equals("true", ignoreCase = true)) true
                        else if (trimmed.equals("false", ignoreCase = true)) false
                        else throw ConfigException(
                            name = name,
                            value = value,
                            message = "Expected value to be either true or false"
                        )
                    } else if (value is Boolean) value
                    else throw ConfigException(
                        name = name,
                        value = value,
                        message = "Expected value to be either true or false"
                    )

                    Type.PASSWORD -> return when (value) {
                        is Password -> value
                        is String -> Password(trimmed!!)

                        else -> throw ConfigException(
                            name = name,
                            value = value,
                            message = "Expected value to be a string, but it was a ${value.javaClass.name}"
                        )
                    }

                    Type.STRING -> return if (value is String) trimmed
                    else throw ConfigException(
                        name = name,
                        value = value,
                        message = "Expected value to be a string, but it was a ${value.javaClass.name}"
                    )

                    Type.INT -> return when (value) {
                        is Int -> value
                        is String -> trimmed!!.toInt()
                        else -> throw ConfigException(
                            name = name,
                            value = value,
                            message = "Expected value to be a 32-bit integer, but it was a ${value.javaClass.name}"
                        )
                    }

                    Type.SHORT -> return when (value) {
                        is Short -> value
                        is String -> trimmed!!.toShort()
                        else -> throw ConfigException(
                            name = name,
                            value = value,
                            message = "Expected value to be a 16-bit integer (short), but it was " +
                                    "a ${value.javaClass.name}"
                        )
                    }

                    Type.LONG -> {
                        return when (value) {
                            is Int -> value.toLong()
                            is Long -> value
                            is String -> trimmed!!.toLong()
                            else -> throw ConfigException(
                                name = name,
                                value = value,
                                message = "Expected value to be a 64-bit integer (long), but it " +
                                        "was a ${value.javaClass.name}"
                            )
                        }
                    }

                    Type.DOUBLE -> return when (value) {
                        is Number -> value.toDouble()
                        is String -> trimmed!!.toDouble()
                        else -> throw ConfigException(
                            name = name,
                            value = value,
                            message = "Expected value to be a double, but it was a " +
                                    value.javaClass.name
                        )
                    }

                    Type.LIST -> return when (value) {
                        is List<*> -> value
                        is String -> {
                            if (trimmed!!.isEmpty()) emptyList<Any>()
                            else COMMA_WITH_WHITESPACE.split(trimmed, -1).toList()
                        }

                        else -> throw ConfigException(
                            name = name,
                            value = value,
                            message = "Expected a comma separated list."
                        )
                    }

                    Type.CLASS -> return when (value) {
                        is Class<*> -> value
                        is String -> {
                            val contextOrKafkaClassLoader = contextOrKafkaClassLoader
                            // Use loadClass here instead of Class.forName because the name we use here may be an alias
                            // and not match the name of the class that gets loaded. If that happens, Class.forName can
                            // throw an exception.
                            val klass = contextOrKafkaClassLoader.loadClass(trimmed)
                            // Invoke forName here with the true name of the requested class to cause class
                            // initialization to take place.
                            Class.forName(klass.name, true, contextOrKafkaClassLoader)
                        }

                        else -> throw ConfigException(
                            name = name,
                            value = value,
                            message = "Expected a Class instance or class name."
                        )
                    }

                    else -> throw IllegalStateException("Unknown type.")
                }
            } catch (e: NumberFormatException) {
                throw ConfigException(name = name, value = value, message = "Not a number of type $type")
            } catch (e: ClassNotFoundException) {
                throw ConfigException(name = name, value = value, message = "Class $value could not be found.")
            }
        }

        fun convertToString(parsedValue: Any?, type: Type?): String? {
            if (parsedValue == null) return null
            if (type == null) return parsedValue.toString()

            return when (type) {
                Type.BOOLEAN,
                Type.SHORT,
                Type.INT,
                Type.LONG,
                Type.DOUBLE,
                Type.STRING,
                Type.PASSWORD,
                -> parsedValue.toString()

                Type.LIST -> {
                    val valueList = parsedValue as List<*>
                    valueList.joinToString(",")
                }

                Type.CLASS -> {
                    val clazz = parsedValue as Class<*>
                    clazz.name
                }
            }
        }

        /**
         * Converts a map of config (key, value) pairs to a map of strings where each value
         * is converted to a string. This method should be used with care since it stores
         * actual password values to String. Values from this map should never be used in log entries.
         */
        fun convertToStringMapWithPasswordValues(configs: Map<String, *>): Map<String, String> {
            val result: MutableMap<String, String> = HashMap()

            configs.forEach { (key, value) ->
                val strValue: String? = when (value) {
                    is Password -> value.value
                    is List<*> -> convertToString(value, Type.LIST)
                    is Class<*> -> convertToString(value, Type.CLASS)
                    else -> convertToString(value, null)
                }

                if (strValue != null) result[key] = strValue
            }
            return result
        }

        fun niceMemoryUnits(bytes: Long): String {
            var value = bytes
            var i = 0

            while (value != 0L && i < 4) {
                if (value % 1024L == 0L) {
                    value /= 1024L
                    i++
                } else break
            }

            val resultFormat = " (" + value + " %s" + (if (value == 1L) ")" else "s)")

            return when (i) {
                1 -> String.format(resultFormat, "kibibyte")
                2 -> String.format(resultFormat, "mebibyte")
                3 -> String.format(resultFormat, "gibibyte")
                4 -> String.format(resultFormat, "tebibyte")
                else -> ""
            }
        }

        fun niceTimeUnits(millis: Long): String {
            var value = millis
            val divisors = longArrayOf(1000, 60, 60, 24)
            val units = arrayOf("second", "minute", "hour", "day")
            var i = 0

            while (value != 0L && i < 4) {
                if (value % divisors[i] == 0L) {
                    value /= divisors[i]
                    i++
                } else {
                    break
                }
            }

            return if (i > 0) {
                " (" + value + " " + units.get(i - 1) + (if (value > 1) "s)" else ")")
            } else ""
        }

        /**
         * Returns a new validator instance that delegates to the base validator but unprefixes the
         * config name along the way.
         */
        private fun embeddedValidator(keyPrefix: String, base: Validator?): Validator? {
            return if (base == null) null else object : Validator {
                override fun ensureValid(name: String, value: Any?) {
                    base.ensureValid(name.substring(keyPrefix.length), value)
                }

                override fun toString(): String {
                    return base.toString()
                }
            }
        }

        /**
         * Updated list of dependent configs with the specified `prefix` added.
         */
        private fun embeddedDependents(
            keyPrefix: String,
            dependents: List<String>,
        ): List<String> = dependents.map { keyPrefix + it }

        /**
         * Returns a new recommender instance that delegates to the base recommender but unprefixes
         * the input parameters along the way.
         */
        private fun embeddedRecommender(keyPrefix: String, base: Recommender?): Recommender? {
            return if (base == null) null else object : Recommender {

                override fun validValues(name: String, parsedConfig: Map<String, Any?>): List<Any> {
                    return base.validValues(unprefixed(name), unprefixed(parsedConfig))
                }

                override fun visible(name: String, parsedConfig: Map<String, Any?>): Boolean {
                    return base.visible(unprefixed(name), unprefixed(parsedConfig))
                }

                private fun unprefixed(k: String): String {
                    return k.substring(keyPrefix.length)
                }

                private fun unprefixed(parsedConfig: Map<String, Any?>): Map<String, Any?> {
                    val unprefixedParsedConfig: MutableMap<String, Any?> = HashMap(parsedConfig.size)

                    parsedConfig.forEach { (key, value) ->
                        if (key.startsWith(keyPrefix)) {
                            unprefixedParsedConfig[unprefixed(key)] = value
                        }
                    }
                    return unprefixedParsedConfig
                }
            }
        }

        private fun addConfigDetail(builder: StringBuilder, name: String, value: String?) {
            builder.append("<tr><th>$name:</th><td>$value</td></tr>\n")
        }
    }
}
