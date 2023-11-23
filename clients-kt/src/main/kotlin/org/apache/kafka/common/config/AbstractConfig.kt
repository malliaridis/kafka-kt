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

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.provider.ConfigProvider
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.utils.Utils
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass

/**
 * A convenient base class for configurations to extend.
 *
 * This class holds both the original configuration that was provided as well as the parsed
 *
 * Constructor constructs a configuration with a ConfigDef and the configuration properties, which
 * can include properties for zero or more [ConfigProvider] that will be used to resolve variables
 * in configuration property values.
 *
 * The originals is a name-value pair configuration properties and optional config provider
 * configs. The value of the configuration can be a variable as defined below or the actual
 * value. This constructor will first instantiate the ConfigProviders using the config provider
 * configs, then it will find all the variables in the values of the originals configurations,
 * attempt to resolve the variables using the named ConfigProviders, and then parse and validate
 * the configurations.
 *
 * ConfigProvider configs can be passed either as configs in the originals map or in the
 * separate configProviderProps map. If config providers properties are passed in the
 * configProviderProps any config provider properties in originals map will be ignored. If
 * ConfigProvider properties are not provided, the constructor will skip the variable
 * substitution step and will simply validate and parse the supplied configuration.
 *
 * The "`config.providers`" configuration property and all configuration properties that begin
 * with the "`config.providers.`" prefix are reserved. The "`config.providers`" configuration
 * property specifies the names of the config providers, and properties that begin with the
 * "`config.providers..`" prefix correspond to the properties for that named provider. For
 * example, the "`config.providers..class`" property specifies the name of the [ConfigProvider]
 * implementation class that should be used for the provider.
 *
 * The keys for ConfigProvider configs in both originals and configProviderProps will start with
 * the above-mentioned "`config.providers.`" prefix.
 *
 * Variables have the form "${providerName:[path:]key}", where "providerName" is the name of a
 * ConfigProvider, "path" is an optional string, and "key" is a required string. This variable
 * is resolved by passing the "key" and optional "path" to a ConfigProvider with the specified
 * name, and the result from the ConfigProvider is then used in place of the variable. Variables
 * that cannot be resolved by the AbstractConfig constructor will be left unchanged in the
 * configuration.
 *
 * @param definition the definition of the configurations; may not be null
 * @param originals the configuration properties plus any optional config provider properties;
 * @param configProviderProps the map of properties of config providers which will be
 * instantiated by the constructor to resolve any variables in `originals`; may be null or empty
 * @param doLog whether the configurations should be logged
 */
open class AbstractConfig(
    private val definition: ConfigDef,
    originals: Map<String, Any?>,
    configProviderProps: Map<String, Any?> = emptyMap(),
    doLog: Boolean = true,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Configs for which values have been requested, used to detect unused configs.
     * This set must be concurrent modifiable and iterable. It will be modified
     * when directly accessed or as a result of RecordingMap access.
     */
    private val used: MutableSet<String?> = ConcurrentHashMap.newKeySet()

    /* the original values passed in by the user */
    private val originals: Map<String, Any>

    /* the parsed values */
    private val values: MutableMap<String, Any?>

    init {
        this.originals = resolveConfigVariables(configProviderProps, originals)
        values = definition.parse(this.originals).toMutableMap()
        val configUpdates = postProcessParsedConfig(values)
        for ((key, value) in configUpdates) values[key] = value
        definition.parse(values)
        if (doLog) logAll()
    }

    /**
     * Called directly after user configs got parsed (and thus default values got set).
     * This allows to change default values for "secondary defaults" if required.
     *
     * @param parsedValues unmodifiable map of current configuration
     * @return a map of updates that should be applied to the configuration (will be validated to
     * prevent bad updates)
     */
    protected open fun postProcessParsedConfig(
        parsedValues: Map<String, Any?>,
    ): Map<String, Any?> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    protected operator fun <T> get(key: String): T? {
        if (!values.containsKey(key)) throw ConfigException("Unknown configuration '$key'")
        used.add(key)
        return values[key] as T?
    }

    fun ignore(key: String) = used.add(key)

    fun getShort(key: String): Short? = get(key)

    fun getInt(key: String): Int? = get(key)

    fun getLong(key: String): Long? = get(key)

    fun getDouble(key: String): Double? = get(key)

    fun getList(key: String): List<String>? = get(key)

    fun getBoolean(key: String): Boolean? = get(key)

    fun getString(key: String): String? = get(key)

    fun typeOf(key: String): ConfigDef.Type? {
        val configKey = definition.configKeys[key] ?: return null
        return configKey.type
    }

    fun documentationOf(key: String): String? {
        val configKey = definition.configKeys[key] ?: return null
        return configKey.documentation
    }

    fun getPassword(key: String): Password? = get(key)

    fun getClass(key: String): Class<*>? = get(key)

    fun unused(): Set<String> {
        val keys = mutableSetOf<String>()
        keys.removeAll(used)

        return keys
    }

    fun originals(): MutableMap<String, Any?> {
        val copy: MutableMap<String, Any?> = RecordingMap()
        copy.putAll(originals)
        return copy
    }

    fun originals(configOverrides: Map<String, Any?>): Map<String, Any?> {
        val copy: MutableMap<String, Any?> = RecordingMap()
        copy.putAll(originals)
        copy.putAll(configOverrides)
        return copy
    }

    /**
     * Get all the original settings, ensuring that all values are of type String.
     *
     * @return the original settings
     * @throws ClassCastException if any of the values are not strings
     */
    fun originalsStrings(): Map<String, String> {
        val copy: MutableMap<String, String> = RecordingMap()

        originals.forEach { (key, value) ->
            if (value !is String) throw ClassCastException(
                "Non-string value found in original settings for key $key: " + value.javaClass.name
            )
            copy[key] = value
        }
        return copy
    }

    /**
     * Gets all original settings with the given prefix.
     *
     * @param prefix The prefix to use as a filter
     * @param strip Whether to strip the prefix before adding to the output. Defaults to `true`.
     * @return a Map containing the settings with the prefix
     */
    fun originalsWithPrefix(prefix: String, strip: Boolean = true): Map<String, Any> {
        val result: MutableMap<String, Any> = RecordingMap(prefix, false)

        originals.forEach { (key, value) ->
            if (key.startsWith(prefix) && key.length > prefix.length) {
                if (strip) result[key.substring(prefix.length)] = value
                else result[key] = value
            }
        }
        return result
    }

    /**
     * Put all keys that do not start with `prefix` and their parsed values in the result map and
     * then put all the remaining keys with the prefix stripped and their parsed values in the
     * result map.
     *
     * This is useful if one wants to allow prefixed configs to override default ones.
     *
     * Two forms of prefixes are supported:
     *
     * - listener.name.{listenerName}.some.prop: If the provided prefix is
     *   `listener.name.{listenerName}.`, the key `some.prop` with the value parsed using the
     *   definition of `some.prop` is returned.
     * - listener.name.{listenerName}.{mechanism}.some.prop: If the provided prefix is
     *   `listener.name.{listenerName}.`, the key `{mechanism}.some.prop` with the value parsed
     *   using the definition of `some.prop` is returned.
     *
     * This is used to provide per-mechanism configs for a broker listener (e.g sasl.jaas.config).
     */
    fun valuesWithPrefixOverride(prefix: String): Map<String, Any?> {
        val result: MutableMap<String, Any?> = RecordingMap(values(), prefix, true)

        originals.forEach { (key, value) ->

            if (key.startsWith(prefix) && key.length > prefix.length) {
                val keyWithNoPrefix = key.substring(prefix.length)
                var configKey = definition.configKeys[keyWithNoPrefix]

                if (configKey != null)
                    result[keyWithNoPrefix] = definition.parseValue(configKey, value, true)
                else {
                    val keyWithNoSecondaryPrefix =
                        keyWithNoPrefix.substring(keyWithNoPrefix.indexOf('.') + 1)
                    configKey = definition.configKeys[keyWithNoSecondaryPrefix]

                    if (configKey != null) result[keyWithNoPrefix] =
                        definition.parseValue(configKey, value, true)
                }
            }
        }

        return result
    }

    /**
     * If at least one key with `prefix` exists, all prefixed values will be parsed and put into
     * map. If no value with `prefix` exists all unprefixed values will be returned.
     *
     * This is useful if one wants to allow prefixed configs to override default ones, but wants to
     * use either only prefixed configs or only regular configs, but not mix them.
     */
    fun valuesWithPrefixAllOrNothing(prefix: String): Map<String, Any?> {
        val withPrefix = originalsWithPrefix(prefix, true)

        return if (withPrefix.isEmpty()) RecordingMap(values(), "", true)
        else {
            val result: MutableMap<String, Any?> = RecordingMap(prefix, true)

            withPrefix.forEach { (key, value) ->
                val configKey = definition.configKeys[key]
                if (configKey != null) result[key] =
                    definition.parseValue(configKey, value, true)
            }
            result
        }
    }

    fun values(): Map<String, Any?> = RecordingMap(values)

    fun nonInternalValues(): Map<String, *> {
        val nonInternalConfigs: MutableMap<String, Any?> = RecordingMap()

        values.forEach { (key, value) ->
            val configKey = definition.configKeys[key]

            if (configKey == null || !configKey.internalConfig)
                nonInternalConfigs[key] = value
        }

        return nonInternalConfigs
    }

    private fun logAll() {
        val b = StringBuilder()
        b.append(javaClass.simpleName)
        b.append(" values: ")
        b.append(Utils.NL)
        for ((key, value) in TreeMap(values)) {
            b.append('\t')
            b.append(key)
            b.append(" = ")
            b.append(value)
            b.append(Utils.NL)
        }
        log.info(b.toString())
    }

    /**
     * Log warnings for any unused configurations
     */
    fun logUnused() {
        val unusedkeys = unused()
        if (unusedkeys.isNotEmpty()) {
            log.warn("These configurations '{}' were supplied but are not used yet.", unusedkeys)
        }
    }

    private fun <T> getConfiguredInstance(
        klass: Any?,
        t: Class<T>,
        configPairs: Map<String, Any?>,
    ): T {

        val o: T = when (klass) {
            is String -> {
                try {
                    Utils.newInstance(klass, t)
                } catch (e: ClassNotFoundException) {
                    throw KafkaException("Class $klass cannot be found", e)
                }
            }

            is Class<*> -> Utils.newInstance(klass as Class<T>)

            else -> throw KafkaException(
                "Unexpected element of type ${klass?.javaClass?.name}, expected String or Class"
            )
        }
        try {
            if (!t.isInstance(o)) throw KafkaException("$klass is not an instance of ${t.name}")
            if (o is Configurable) o.configure(configPairs)
        } catch (e: Exception) {
            maybeClose(
                o,
                "AutoCloseable object constructed and configured during failed call to " +
                        "getConfiguredInstance"
            )
            throw e
        }
        return t.cast(o)
    }

    /**
     * Get a configured instance of the give class specified by the given configuration key. If the
     * object implements [Configurable] configure it using the configuration.
     *
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @param configOverrides override origin configs
     * @return A configured instance of the class
     */
    fun <T> getConfiguredInstance(
        key: String,
        t: Class<T>,
        configOverrides: Map<String, Any?> = emptyMap<String, Any>(),
    ): T? {
        val c = getClass(key)
        return getConfiguredInstance(c, t, originals(configOverrides))
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     *
     * @param key The configuration key for the class
     * @param t   The interface the class should implement
     * @return The list of configured instances
     */
    fun <T> getConfiguredInstances(key: String, t: Class<T>): List<T> {
        return getConfiguredInstances(key, t, emptyMap())
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration
     * key. The configuration may specify either null or an empty string to indicate no configured
     * instances. In both cases, this method returns an empty list to indicate no configured
     * instances.
     *
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    fun <T> getConfiguredInstances(
        key: String,
        t: Class<T>,
        configOverrides: Map<String, Any?>,
    ): List<T> {
        return getConfiguredInstances(getList(key), t, configOverrides)
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration
     * key. The configuration may specify either null or an empty string to indicate no configured
     * instances. In both cases, this method returns an empty list to indicate no configured
     * instances.
     *
     * @param classNames The list of class names of the instances to create
     * @param t The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    fun <T> getConfiguredInstances(
        classNames: List<String>?,
        t: Class<T>,
        configOverrides: Map<String, Any?>?,
    ): List<T> {
        val objects: MutableList<T> = ArrayList()

        if (classNames == null) return objects

        val configPairs = originals(configOverrides!!)

        try {
            classNames.forEach { klass: Any ->
                val o: T? = getConfiguredInstance(klass, t, configPairs)
                objects.add(t.cast(o))
            }
        } catch (e: Exception) {
            objects.forEach { obj ->
                maybeClose(
                    obj,
                    "AutoCloseable object constructed and configured during failed call to getConfiguredInstances"
                )
            }
            throw e
        }

        return objects
    }

    private fun extractPotentialVariables(configMap: Map<*, *>): Map<String, String> {
        // Variables are tuples of the form "${providerName:[path:]key}". From the configMap we extract the subset of configs with string
        // values as potential variables.
        val configMapAsString: MutableMap<String, String> = HashMap()
        for ((key, value) in configMap) {
            if (value is String) configMapAsString[key as String] = value
        }
        return configMapAsString
    }

    /**
     * Instantiates given list of config providers and fetches the actual values of config variables
     * from the config providers.
     *
     * returns a map of config key and resolved values.
     *
     * @param configProviderProps The map of config provider configs
     * @param originals The map of raw configs.
     * @return map of resolved config variable.
     */
    private fun resolveConfigVariables(
        configProviderProps: Map<String, Any?>?,
        originals: Map<String, Any?>,
    ): Map<String, Any> {

        val resolvedOriginals = mutableMapOf<String, Any?>()
        // As variable configs are strings, parse the originals and obtain the potential variable configs.
        val indirectVariables = extractPotentialVariables(originals)
        resolvedOriginals.putAll(originals)

        val providers = if (configProviderProps.isNullOrEmpty()) instantiateConfigProviders(
            indirectConfigs = indirectVariables,
            providerConfigProperties = originals,
        ) else instantiateConfigProviders(
            indirectConfigs = extractPotentialVariables(configProviderProps),
            providerConfigProperties = configProviderProps,
        )

        if (providers.isNotEmpty()) {
            val configTransformer = ConfigTransformer(providers)
            val result = configTransformer.transform(indirectVariables)

            if (result.data.isNotEmpty()) resolvedOriginals.putAll(result.data)
        }

        providers.values.forEach { x: ConfigProvider? ->
            Utils.closeQuietly(x, "config provider")
        }

        return ResolvingMap(resolvedOriginals, originals)
    }

    private fun configProviderProperties(
        configProviderPrefix: String,
        providerConfigProperties: Map<String, Any?>,
    ): Map<String, Any?> {
        val result = mutableMapOf<String, Any?>()

        providerConfigProperties.forEach { (key, value) ->
            if (key.startsWith(configProviderPrefix) && key.length > configProviderPrefix.length) {
                result[key.substring(configProviderPrefix.length)] = value
            }
        }
        return result
    }

    /**
     * Instantiates and configures the ConfigProviders. The config providers configs are defined as follows:
     * config.providers : A comma-separated list of names for providers.
     * config.providers.{name}.class : The Java class name for a provider.
     * config.providers.{name}.param.{param-name} : A parameter to be passed to the above Java class on initialization.
     * returns a map of config provider name and its instance.
     *
     * @param indirectConfigs          The map of potential variable configs
     * @param providerConfigProperties The map of config provider configs
     * @return map map of config provider name and its instance.
     */
    private fun instantiateConfigProviders(
        indirectConfigs: Map<String, String>,
        providerConfigProperties: Map<String, Any?>,
    ): Map<String, ConfigProvider> {

        val configProviders = indirectConfigs[CONFIG_PROVIDERS_CONFIG]
        if (configProviders.isNullOrEmpty()) return emptyMap()

        val providerMap = mutableMapOf<String, String>()

        configProviders.split(",".toRegex())
            .dropLastWhile { it.isEmpty() }
            .forEach { provider ->

            val providerClass = providerClassProperty(provider)
            if (indirectConfigs.containsKey(providerClass)) providerMap[provider] =
                indirectConfigs[providerClass]!!
        }

        // Instantiate Config Providers
        val configProviderInstances: MutableMap<String, ConfigProvider> = HashMap()

        providerMap.forEach { (key, value) ->
            try {
                val prefix = "$CONFIG_PROVIDERS_CONFIG.$key$CONFIG_PROVIDERS_PARAM"

                val configProperties: Map<String, Any?> =
                    configProviderProperties(prefix, providerConfigProperties)

                val provider = Utils.newInstance(
                    value,
                    ConfigProvider::class.java
                )

                provider.configure(configProperties)
                configProviderInstances[key] = provider
            } catch (e: ClassNotFoundException) {
                log.error("Could not load config provider class $value", e)
                throw ConfigException(
                    providerClassProperty(key),
                    value,
                    "Could not load config provider class or one of its dependencies"
                )
            }
        }
        return configProviderInstances
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val that = o as AbstractConfig
        return (originals == that.originals)
    }

    override fun hashCode(): Int {
        return originals.hashCode()
    }

    /**
     * Marks keys retrieved via `get` as used. This is needed because `Configurable.configure` takes
     * a `Map` instead of an `AbstractConfig` and we can't change that without breaking public API
     * like `Partitioner`.
     */
    private inner class RecordingMap<V> : HashMap<String, V> {
        private val prefix: String
        private val withIgnoreFallback: Boolean

        constructor(
            prefix: String = "",
            withIgnoreFallback: Boolean = false,
        ) {
            this.prefix = prefix
            this.withIgnoreFallback = withIgnoreFallback
        }

        constructor(
            m: Map<String, V>?,
            prefix: String = "",
            withIgnoreFallback: Boolean = false,
        ) : super(m) {
            this.prefix = prefix
            this.withIgnoreFallback = withIgnoreFallback
        }

        override operator fun get(key: String): V? {
            val keyWithPrefix: String =
                if (prefix.isEmpty()) key
                else prefix + key

            ignore(keyWithPrefix)
            if (withIgnoreFallback) ignore(key)

            return super.get(key)
        }
    }

    /**
     * ResolvingMap keeps a track of the original map instance and the resolved configs.
     * The originals are tracked in a separate nested map and may be a `RecordingMap`; thus
     * any access to a value for a key needs to be recorded on the originals map.
     * The resolved configs are kept in the inherited map and are therefore mutable, though any
     * mutations are not applied to the originals.
     */
    private class ResolvingMap<V>(
        resolved: Map<String, V?>,
        private val originals: Map<String, Any?>,
    ) : HashMap<String, V>(resolved) {

        override operator fun get(key: String): V? {
            if (originals.containsKey(key)) {
                // Intentionally ignore the result; call just to mark the original entry as used
                originals[key]
            }
            // But always use the resolved entry
            return super.get(key)
        }
    }

    companion object {

        const val CONFIG_PROVIDERS_CONFIG = "config.providers"

        private const val CONFIG_PROVIDERS_PARAM = ".param."

        private fun <T> maybeClose(obj: T, name: String) {
            if (obj is AutoCloseable) Utils.closeQuietly(obj, name)
        }

        private fun providerClassProperty(providerName: String): String {
            return "$CONFIG_PROVIDERS_CONFIG.$providerName.class"
        }
    }
}
