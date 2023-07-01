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


object SaslConfigs {

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL
     * BREAK USER CODE.
     */

    private const val OAUTHBEARER_NOTE = " Currently applies only to OAUTHBEARER."

    /**
     * SASL mechanism configuration - standard mechanism names are listed
     * [here](http://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml).
     */
    const val SASL_MECHANISM = "sasl.mechanism"

    const val SASL_MECHANISM_DOC =
        "SASL mechanism used for client connections. This may be any mechanism for which a security provider is available. GSSAPI is the default mechanism."

    const val GSSAPI_MECHANISM = "GSSAPI"

    const val DEFAULT_SASL_MECHANISM = GSSAPI_MECHANISM

    const val SASL_JAAS_CONFIG = "sasl.jaas.config"

    const val SASL_JAAS_CONFIG_DOC =
        ("JAAS login context parameters for SASL connections in the format used by JAAS configuration files. "
                + "JAAS configuration file format is described <a href=\"http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html\">here</a>. "
                + "The format for the value is: <code>loginModuleClass controlFlag (optionName=optionValue)*;</code>. For brokers, "
                + "the config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example, "
                + "listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=com.example.ScramLoginModule required;")

    const val SASL_CLIENT_CALLBACK_HANDLER_CLASS = "sasl.client.callback.handler.class"

    const val SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC =
        ("The fully qualified name of a SASL client callback handler class "
                + "that implements the AuthenticateCallbackHandler interface.")

    const val SASL_LOGIN_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class"

    const val SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC =
        ("The fully qualified name of a SASL login callback handler class "
                + "that implements the AuthenticateCallbackHandler interface. For brokers, login callback handler config must be prefixed with "
                + "listener prefix and SASL mechanism name in lower-case. For example, "
                + "listener.name.sasl_ssl.scram-sha-256.sasl.login.callback.handler.class=com.example.CustomScramLoginCallbackHandler")

    const val SASL_LOGIN_CLASS = "sasl.login.class"

    const val SASL_LOGIN_CLASS_DOC =
        ("The fully qualified name of a class that implements the Login interface. "
                + "For brokers, login config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example, "
                + "listener.name.sasl_ssl.scram-sha-256.sasl.login.class=com.example.CustomScramLogin")

    const val SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name"

    const val SASL_KERBEROS_SERVICE_NAME_DOC = ("The Kerberos principal name that Kafka runs as. "
            + "This can be defined either in Kafka's JAAS config or in Kafka's config.")

    const val SASL_KERBEROS_KINIT_CMD = "sasl.kerberos.kinit.cmd"

    const val SASL_KERBEROS_KINIT_CMD_DOC = "Kerberos kinit command path."

    const val DEFAULT_KERBEROS_KINIT_CMD = "/usr/bin/kinit"

    const val SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR = "sasl.kerberos.ticket.renew.window.factor"

    const val SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC =
        ("Login thread will sleep until the specified window factor of time from last refresh"
                + " to ticket's expiry has been reached, at which time it will try to renew the ticket.")

    const val DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR = 0.80

    const val SASL_KERBEROS_TICKET_RENEW_JITTER = "sasl.kerberos.ticket.renew.jitter"

    const val SASL_KERBEROS_TICKET_RENEW_JITTER_DOC =
        "Percentage of random jitter added to the renewal time."

    const val DEFAULT_KERBEROS_TICKET_RENEW_JITTER = 0.05

    const val SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN = "sasl.kerberos.min.time.before.relogin"

    const val SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC =
        "Login thread sleep time between refresh attempts."

    const val DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L

    const val SASL_LOGIN_REFRESH_WINDOW_FACTOR = "sasl.login.refresh.window.factor"

    const val SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC =
        ("Login refresh thread will sleep until the specified window factor relative to the"
                + " credential's lifetime has been reached, at which time it will try to refresh the credential."
                + " Legal values are between 0.5 (50%) and 1.0 (100%) inclusive; a default value of 0.8 (80%) is used"
                + " if no value is specified."
                + OAUTHBEARER_NOTE)

    const val DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR = 0.80

    const val SASL_LOGIN_REFRESH_WINDOW_JITTER = "sasl.login.refresh.window.jitter"

    const val SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC =
        ("The maximum amount of random jitter relative to the credential's lifetime"
                + " that is added to the login refresh thread's sleep time. Legal values are between 0 and 0.25 (25%) inclusive;"
                + " a default value of 0.05 (5%) is used if no value is specified."
                + OAUTHBEARER_NOTE)

    const val DEFAULT_LOGIN_REFRESH_WINDOW_JITTER = 0.05

    const val SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS = "sasl.login.refresh.min.period.seconds"

    const val SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC =
        ("The desired minimum time for the login refresh thread to wait before refreshing a credential,"
                + " in seconds. Legal values are between 0 and 900 (15 minutes); a default value of 60 (1 minute) is used if no value is specified. This value and "
                + " sasl.login.refresh.buffer.seconds are both ignored if their sum exceeds the remaining lifetime of a credential."
                + OAUTHBEARER_NOTE)

    const val DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS: Short = 60

    const val SASL_LOGIN_REFRESH_BUFFER_SECONDS = "sasl.login.refresh.buffer.seconds"

    const val SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC =
        ("The amount of buffer time before credential expiration to maintain when refreshing a credential,"
                + " in seconds. If a refresh would otherwise occur closer to expiration than the number of buffer seconds then the refresh will be moved up to maintain"
                + " as much of the buffer time as possible. Legal values are between 0 and 3600 (1 hour); a default value of  300 (5 minutes) is used if no value is specified."
                + " This value and sasl.login.refresh.min.period.seconds are both ignored if their sum exceeds the remaining lifetime of a credential."
                + OAUTHBEARER_NOTE)

    const val DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS: Short = 300

    const val SASL_LOGIN_CONNECT_TIMEOUT_MS = "sasl.login.connect.timeout.ms"

    const val SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC =
        ("The (optional) value in milliseconds for the external authentication provider connection timeout."
                + OAUTHBEARER_NOTE)

    const val SASL_LOGIN_READ_TIMEOUT_MS = "sasl.login.read.timeout.ms"

    const val SASL_LOGIN_READ_TIMEOUT_MS_DOC =
        ("The (optional) value in milliseconds for the external authentication provider read timeout."
                + OAUTHBEARER_NOTE)
    private const val LOGIN_EXPONENTIAL_BACKOFF_NOTE =
        (" Login uses an exponential backoff algorithm with an initial wait based on the"
                + " sasl.login.retry.backoff.ms setting and will double in wait length between attempts up to a maximum wait length specified by the"
                + " sasl.login.retry.backoff.max.ms setting."
                + OAUTHBEARER_NOTE)

    const val SASL_LOGIN_RETRY_BACKOFF_MAX_MS = "sasl.login.retry.backoff.max.ms"

    const val DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS: Long = 10000

    const val SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC =
        ("The (optional) value in milliseconds for the maximum wait between login attempts to the"
                + " external authentication provider."
                + LOGIN_EXPONENTIAL_BACKOFF_NOTE)

    const val SASL_LOGIN_RETRY_BACKOFF_MS = "sasl.login.retry.backoff.ms"

    const val DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS: Long = 100

    const val SASL_LOGIN_RETRY_BACKOFF_MS_DOC =
        ("The (optional) value in milliseconds for the initial wait between login attempts to the external"
                + " authentication provider."
                + LOGIN_EXPONENTIAL_BACKOFF_NOTE)

    const val SASL_OAUTHBEARER_SCOPE_CLAIM_NAME = "sasl.oauthbearer.scope.claim.name"

    const val DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME = "scope"

    const val SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC =
        ("The OAuth claim for the scope is often named \"" + DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME + "\", but this (optional)"
                + " setting can provide a different name to use for the scope included in the JWT payload's claims if the OAuth/OIDC provider uses a different"
                + " name for that claim.")

    const val SASL_OAUTHBEARER_SUB_CLAIM_NAME = "sasl.oauthbearer.sub.claim.name"

    const val DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME = "sub"

    const val SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC =
        ("The OAuth claim for the subject is often named \"" + DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME + "\", but this (optional)"
                + " setting can provide a different name to use for the subject included in the JWT payload's claims if the OAuth/OIDC provider uses a different"
                + " name for that claim.")

    const val SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url"

    const val SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC =
        ("The URL for the OAuth/OIDC identity provider. If the URL is HTTP(S)-based, it is the issuer's token"
                + " endpoint URL to which requests will be made to login based on the configuration in " + SASL_JAAS_CONFIG + ". If the URL is file-based, it"
                + " specifies a file containing an access token (in JWT serialized form) issued by the OAuth/OIDC identity provider to use for authorization.")

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_URL = "sasl.oauthbearer.jwks.endpoint.url"

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC =
        ("The OAuth/OIDC provider URL from which the provider's"
                + " <a href=\"https://datatracker.ietf.org/doc/html/rfc7517#section-5\">JWKS (JSON Web Key Set)</a> can be retrieved. The URL can be HTTP(S)-based or file-based."
                + " If the URL is HTTP(S)-based, the JWKS data will be retrieved from the OAuth/OIDC provider via the configured URL on broker startup. All then-current"
                + " keys will be cached on the broker for incoming requests. If an authentication request is received for a JWT that includes a \"kid\" header claim value that"
                + " isn't yet in the cache, the JWKS endpoint will be queried again on demand. However, the broker polls the URL every sasl.oauthbearer.jwks.endpoint.refresh.ms"
                + " milliseconds to refresh the cache with any forthcoming keys before any JWT requests that include them are received."
                + " If the URL is file-based, the broker will load the JWKS file from a configured location on startup. In the event that the JWT includes a \"kid\" header"
                + " value that isn't in the JWKS file, the broker will reject the JWT and authentication will fail.")

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS =
        "sasl.oauthbearer.jwks.endpoint.refresh.ms"

    const val DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS = (60 * 60 * 1000).toLong()

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC =
        ("The (optional) value in milliseconds for the broker to wait between refreshing its JWKS (JSON Web Key Set)"
                + " cache that contains the keys to verify the signature of the JWT.")

    private const val JWKS_EXPONENTIAL_BACKOFF_NOTE =
        (" JWKS retrieval uses an exponential backoff algorithm with an initial wait based on the"
                + " sasl.oauthbearer.jwks.endpoint.retry.backoff.ms setting and will double in wait length between attempts up to a maximum wait length specified by the"
                + " sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms setting.")

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS =
        "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms"

    const val DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS: Long = 10000

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC =
        ("The (optional) value in milliseconds for the maximum wait between attempts to retrieve the JWKS (JSON Web Key Set)"
                + " from the external authentication provider."
                + JWKS_EXPONENTIAL_BACKOFF_NOTE)

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS =
        "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms"

    const val DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS: Long = 100

    const val SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC =
        ("The (optional) value in milliseconds for the initial wait between JWKS (JSON Web Key Set) retrieval attempts from the external"
                + " authentication provider."
                + JWKS_EXPONENTIAL_BACKOFF_NOTE)

    const val SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS = "sasl.oauthbearer.clock.skew.seconds"

    const val DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS = 30

    const val SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC =
        ("The (optional) value in seconds to allow for differences between the time of the OAuth/OIDC identity provider and"
                + " the broker.")

    const val SASL_OAUTHBEARER_EXPECTED_AUDIENCE = "sasl.oauthbearer.expected.audience"

    const val SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC =
        ("The (optional) comma-delimited setting for the broker to use to verify that the JWT was issued for one of the"
                + " expected audiences. The JWT will be inspected for the standard OAuth \"aud\" claim and if this value is set, the broker will match the value from JWT's \"aud\" claim "
                + " to see if there is an exact match. If there is no match, the broker will reject the JWT and authentication will fail.")

    const val SASL_OAUTHBEARER_EXPECTED_ISSUER = "sasl.oauthbearer.expected.issuer"

    const val SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC =
        ("The (optional) setting for the broker to use to verify that the JWT was created by the expected issuer. The JWT will"
                + " be inspected for the standard OAuth \"iss\" claim and if this value is set, the broker will match it exactly against what is in the JWT's \"iss\" claim. If there is no"
                + " match, the broker will reject the JWT and authentication will fail.")

    fun addClientSaslSupport(config: ConfigDef) {
        config.define(
            name = SASL_KERBEROS_SERVICE_NAME,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_KERBEROS_SERVICE_NAME_DOC
        ).define(
            name = SASL_KERBEROS_KINIT_CMD,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_KERBEROS_KINIT_CMD,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_KERBEROS_KINIT_CMD_DOC
        ).define(
            name = SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
            type = ConfigDef.Type.DOUBLE,
            defaultValue = DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC
        ).define(
            name = SASL_KERBEROS_TICKET_RENEW_JITTER,
            type = ConfigDef.Type.DOUBLE,
            defaultValue = DEFAULT_KERBEROS_TICKET_RENEW_JITTER,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_KERBEROS_TICKET_RENEW_JITTER_DOC
        ).define(
            name = SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
            type = ConfigDef.Type.LONG,
            defaultValue = DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC
        ).define(
            name = SASL_LOGIN_REFRESH_WINDOW_FACTOR,
            type = ConfigDef.Type.DOUBLE,
            defaultValue = DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR,
            validator = ConfigDef.Range.between(0.5, 1.0),
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
        ).define(
            name = SASL_LOGIN_REFRESH_WINDOW_JITTER,
            type = ConfigDef.Type.DOUBLE,
            defaultValue = DEFAULT_LOGIN_REFRESH_WINDOW_JITTER,
            validator = ConfigDef.Range.between(0.0, 0.25),
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
        ).define(
            name = SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
            type = ConfigDef.Type.SHORT,
            defaultValue = DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
            validator = ConfigDef.Range.between(0, 900),
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
        ).define(
            name = SASL_LOGIN_REFRESH_BUFFER_SECONDS,
            type = ConfigDef.Type.SHORT,
            defaultValue = DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS,
            validator = ConfigDef.Range.between(0, 3600),
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC
        ).define(
            name = SASL_MECHANISM,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SASL_MECHANISM,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_MECHANISM_DOC
        ).define(
            name = SASL_JAAS_CONFIG,
            type = ConfigDef.Type.PASSWORD,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_JAAS_CONFIG_DOC
        ).define(
            name = SASL_CLIENT_CALLBACK_HANDLER_CLASS,
            type = ConfigDef.Type.CLASS,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC
        ).define(
            name = SASL_LOGIN_CALLBACK_HANDLER_CLASS,
            type = ConfigDef.Type.CLASS,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC
        ).define(
            name = SASL_LOGIN_CLASS,
            type = ConfigDef.Type.CLASS,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_LOGIN_CLASS_DOC
        ).define(
            name = SASL_LOGIN_CONNECT_TIMEOUT_MS,
            type = ConfigDef.Type.INT,
            defaultValue = null,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC
        ).define(
            name = SASL_LOGIN_READ_TIMEOUT_MS,
            type = ConfigDef.Type.INT,
            defaultValue = null,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_READ_TIMEOUT_MS_DOC
        ).define(
            name = SASL_LOGIN_RETRY_BACKOFF_MAX_MS,
            type = ConfigDef.Type.LONG,
            defaultValue = DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC
        ).define(
            name = SASL_LOGIN_RETRY_BACKOFF_MS,
            type = ConfigDef.Type.LONG,
            defaultValue = DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_LOGIN_RETRY_BACKOFF_MS_DOC
        ).define(
            name = SASL_OAUTHBEARER_SCOPE_CLAIM_NAME,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC
        ).define(
            name = SASL_OAUTHBEARER_SUB_CLAIM_NAME,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC
        ).define(
            name = SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC
        ).define(
            name = SASL_OAUTHBEARER_JWKS_ENDPOINT_URL,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC
        ).define(
            name = SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
            type = ConfigDef.Type.LONG,
            defaultValue = DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC
        ).define(
            name = SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
            type = ConfigDef.Type.LONG,
            defaultValue = DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC
        ).define(
            name = SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
            type = ConfigDef.Type.LONG,
            defaultValue = DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC
        ).define(
            name = SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS,
            type = ConfigDef.Type.INT,
            defaultValue = DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC
        ).define(
            name = SASL_OAUTHBEARER_EXPECTED_AUDIENCE,
            type = ConfigDef.Type.LIST,
            defaultValue = null,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC
        ).define(
            name = SASL_OAUTHBEARER_EXPECTED_ISSUER,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.LOW,
            documentation = SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC
        )
    }
}
