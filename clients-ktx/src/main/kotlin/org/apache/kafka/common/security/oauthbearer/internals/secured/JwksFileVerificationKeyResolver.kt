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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import java.io.IOException
import java.nio.file.Path
import java.security.Key
import org.apache.kafka.common.utils.Utils.readFileAsString
import org.jose4j.jwk.JsonWebKeySet
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwx.JsonWebStructure
import org.jose4j.keys.resolvers.JwksVerificationKeyResolver
import org.jose4j.keys.resolvers.VerificationKeyResolver
import org.jose4j.lang.JoseException
import org.jose4j.lang.UnresolvableKeyException
import org.slf4j.LoggerFactory

/**
 * `JwksFileVerificationKeyResolver` is a [VerificationKeyResolver] implementation that will load
 * the JWKS from the given file system directory.
 *
 * A [JWKS (JSON Web Key Set)](https://datatracker.ietf.org/doc/html/rfc7517#section-5) is a JSON
 * document provided by the OAuth/OIDC provider that lists the keys used to sign the JWTs it issues.
 *
 * Here is a sample JWKS JSON document:
 *
 * ```json
 * {
 *   "keys": [
 *     {
 *       "kty": "RSA",
 *       "alg": "RS256",
 *       "kid": "abc123",
 *       "use": "sig",
 *       "e": "AQAB",
 *       "n": "..."
 *     },
 *     {
 *       "kty": "RSA",
 *       "alg": "RS256",
 *       "kid": "def456",
 *       "use": "sig",
 *       "e": "AQAB",
 *       "n": "..."
 *     }
 *   ]
 * }
 * ```
 *
 * Without going into too much detail, the array of keys enumerates the key data that the provider
 * is using to sign the JWT. The key ID (`kid`) is referenced by the JWT's header in order to match
 * up the JWT's signing key with the key in the JWKS. During the validation step of the broker, the
 * jose4j OAuth library will use the contents of the appropriate key in the JWKS to validate the
 * signature.
 *
 * Given that the JWKS is referenced by the JWT, the JWKS must be made available by the OAuth/OIDC
 * provider so that a JWT can be validated.
 *
 * @see org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
 * @see VerificationKeyResolver
 */
class JwksFileVerificationKeyResolver(
    private val jwksFile: Path,
) : CloseableVerificationKeyResolver {

    private var delegate: VerificationKeyResolver? = null

    @Throws(IOException::class)
    override fun init() {
        log.debug(
            "Starting creation of new VerificationKeyResolver from {}",
            jwksFile
        )
        val json = readFileAsString(jwksFile.toFile().path)
        val jwks: JsonWebKeySet = try {
            JsonWebKeySet(json)
        } catch (e: JoseException) {
            throw IOException(e)
        }
        delegate = JwksVerificationKeyResolver(jwks.jsonWebKeys)
    }

    @Throws(UnresolvableKeyException::class)
    override fun resolveKey(jws: JsonWebSignature, nestingContext: List<JsonWebStructure>): Key {
        return delegate?.resolveKey(jws, nestingContext) ?: throw UnresolvableKeyException(
            "VerificationKeyResolver delegate is null; please call init() first"
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(
            JwksFileVerificationKeyResolver::class.java
        )
    }
}
