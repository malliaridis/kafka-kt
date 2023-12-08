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

import org.apache.kafka.common.config.SaslConfigs
import org.jose4j.keys.resolvers.VerificationKeyResolver

object AccessTokenValidatorFactory {

    fun create(
        configs: Map<String, *>,
        saslMechanism: String? = null,
    ): AccessTokenValidator {
        val cu = ConfigurationUtils(configs, saslMechanism)
        val scopeClaimName = cu.get<String>(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME)!!
        val subClaimName = cu.get<String>(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME)!!

        return LoginAccessTokenValidator(scopeClaimName, subClaimName)
    }

    fun create(
        configs: Map<String, *>,
        saslMechanism: String? = null,
        verificationKeyResolver: VerificationKeyResolver?
    ): AccessTokenValidator {
        val cu = ConfigurationUtils(configs, saslMechanism)
        val expectedAudiences = cu.get<Collection<String>>(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE)?.toSet()

        val clockSkew = cu.validateOptionalInt(SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS)
        val expectedIssuer = cu.validateOptionalString(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER)
        val scopeClaimName = cu.validateRequiredString(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME)
        val subClaimName = cu.validateRequiredString(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME)

        return ValidatorAccessTokenValidator(
            clockSkew,
            expectedAudiences,
            expectedIssuer,
            verificationKeyResolver,
            scopeClaimName,
            subClaimName
        )
    }
}
