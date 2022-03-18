/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.auth.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKMatcher;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.JWKSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides a way to get JWKS json. Hive will use this to verify the incoming JWTs.
 */
public class URLBasedJWKSProvider {

  private static final Logger LOG = LoggerFactory.getLogger(URLBasedJWKSProvider.class.getName());
  private final HiveConf conf;
  private List<JWKSet> jwkSets = new ArrayList<>();

  public URLBasedJWKSProvider(HiveConf conf) throws IOException, ParseException {
    this.conf = conf;
    loadJWKSets();
  }

  /**
   * Fetches the JWKS and stores into memory. The JWKS are expected to be in the standard form as defined here -
   * https://datatracker.ietf.org/doc/html/rfc7517#appendix-A.
   */
  private void loadJWKSets() throws IOException, ParseException {
    String jwksURL = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION_JWT_JWKS_URL);
    String[] jwksURLs = jwksURL.split(",");
    for (String urlString : jwksURLs) {
      URL url = new URL(urlString);
      jwkSets.add(JWKSet.load(url));
      LOG.info("Loaded JWKS from " + urlString);
    }
  }

  /**
   * Returns filtered JWKS by one or more criteria, such as kid, typ, alg.
   */
  public List<JWK> getJWKs(JWSHeader header) throws AuthenticationException {
    JWKMatcher matcher = JWKMatcher.forJWSHeader(header);
    if (matcher == null) {
      throw new AuthenticationException("Unsupported algorithm: " + header.getAlgorithm());
    }

    List<JWK> jwks = new ArrayList<>();
    JWKSelector selector = new JWKSelector(matcher);
    for (JWKSet jwkSet : jwkSets) {
      jwks.addAll(selector.select(jwkSet));
    }
    return jwks;
  }
}
