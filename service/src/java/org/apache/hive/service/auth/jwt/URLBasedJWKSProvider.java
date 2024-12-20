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
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import javax.security.sasl.AuthenticationException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
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

  public URLBasedJWKSProvider(HiveConf conf) throws IOException, ParseException, GeneralSecurityException {
    this.conf = conf;
    loadJWKSets();
  }

  /**
   * Fetches the JWKS and stores into memory. The JWKS are expected to be in the standard form as defined here -
   * https://datatracker.ietf.org/doc/html/rfc7517#appendix-A.
   */
  private void loadJWKSets() throws IOException, ParseException, GeneralSecurityException {
    String jwksURL = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION_JWT_JWKS_URL);
    if (jwksURL == null || jwksURL.isEmpty()) {
      throw new IOException("Invalid value of property: " + 
          HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION_JWT_JWKS_URL.varname);
    }
    String[] jwksURLs = jwksURL.split(",");
    for (String urlString : jwksURLs) {
      SSLContext context = null;
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION_JWT_JWKS_SKIP_SSL_CERT, false)) {
        context = SSLContext.getInstance("TLS");
        X509TrustManager trustAllManager = new X509TrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] chain, String authType)
              throws CertificateException {
          }
          @Override
          public void checkServerTrusted(X509Certificate[] chain, String authType)
              throws CertificateException {
          }
          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }
        };
        context.init(null, new X509TrustManager[]{trustAllManager}, new SecureRandom());
      }
      HttpGet get = new HttpGet(urlString);
      try (CloseableHttpClient httpClient = HttpClients.custom().setSSLContext(context).build();
          CloseableHttpResponse response = httpClient.execute(get)) {
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          jwkSets.add(JWKSet.load(entity.getContent()));
        }
      }
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
