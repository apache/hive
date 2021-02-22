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

package org.apache.hive.service.auth.saml;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.util.generator.ValueGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relay state generator for the SAML Request which includes the port number from the
 * request header. This port number is used eventually to redirect the token to the
 * localhost:port from the browser.
 */
public class HiveSamlRelayStateStore implements ValueGenerator {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveSamlRelayStateStore.class);
  private final Cache<String, HiveSamlRelayStateInfo> relayStateCache =
      CacheBuilder.newBuilder()
          //TODO make this configurable
          .expireAfterWrite(5, TimeUnit.MINUTES)
          .build();

  private static final HiveSamlRelayStateStore INSTANCE = new HiveSamlRelayStateStore();

  private HiveSamlRelayStateStore() {
  }

  public static HiveSamlRelayStateStore get() {
    return INSTANCE;
  }

  /**
   * This method is used to generate the RelayState for a given SAML authentication
   * request. It generates a UUID which is added to the SAMl authentication request URL
   * as a query parameter RelayState. This RelayState is resent by the IDP later as per
   * SAML specification and it is used to extract the state associated with the given
   * SAML authentication request. It is important to note that the RelayState identifier
   * itself should not contain any secrets since it can be seen by anyone from the URL.
   * @return RelayState identifier to be used as value of the query parameter RelayState
   * in the SAML authentication request redirect URL.
   */
  @Override
  public String generateValue(WebContext webContext) {
    Optional<String> portNumber = webContext
        .getRequestHeader(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT);
    if (!portNumber.isPresent()) {
      throw new RuntimeException(
          "SAML response port header " + HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT
              + " is not set ");
    }
    int port = Integer.parseInt(portNumber.get());
    String relayState = UUID.randomUUID().toString();
    // note that clientIdentifier below is a different UUID than relayState.
    // relayState should not contain any secrets since it is visible on the URL string.
    HiveSamlRelayStateInfo relayStateInfo = new HiveSamlRelayStateInfo(port,
        UUID.randomUUID().toString());
    webContext.setResponseHeader(HiveSamlUtils.SSO_CLIENT_IDENTIFIER,
        relayStateInfo.getClientIdentifier());
    relayStateCache.put(relayState, relayStateInfo);
    return relayState;
  }

  /**
   * Get the RelayState from the {@link HttpServletRequest}.
   * @throws HttpSamlAuthenticationException If RelayState parameter is not available
   * in the request.
   */
  public String getRelayStateInfo(HttpServletRequest request,
      HttpServletResponse response)
      throws HttpSamlAuthenticationException {
    String relayState = request.getParameter("RelayState");
    if (relayState == null) {
      throw new HttpSamlAuthenticationException(
          "Could not get the RelayState from the SAML response");
    }
    return relayState;
  }

  /**
   * Given a RelayStateIdentifier, return the {@link HiveSamlRelayStateInfo}
   * @param relayStateId
   * @return
   * @throws HttpSamlAuthenticationException if there is no {@link HiveSamlRelayStateInfo}
   * for the given relayStateIdentifier. This could happen if there is a unknown relay
   * state from the request or if the IDP responded too late and the cached RelayState
   * was evicted.
   */
  public HiveSamlRelayStateInfo getRelayStateInfo(String relayStateId)
      throws HttpSamlAuthenticationException {
    HiveSamlRelayStateInfo relayStateInfo = relayStateCache.getIfPresent(relayStateId);
    if (relayStateInfo == null) {
      throw new HttpSamlAuthenticationException(
          "Invalid value of relay state received: " + relayStateId);
    }
    return relayStateInfo;
  }

  /**
   * Given a relayState and a client identifier, this method makes sure that the relay
   * state exists and that the client identifier matches with the relay state. Note that
   * this is one-time use only. Once successfully validated, the same relayState and
   * client identifier cannot be validated again. This is mainly used to make sure that
   * the SAML token issued by the Hiveserver2 is only used once.
   */
  public synchronized boolean validateClientIdentifier(String relayStateKey,
      String clientIdentifier) {
    HiveSamlRelayStateInfo relayStateInfo = relayStateCache.getIfPresent(relayStateKey);
    if (relayStateInfo == null) {
      return false;
    }
    relayStateCache.invalidate(relayStateKey);
    LOG.debug("Validating client identifier {} with {}", clientIdentifier,
        relayStateInfo.getClientIdentifier());
    return String.valueOf(relayStateInfo.getClientIdentifier()).equals(clientIdentifier);
  }
}
