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

import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.core.exception.http.WithLocationAction;
import org.pac4j.saml.client.SAML2Client;
import org.pac4j.saml.config.SAML2Configuration;
import org.pac4j.saml.credentials.SAML2Credentials;
import org.pac4j.saml.credentials.extractor.SAML2CredentialsExtractor;
import org.pac4j.saml.replay.ReplayCacheProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveServer2's implementation of SAML2Client. We mostly rely on pac4j to do most of the
 * heavy lifting. This class implements the initialization logic of the underlying {@link
 * SAML2Client} using the HiveConf. Also, implements the generation of SAML requests using
 * HTTP-Redirect binding. //TODO: Add support for HTTP-Post binding for SAML request.
 *
 * //TODO Note that this implementation is only to be used when HiveServer2 is not
 * deployed in HA mode. In case of HA mode, we should implement a
 * {@link ReplayCacheProvider} such that the {@link org.opensaml.storage.ReplayCache} is
 * shared between all HS2 instances. Ref: http://www.pac4j.org/docs/clients/saml.html
 */
public class HiveSaml2Client extends SAML2Client {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSaml2Client.class);
  private static HiveSaml2Client INSTANCE;
  private final HiveSamlGroupNameFilter groupNameFilter;

  private HiveSaml2Client(HiveConf conf) throws Exception {
    super(getSamlConfig(conf));
    setCallbackUrl(getCallBackUrl(conf));
    setName(HiveSaml2Client.class.getSimpleName());
    setStateGenerator(HiveSamlRelayStateStore.get());
    groupNameFilter = new HiveSamlGroupNameFilter(conf);
    init();
  }

  private static String getCallBackUrl(HiveConf conf) throws Exception {
    URI callbackURI = HiveSamlUtils.getCallBackUri(conf);
    return callbackURI.toString();
  }

  public static synchronized HiveSaml2Client get(HiveConf conf)
      throws HttpSamlAuthenticationException {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    try {
      INSTANCE = new HiveSaml2Client(conf);
    } catch (Exception e) {
      throw new HttpSamlAuthenticationException("Could not instantiate SAML2.0 client",
          e);
    }
    return INSTANCE;
  }

  /**
   * Extracts the SAML specific configuration needed to initialize the SAML2.0 client.
   */
  private static SAML2Configuration getSamlConfig(HiveConf conf) throws Exception {
    // by default the SAML2Client will create the keystore if the keystore path provided
    // doesn't exist. The directory where the keystore path is set must be accessible.
    SAML2Configuration saml2Configuration = new SAML2Configuration(
        conf.get(ConfVars.HIVE_SERVER2_SAML_KEYSTORE_PATH.varname),
        String.valueOf(
            conf.getPassword(ConfVars.HIVE_SERVER2_SAML_KEYSTORE_PASSWORD.varname)),
        String.valueOf(
            conf.getPassword(ConfVars.HIVE_SERVER2_SAML_PRIVATE_KEY_PASSWORD.varname)),
        conf.get(ConfVars.HIVE_SERVER2_SAML_IDP_METADATA.varname));
    saml2Configuration
        .setAuthnRequestBindingType(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
    saml2Configuration.setResponseBindingType(SAML2_POST_BINDING_URI);
    saml2Configuration.setForceAuth(
        conf.getBoolean(ConfVars.HIVE_SERVER2_SAML_FORCE_AUTH.varname, false));
    saml2Configuration.setMaximumAuthenticationLifetime(
        ((int) conf.getTimeVar(ConfVars.HIVE_SERVER2_SAML_AUTHENTICATION_LIFETIME,
            TimeUnit.SECONDS)));
    String acsIndex = conf.get(ConfVars.HIVE_SERVER2_SAML_ACS_INDEX.varname, "");
    if (!acsIndex.isEmpty()) {
      saml2Configuration.setAssertionConsumerServiceIndex(
          conf.getIntVar(ConfVars.HIVE_SERVER2_SAML_ACS_INDEX));
    }
    String disallowedSignatureAlgos = conf
        .get(ConfVars.HIVE_SERVER2_SAML_BLACKLISTED_SIGNATURE_ALGORITHMS.varname,
            "");
    if (!disallowedSignatureAlgos.isEmpty()) {
      LOG.info(
          "List of disallowed signature algorithms: " + disallowedSignatureAlgos);
      List<String> blackListedSignAlgos = Splitter.on(',').splitToList(
          disallowedSignatureAlgos);
      saml2Configuration.setBlackListedSignatureSigningAlgorithms(blackListedSignAlgos);
    }
    // if the SP id is set use it else we configure the SP Id as the callback id.
    // this behavior IDP dependent. E.g. in case of Okta we can explicitly set a
    // different SP id.
    saml2Configuration.setServiceProviderEntityId(
        conf.get(ConfVars.HIVE_SERVER2_SAML_SP_ID.varname, getCallBackUrl(conf)));
    saml2Configuration.setWantsAssertionsSigned(
        conf.getBoolVar(ConfVars.HIVE_SERVER2_SAML_WANT_ASSERTIONS_SIGNED));
    saml2Configuration
        .setAuthnRequestSigned(conf.getBoolVar(ConfVars.HIVE_SERVER2_SAML_SIGN_REQUESTS));
    return saml2Configuration;
  }

  @VisibleForTesting
  public static synchronized void shutdown() {
    INSTANCE = null;
    HiveSamlAuthTokenGenerator.shutdown();
  }

  /**
   * Generates a SAML request using the HTTP-Redirect Binding.
   */
  public void setRedirect(HttpServletRequest request, HttpServletResponse response)
      throws HttpSamlAuthenticationException {
    int responsePort = HiveSamlUtils.validateSamlResponsePort(request);
    LOG.debug("Request has response port set as {}", responsePort);
    Optional<RedirectionAction> redirect = getRedirectionAction(
        new JEEContext(request, response));
    if (!redirect.isPresent()) {
      throw new HttpSamlAuthenticationException("Could not get the redirect response");
    }
    response.setStatus(redirect.get().getCode());
    WithLocationAction locationAction = (WithLocationAction) redirect.get();
    try {
      String location = locationAction.getLocation();
      LOG.debug("Sending a redirect response to location = {}", location);
      response.sendRedirect(locationAction.getLocation());
    } catch (IOException e) {
      throw new HttpSamlAuthenticationException(e);
    }
  }

  /**
   * Given a response which may contain a SAML Assertion, validates it. If the validation
   * is successful, it extracts the nameId from the assertion which is used as the
   * identity of the end user.
   *
   * @param request
   * @param response
   * @return the NameId as received in the assertion if the assertion was valid.
   * @throws HttpSamlAuthenticationException In case the assertion is not present or is
   *                                         invalid.
   */
  public String validate(HttpServletRequest request, HttpServletResponse response)
      throws HttpSamlAuthenticationException {
    Optional<SAML2Credentials> credentials;
    try {
      SAML2CredentialsExtractor credentialsExtractor = new SAML2CredentialsExtractor(
          this);
      credentials = credentialsExtractor
          .extract(new JEEContext(request, response));
    } catch (Exception ex) {
      throw new HttpSamlAuthenticationException("Could not validate the SAML response",
          ex);
    }
    if (!credentials.isPresent()) {
      throw new HttpSamlAuthenticationException("Credentials could not be extracted");
    }
    String nameId = credentials.get().getNameId().getValue();
    if (!groupNameFilter.apply(credentials.get().getAttributes())) {
      LOG.warn("Could not match any groups for the nameid {}", nameId);
      throw new HttpSamlNoGroupsMatchedException(
          "None of the configured groups match for the user");
    }
    return nameId;
  }
}
