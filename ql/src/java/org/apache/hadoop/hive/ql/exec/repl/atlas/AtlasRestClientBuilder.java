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

package org.apache.hadoop.hive.ql.exec.repl.atlas;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Builder for AtlasRestClient.
 */
public class AtlasRestClientBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(AtlasRestClientBuilder.class);
  private static final String ATLAS_PROPERTY_CLIENT_HA_RETRIES_KEY = "atlas.client.ha.retries";
  private static final String ATLAS_PROPERTY_CLIENT_HA_SLEEP_INTERVAL_MS_KEY = "atlas.client.ha.sleep.interval.ms";
  private static final String ATLAS_PROPERTY_REST_ADDRESS = "atlas.rest.address";
  private static final String ATLAS_PROPERTY_AUTH_KERBEROS = "atlas.authentication.method.kerberos";
  private static final String URL_SEPERATOR = ",";

  private UserGroupInformation userGroupInformation;
  protected String incomingUrl;
  protected String[] baseUrls;

  public AtlasRestClientBuilder(String urls) {
    this.incomingUrl = urls;
    if (urls.contains(URL_SEPERATOR)) {
      this.baseUrls = urls.split(URL_SEPERATOR);
    } else {
      this.baseUrls = new String[]{urls};
    }
  }

  public AtlasRestClient getClient(HiveConf conf) throws SemanticException {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)) {
      return new NoOpAtlasRestClient();
    }
    return create();
  }

  private AtlasRestClient create() throws SemanticException {
    if (baseUrls == null || baseUrls.length == 0) {
      throw new SemanticException("baseUrls is not set.");
    }
    setUGInfo();
    initializeAtlasApplicationProperties();
    AtlasClientV2 clientV2 = new AtlasClientV2(this.userGroupInformation,
            this.userGroupInformation.getShortUserName(), baseUrls);
    return new AtlasRestClientImpl(clientV2);
  }

  private AtlasRestClientBuilder setUGInfo() throws SemanticException {
    try {
      this.userGroupInformation = UserGroupInformation.getLoginUser();
      LOG.info("AuthStrategy: Kerberos : urls: {} : userGroupInformation: {}", baseUrls, userGroupInformation);
    } catch (Exception e) {
      throw new SemanticException("Error: setAuthStrategy: UserGroupInformation.getLoginUser: failed!", e);
    }
    return this;
  }

  private void initializeAtlasApplicationProperties() throws SemanticException {
    try {
      Properties props = new Properties();
      props.setProperty(ATLAS_PROPERTY_CLIENT_HA_RETRIES_KEY, "1");
      props.setProperty(ATLAS_PROPERTY_CLIENT_HA_SLEEP_INTERVAL_MS_KEY, "0");
      props.setProperty(ATLAS_PROPERTY_REST_ADDRESS, incomingUrl);
      props.setProperty(ATLAS_PROPERTY_AUTH_KERBEROS, "true");
      ApplicationProperties.set(ConfigurationConverter.getConfiguration(props));
    } catch (AtlasException e) {
      throw new SemanticException(e);
    }
  }
}
