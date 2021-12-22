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
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
  public static final String ATLAS_PROPERTY_CONNECT_TIMEOUT_IN_MS = "atlas.client.connectTimeoutMSecs";
  public static final String ATLAS_PROPERTY_READ_TIMEOUT_IN_MS = "atlas.client.readTimeoutMSecs";

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
    return create(conf);
  }

  private AtlasRestClient create(HiveConf conf) throws SemanticException {
    if (baseUrls == null || baseUrls.length == 0) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format("baseUrls is not set.",
        ReplUtils.REPL_ATLAS_SERVICE));
    }
    setUGInfo();
    initializeAtlasApplicationProperties(conf);
    AtlasClientV2 clientV2 = new AtlasClientV2(this.userGroupInformation,
            this.userGroupInformation.getShortUserName(), baseUrls);
    return new AtlasRestClientImpl(clientV2, conf);
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

  private void initializeAtlasApplicationProperties(HiveConf conf) throws SemanticException {
    try {
      Properties props = new Properties();
      props.setProperty(ATLAS_PROPERTY_CONNECT_TIMEOUT_IN_MS, String.valueOf(
              conf.getTimeVar(HiveConf.ConfVars.REPL_EXTERNAL_CLIENT_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS)));
      props.setProperty(ATLAS_PROPERTY_READ_TIMEOUT_IN_MS, String.valueOf(
              conf.getTimeVar(HiveConf.ConfVars.REPL_ATLAS_CLIENT_READ_TIMEOUT, TimeUnit.MILLISECONDS)));
      props.setProperty(ATLAS_PROPERTY_CLIENT_HA_RETRIES_KEY, "1");
      props.setProperty(ATLAS_PROPERTY_CLIENT_HA_SLEEP_INTERVAL_MS_KEY, "0");
      props.setProperty(ATLAS_PROPERTY_REST_ADDRESS, incomingUrl);
      props.setProperty(ATLAS_PROPERTY_AUTH_KERBEROS, "true");
      ApplicationProperties.set(ConfigurationConverter.getConfiguration(props));
    } catch (AtlasException e) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_INTERNAL_CONFIG_FOR_SERVICE.format(e.getMessage(),
        ReplUtils.REPL_ATLAS_SERVICE), e);
    }
  }
}
