/**
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

package org.apache.hadoop.hive.llap.security;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTokenLocalClient {
  private static final Logger LOG = LoggerFactory.getLogger(LlapTokenLocalClient.class);
  private final SecretManager secretManager;

  public LlapTokenLocalClient(Configuration conf, String clusterId) {
    secretManager = SecretManager.createSecretManager(conf, clusterId);
  }

  public Token<LlapTokenIdentifier> createToken(String appId, String user) throws IOException {
    try {
      Token<LlapTokenIdentifier> token = secretManager.createLlapToken(appId, user);
      if (LOG.isInfoEnabled()) {
        LOG.info("Created a LLAP delegation token locally: " + token);
      }
      return token;
    } catch (Exception ex) {
      throw new IOException("Failed to create LLAP token locally. You might need to set "
          + ConfVars.LLAP_CREATE_TOKEN_LOCALLY.varname
          + " to false, or make sure you can access secure ZK paths.", ex);
    }
  }

  public void close() {
    try {
      secretManager.stopThreads();
    } catch (Exception ex) {
      // Ignore.
    }
  }
}
