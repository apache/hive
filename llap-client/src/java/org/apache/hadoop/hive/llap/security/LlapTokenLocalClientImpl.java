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

package org.apache.hadoop.hive.llap.security;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTokenLocalClientImpl implements LlapTokenLocalClient {
  private static final Logger LOG = LoggerFactory.getLogger(LlapTokenLocalClientImpl.class);
  private final SecretManager secretManager;

  public LlapTokenLocalClientImpl(Configuration conf, String clusterId) {
    // TODO: create this centrally in HS2 case
    secretManager = SecretManager.createSecretManager(conf, clusterId);
  }

  @Override
  public Token<LlapTokenIdentifier> createToken(
      String appId, String user, boolean isSignatureRequired) throws IOException {
    try {
      Token<LlapTokenIdentifier> token = secretManager.createLlapToken(
          appId, user, isSignatureRequired);
      if (LOG.isInfoEnabled()) {
        LOG.info("Created a LLAP delegation token locally: " + token);
      }
      return token;
    } catch (Exception ex) {
      throw (ex instanceof IOException) ? (IOException)ex : new IOException(ex);
    }
  }

  @Override
  public void close() {
    try {
      secretManager.stopThreads();
    } catch (Exception ex) {
      // Ignore.
    }
  }
}
