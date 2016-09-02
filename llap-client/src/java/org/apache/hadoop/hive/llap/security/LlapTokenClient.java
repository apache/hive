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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenRequestProto;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

public class LlapTokenClient {
  private static final Logger LOG = LoggerFactory.getLogger(LlapTokenClient.class);

  private final LlapRegistryService registry;
  private final SocketFactory socketFactory;
  private final RetryPolicy retryPolicy;
  private final Configuration conf;
  private ServiceInstanceSet activeInstances;
  private Collection<ServiceInstance> lastKnownInstances;
  private LlapManagementProtocolClientImpl client;
  private ServiceInstance clientInstance;

  public LlapTokenClient(Configuration conf) {
    this.conf = conf;
    registry = new LlapRegistryService(false);
    registry.init(conf);
    socketFactory = NetUtils.getDefaultSocketFactory(conf);
    retryPolicy = RetryPolicies.retryUpToMaximumTimeWithFixedSleep(
        16000, 2000l, TimeUnit.MILLISECONDS);
  }

  public Token<LlapTokenIdentifier> getDelegationToken(String appId) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return null;
    Iterator<ServiceInstance> llaps = null;
    if (clientInstance == null) {
      assert client == null;
      llaps = getLlapServices(false).iterator();
      clientInstance = llaps.next();
    }

    ByteString tokenBytes = null;
    boolean hasRefreshed = false;
    while (true) {
      try {
        tokenBytes = getTokenBytes(appId);
        break;
      } catch (IOException | ServiceException ex) {
        LOG.error("Cannot get a token, trying a different instance", ex);
        client = null;
        clientInstance = null;
      }
      if (llaps == null || !llaps.hasNext()) {
        if (hasRefreshed) { // Only refresh once.
          throw new RuntimeException("Cannot find any LLAPs to get the token from");
        }
        llaps = getLlapServices(true).iterator();
        hasRefreshed = true;
      }
      clientInstance = llaps.next();
    }

    Token<LlapTokenIdentifier> token = extractToken(tokenBytes);
    if (LOG.isInfoEnabled()) {
      LOG.info("Obtained a LLAP delegation token from " + clientInstance + ": " + token);
    }
    return token;
  }

  private Token<LlapTokenIdentifier> extractToken(ByteString tokenBytes) throws IOException {
    Token<LlapTokenIdentifier> token = new Token<>();
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(tokenBytes.asReadOnlyByteBuffer());
    token.readFields(in);
    return token;
  }

  private ByteString getTokenBytes(final String appId) throws IOException, ServiceException {
    assert clientInstance != null;
    if (client == null) {
      client = new LlapManagementProtocolClientImpl(conf, clientInstance.getHost(),
          clientInstance.getManagementPort(), retryPolicy, socketFactory);
    }
    GetTokenRequestProto.Builder req = GetTokenRequestProto.newBuilder();
    if (!StringUtils.isBlank(appId)) {
      req.setAppId(appId);
    }
    return client.getDelegationToken(null, req.build()).getToken();
  }

  /** Synchronized - LLAP registry and instance set are not thread safe. */
  private synchronized List<ServiceInstance> getLlapServices(
      boolean doForceRefresh) throws IOException {
    if (!doForceRefresh && lastKnownInstances != null) {
      return new ArrayList<>(lastKnownInstances);
    }
    if (activeInstances == null) {
      registry.start();
      activeInstances = registry.getInstances();
    }
    Collection<ServiceInstance> daemons = activeInstances.getAll();
    if (daemons == null || daemons.isEmpty()) {
      throw new RuntimeException("No LLAPs found");
    }
    lastKnownInstances = daemons;
    return new ArrayList<ServiceInstance>(lastKnownInstances);
  }

}
