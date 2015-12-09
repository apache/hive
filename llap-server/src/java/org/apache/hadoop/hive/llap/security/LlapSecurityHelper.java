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
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenRequestProto;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/** Individual instances of this class are not thread safe. */
public class LlapSecurityHelper implements LlapTokenProvider {
  private static final Logger LOG = LoggerFactory.getLogger(LlapSecurityHelper.class);

  private UserGroupInformation llapUgi;

  private final LlapRegistryService registry;
  private ServiceInstanceSet activeInstances;
  private final Configuration conf;
  private LlapManagementProtocolClientImpl client;

  private final SocketFactory socketFactory;
  private final RetryPolicy retryPolicy;

  public LlapSecurityHelper(Configuration conf) {
    this.conf = conf;
    registry = new LlapRegistryService(false);
    registry.init(conf);
    socketFactory = NetUtils.getDefaultSocketFactory(conf);
    retryPolicy = RetryPolicies.retryUpToMaximumTimeWithFixedSleep(
        16000, 2000l, TimeUnit.MILLISECONDS);
  }

  public static UserGroupInformation loginWithKerberos(
      String principal, String keytabFile) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return null;
    if (principal.isEmpty() || keytabFile.isEmpty()) {
      throw new RuntimeException("Kerberos principal and/or keytab are empty");
    }
    LOG.info("Logging in as " + principal + " via " + keytabFile);
    UserGroupInformation.loginUserFromKeytab(
        SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keytabFile);
    return UserGroupInformation.getLoginUser();
  }

  @Override
  public Token<LlapTokenIdentifier> getDelegationToken() throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return null;
    if (llapUgi == null) {
      llapUgi = UserGroupInformation.getCurrentUser();
      // We could have also added keytab support; right now client must do smth like kinit.
    }
    Iterator<ServiceInstance> llaps = null;
    ServiceInstance someLlap = null;
    if (client == null) {
      llaps = getLlapServices(false);
      someLlap = llaps.next();
    }

    ByteString tokenBytes = null;
    boolean hasRefreshed = false;
    while (true) {
      try {
        tokenBytes = getTokenBytes(someLlap);
        break;
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      } catch (IOException ex) {
        LOG.error("Cannot get a token, trying a different instance", ex);
        client = null;
      }
      if (llaps == null || !llaps.hasNext()) {
        if (hasRefreshed) { // Only refresh once.
          throw new RuntimeException("Cannot find any LLAPs to get the token from");
        }
        llaps = getLlapServices(true);
        hasRefreshed = true;
      }
      someLlap = llaps.next();
    }

    // Stupid protobuf byte-buffer reinvention.
    Token<LlapTokenIdentifier> token = new Token<>();
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(tokenBytes.asReadOnlyByteBuffer());
    token.readFields(in);
    return token;
  }

  private ByteString getTokenBytes(
      final ServiceInstance si) throws InterruptedException, IOException {
    return llapUgi.doAs(new PrivilegedExceptionAction<ByteString>() {
      @Override
      public ByteString run() throws Exception {
        if (client == null) {
          client = new LlapManagementProtocolClientImpl(
            conf, si.getHost(), si.getManagementPort(), retryPolicy, socketFactory);
        }
        // Client only connects on the first call, so this has to be done in doAs.
        GetTokenRequestProto req = GetTokenRequestProto.newBuilder().build();
        return client.getDelegationToken(null, req).getToken();
      }
    });
  }

  private Iterator<ServiceInstance> getLlapServices(boolean doForceRefresh) throws IOException {
    if (activeInstances == null) {
      registry.start();
      activeInstances = registry.getInstances();
    }
    Map<String, ServiceInstance> daemons = activeInstances.getAll();
    if (doForceRefresh || daemons == null || daemons.isEmpty()) {
      activeInstances.refresh();
      daemons = activeInstances.getAll();
      if (daemons == null || daemons.isEmpty()) throw new RuntimeException("No LLAPs found");
    }
    return daemons.values().iterator();
  }
}
