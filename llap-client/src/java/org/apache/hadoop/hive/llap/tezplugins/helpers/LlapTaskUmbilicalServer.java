/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.tezplugins.helpers;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.Validator.RangeValidator;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTaskUmbilicalServer {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskUmbilicalServer.class);

  protected volatile Server server;
  private final InetSocketAddress address;
  private final AtomicBoolean started = new AtomicBoolean(true);
  private JobTokenSecretManager jobTokenSecretManager;
  private Map<String, int[]> tokenRefMap = new HashMap<String, int[]>();

  public LlapTaskUmbilicalServer(Configuration conf, LlapTaskUmbilicalProtocol umbilical, int numHandlers) throws IOException {
    jobTokenSecretManager = new JobTokenSecretManager();

    String[] portRange =
        conf.get(HiveConf.ConfVars.LLAP_TASK_UMBILICAL_SERVER_PORT.varname)
            .split("-");
    boolean isHadoopSecurityAuthorizationEnabled = conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false);

    int minPort = Integer.parseInt(portRange[0]);
    boolean portFound = false;
    IOException e = null;
    if (portRange.length == 1) {
      // Single port specified, not Range.
      startServer(conf, umbilical, numHandlers, minPort,
          isHadoopSecurityAuthorizationEnabled);
      portFound = true;
      LOG.info("Successfully bound to port {}", minPort);
    } else {
      int maxPort = Integer.parseInt(portRange[1]);
      // Validate the range specified is valid. i.e the ports lie between
      // 1024 and 65535.
      validatePortRange(portRange[0], portRange[1]);

      for (int i = minPort; i < maxPort; i++) {
        try {
          startServer(conf, umbilical, numHandlers, i,
              isHadoopSecurityAuthorizationEnabled);
          portFound = true;
          LOG.info("Successfully bound to port {}", i);
          break;
        } catch (BindException be) {
          // Ignore and move ahead, in search of a free port.
          LOG.warn("Unable to bind to port {}", i, be);
          e = be;
        }
      }
    }
    if (!portFound) {
      throw e;
    }

    this.address = NetUtils.getConnectAddress(server);
    LOG.info(
        "Started TaskUmbilicalServer: " + umbilical.getClass().getName() + " at address: " + address +
            " with numHandlers=" + numHandlers);
  }

  private void validatePortRange(String minPort, String maxPort)
      throws IOException {
    RangeValidator rangeValidator = new RangeValidator(1024L, 65535L);
    String valMin = rangeValidator.validate(minPort);
    String valMax = rangeValidator.validate(maxPort);
    if (valMin == null & valMax == null) {
      throw new IOException("Invalid minimum range value: " + minPort + " and "
          + "maximum range value: " + maxPort + " for "
          + HiveConf.ConfVars.LLAP_TASK_UMBILICAL_SERVER_PORT.varname
          + ". The value should be between 1024 and 65535.");
    }
    if (valMin != null) {
      throw new IOException("Invalid minimum range value :" + minPort + " for "
          + HiveConf.ConfVars.LLAP_TASK_UMBILICAL_SERVER_PORT.varname
          + ". The value should be between 1024 and 65535.");
    }
    if (valMax != null) {
      throw new IOException("Invalid maximum range value:" + maxPort + " for "
          + HiveConf.ConfVars.LLAP_TASK_UMBILICAL_SERVER_PORT.varname
          + ". The value should be between 1024 and 65535.");
    }
  }

  private void startServer(Configuration conf,
      LlapTaskUmbilicalProtocol umbilical, int numHandlers, int port,
      boolean isHadoopSecurityAuthorizationEnabled) throws IOException {
    server = new RPC.Builder(conf).setProtocol(LlapTaskUmbilicalProtocol.class)
        .setBindAddress("0.0.0.0").setPort(port).setInstance(umbilical)
        .setNumHandlers(numHandlers).setSecretManager(jobTokenSecretManager)
        .build();
    if (isHadoopSecurityAuthorizationEnabled) {
      server.refreshServiceAcl(conf, new LlapUmbilicalExternalPolicyProvider());
    }
    server.start();
  }

  public InetSocketAddress getAddress() {
    return this.address;
  }

  public int getNumOpenConnections() {
    return server.getNumOpenConnections();
  }

  public synchronized void addTokenForJob(String tokenIdentifier, Token<JobTokenIdentifier> token) {
    // Maintain count of outstanding requests for tokenIdentifier.
    int[] refCount = tokenRefMap.get(tokenIdentifier);
    if (refCount == null) {
      refCount = new int[] { 0 };
      tokenRefMap.put(tokenIdentifier, refCount);
      // Should only need to insert the token the first time.
      jobTokenSecretManager.addTokenForJob(tokenIdentifier, token);
    }
    refCount[0]++;
  }

  public synchronized void removeTokenForJob(String tokenIdentifier) {
    // Maintain count of outstanding requests for tokenIdentifier.
    // If count goes to 0, it is safe to remove the token.
    int[] refCount = tokenRefMap.get(tokenIdentifier);
    if (refCount == null) {
      LOG.warn("No refCount found for tokenIdentifier " + tokenIdentifier);
    } else {
      refCount[0]--;
      if (refCount[0] <= 0) {
        tokenRefMap.remove(tokenIdentifier);
        jobTokenSecretManager.removeTokenForJob(tokenIdentifier);
      }
    }
  }

  public void shutdownServer() {
    if (started.get()) { // Primarily to avoid multiple shutdowns.
      started.set(false);
      server.stop();
    }
  }

  public static class LlapUmbilicalExternalPolicyProvider extends PolicyProvider {

    private static final Service[] services = {
      new Service(
          MRJobConfig.MR_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL,
          LlapTaskUmbilicalProtocol.class)
    };

    @Override
    public Service[] getServices() {
      return services.clone();
    }
  }
}
