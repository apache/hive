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

package org.apache.hadoop.hive.llap.coordinator;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.coordinator.LlapCoordinator;
import org.apache.hadoop.hive.llap.security.LlapSigner;
import org.apache.hadoop.hive.llap.security.LlapSignerImpl;
import org.apache.hadoop.hive.llap.security.LlapTokenLocalClient;
import org.apache.hadoop.hive.llap.security.LlapTokenLocalClientImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * The class containing facilities for LLAP interactions in HS2.
 * This may eventually evolve into a central LLAP manager hosted by HS2 or elsewhere.
 * Refactor as needed.
 */
public class LlapCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(LlapCoordinator.class);

  /** We'll keep signers per cluster around for some time, for reuse. */
  private final Cache<String, LlapSigner> signers = CacheBuilder.newBuilder().removalListener(
      new RemovalListener<String, LlapSigner>() {
        @Override
        public void onRemoval(RemovalNotification<String, LlapSigner> notification) {
          if (notification.getValue() != null) {
            notification.getValue().close();
          }
        }
      }).expireAfterAccess(10, TimeUnit.MINUTES).build();

  // TODO: probably temporary before HIVE-13698; after that we may create one per session.
  private static final Cache<String, LlapTokenLocalClient> localClientCache = CacheBuilder
      .newBuilder().expireAfterAccess(10, TimeUnit.MINUTES)
      .removalListener(new RemovalListener<String, LlapTokenLocalClient>() {
        @Override
        public void onRemoval(RemovalNotification<String, LlapTokenLocalClient> notification) {
          if (notification.getValue() != null) {
            notification.getValue().close();
          }
        }
      }).build();

  private HiveConf hiveConf;
  private String clusterUser;
  private long startTime;
  private final AtomicInteger appIdCounter = new AtomicInteger(0);

  LlapCoordinator() {
  }

  private void init(HiveConf hiveConf) throws IOException {
    // Only do the lightweight stuff in ctor; by default, LLAP coordinator is created during
    // HS2 init without the knowledge of LLAP usage (or lack thereof) in the cluster.
    this.hiveConf = hiveConf;
    this.clusterUser = UserGroupInformation.getCurrentUser().getShortUserName();
    // TODO: if two HS2s start at exactly the same time, which could happen during a coordinated
    //       restart, they could start generating the same IDs. Should we store the startTime
    //       somewhere like ZK? Try to randomize it a bit for now...
    long randomBits = (long)(new Random().nextInt()) << 32;
    this.startTime = Math.abs((System.currentTimeMillis() & (long)Integer.MAX_VALUE) | randomBits);
  }

  public LlapSigner getLlapSigner(final Configuration jobConf) {
    // Note that we create the cluster name from user conf (hence, a user can target a cluster),
    // but then we create the signer using hiveConf (hence, we control the ZK config and stuff).
    assert UserGroupInformation.isSecurityEnabled();
    final String clusterId = DaemonId.createClusterString(
        clusterUser, LlapUtil.generateClusterName(jobConf));
    try {
      return signers.get(clusterId, new Callable<LlapSigner>() {
        public LlapSigner call() throws Exception {
          return new LlapSignerImpl(hiveConf, clusterId);
        }
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public ApplicationId createExtClientAppId() {
    // Note that we cannot allow users to provide app ID, since providing somebody else's appId
    // would give one LLAP token (and splits) for that app ID. If we could verify it somehow
    // (YARN token? nothing we can do in an UDF), we could get it from client already running on
    // YARN. As such, the clients running on YARN will have two app IDs to be aware of.
    return ApplicationId.newInstance(startTime, appIdCounter.incrementAndGet());
  }

  public LlapTokenLocalClient getLocalTokenClient(
      final Configuration conf, String clusterUser) throws IOException {
    // Note that we create the cluster name from user conf (hence, a user can target a cluster),
    // but then we create the signer using hiveConf (hence, we control the ZK config and stuff).
    assert UserGroupInformation.isSecurityEnabled();
    String clusterName = LlapUtil.generateClusterName(conf);
    // This assumes that the LLAP cluster and session are both running under HS2 user.
    final String clusterId = DaemonId.createClusterString(clusterUser, clusterName);
    try {
      return localClientCache.get(clusterId, new Callable<LlapTokenLocalClientImpl>() {
        @Override
        public LlapTokenLocalClientImpl call() throws Exception {
          return new LlapTokenLocalClientImpl(hiveConf, clusterId);
        }
      });
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  public void close() {
    try {
      localClientCache.invalidateAll();
      signers.invalidateAll();
      localClientCache.cleanUp();
      signers.cleanUp();
    } catch (Exception ex) {
      LOG.error("Error closing the coordinator; ignoring", ex);
    }
  }

  /** TODO: ideally, when the splits UDF is made a proper API, coordinator should not
   *        be managed as a global. HS2 should create it and then pass it around. */
  private static final LlapCoordinator INSTANCE = new LlapCoordinator();
  public static void initializeInstance(HiveConf hiveConf) throws IOException {
    INSTANCE.init(hiveConf);
  }

  public static LlapCoordinator getInstance() {
    return INSTANCE;
  }
}
