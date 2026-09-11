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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.llap.coordinator;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.security.LlapTokenLocalClient;
import org.apache.hadoop.hive.llap.security.LlapTokenLocalClientImpl;
import org.apache.hadoop.security.UserGroupInformation;
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
  private final AtomicInteger appIdCounter = new AtomicInteger(0);

  LlapCoordinator() {
  }

  private void init(HiveConf hiveConf) throws IOException {
    // Only do the lightweight stuff in ctor; by default, LLAP coordinator is created during
    // HS2 init without the knowledge of LLAP usage (or lack thereof) in the cluster.
    this.hiveConf = hiveConf;
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
      localClientCache.cleanUp();
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
