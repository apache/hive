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
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGStatus;

/**
 * A bogus interface that basically describes the evolved usage patterns of TezSessionStateImpl.
 * Needed due to lack of multiple inheritance in Java; probably good to have, too - may make
 * TezSessionState interface a little bit clearer or even encourage some future cleanup.
 *
 * It's implemented in two ways - core implementations (regular session, external session),
 * and extra functionality implementation (pool session, WM session, etc.) that wraps an instance
 * of the core implementation (i.e. use composition). With MI, each session type would just inherit
 * from one of each.
 */
public interface TezSession {
  final class HiveResources {
    public HiveResources(Path dagResourcesDir) {
      this.dagResourcesDir = dagResourcesDir;
    }
    /** A directory that will contain resources related to DAGs and specified in configs. */
    final Path dagResourcesDir;
    final Map<String, LocalResource> additionalFilesNotFromConf = new HashMap<>();
    /** Localized resources of this session; both from conf and not from conf (above). */
    final Set<LocalResource> localizedResources = new HashSet<>();

    @Override
    public String toString() {
      return dagResourcesDir + "; " + additionalFilesNotFromConf.size() + " additional files, "
          + localizedResources.size() + " localized resources";
    }
  }

  // Core session operations.
  void open() throws IOException, TezException;
  void open(HiveResources resources) throws IOException, TezException;
  void open(String[] additionalFilesNotFromConf) throws IOException, TezException;
  void beginOpen(String[] additionalFiles, LogHelper console) throws IOException, TezException;
  void endOpen() throws InterruptedException, CancellationException;
  TezSession reopen() throws Exception;
  void destroy() throws Exception;
  void close(boolean keepTmpDir) throws Exception;
  void returnToSessionManager() throws Exception;

  /** This is called during open and update (i.e. internally and externally) to localize conf resources. */
  void ensureLocalResources(Configuration conf, String[] newFilesNotFromConf) throws IOException;
  HiveResources extractHiveResources();
  Path replaceHiveResources(HiveResources resources, boolean isAsync);

  List<LocalResource> getLocalizedResources();
  LocalResource getAppJarLr();

  HiveConf getConf();
  TezClient getTezClient();
  boolean isOpen();
  boolean isOpening();
  boolean getDoAsEnabled();
  String getSessionId();
  String getUser();
  WmContext getWmContext(); // Necessary for triggers, even for non-WM sessions.
  void setWmContext(WmContext ctx);
  void setQueueName(String queueName);
  String getQueueName();
  void setDefault();
  boolean isDefault();
  boolean getLegacyLlapMode();
  void setLegacyLlapMode(boolean b);
  void unsetOwnerThread();
  void setOwnerThread();
  KillQuery getKillQuery();
  void setKillQuery(KillQuery kq);
  boolean killQuery(String reason) throws HiveException;
  String getAppMasterUri();
  default Map<String, Double> getMetrics() {
    return new HashMap<>();
  }
  default void updateDagStatus(DAGStatus dagStatus) {
  }
}
