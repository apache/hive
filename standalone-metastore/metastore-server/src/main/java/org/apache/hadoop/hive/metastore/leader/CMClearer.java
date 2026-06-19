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

package org.apache.hadoop.hive.metastore.leader;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.CM_THREAD_NAME_PREFIX;

public class CMClearer implements LeaderElection.LeadershipStateListener {

  private final Configuration configuration;

  private ScheduledExecutorService executor;

  public CMClearer(Configuration configuration) {
    this.configuration = requireNonNull(configuration, "configuration is null");
  }

  @Override
  public void takeLeadership(LeaderElection election) throws Exception {
    if (!MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.REPLCMENABLED)) {
      return;
    }
    if (executor != null) {
      throw new IllegalStateException("There should be no running tasks");
    }
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new BasicThreadFactory.Builder()
            .namingPattern(CM_THREAD_NAME_PREFIX + "%d")
            .daemon(true)
            .build());
    executor.scheduleAtFixedRate(new ReplChangeManager.CMClearer(
        MetastoreConf.getTimeVar(configuration, MetastoreConf.ConfVars.REPLCMRETIAN, TimeUnit.SECONDS), configuration),
        0,
        MetastoreConf.getTimeVar(configuration, MetastoreConf.ConfVars.REPLCMINTERVAL, TimeUnit.SECONDS),
        TimeUnit.SECONDS);
  }

  @Override
  public void lossLeadership(LeaderElection election) throws Exception {
    if (executor != null) {
      executor.shutdown();
      executor = null;
    }
  }

}
