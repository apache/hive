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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class StatsUpdaterTask implements LeaderElection.LeadershipStateListener {

  private final Configuration configuration;
  private Optional<MetaStoreThread> statsUpdater;
  private AtomicBoolean stop;

  public StatsUpdaterTask(Configuration configuration) {
    this.configuration = requireNonNull(configuration, "configuration is null");
  }

  private MetaStoreThread instantiateThread(String classname) throws Exception {
    Object o = JavaUtils.newInstance(Class.forName(classname));
    if (MetaStoreThread.class.isAssignableFrom(o.getClass())) {
      return (MetaStoreThread) o;
    } else {
      String s = classname + " is not an instance of MetaStoreThread.";
      HiveMetaStore.LOG.error(s);
      throw new IOException(s);
    }
  }

  // Copied from HiveMetaStore
  public Optional<MetaStoreThread> getStatsUpdaterThread() throws Exception {
    MetastoreConf.StatsUpdateMode mode = MetastoreConf.StatsUpdateMode.valueOf(
        MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.STATS_AUTO_UPDATE).toUpperCase());
    if (mode == MetastoreConf.StatsUpdateMode.NONE) {
      return Optional.empty();
    }
    MetaStoreThread t =
        instantiateThread("org.apache.hadoop.hive.ql.stats.StatsUpdaterThread");
    return Optional.of(t);
  }

  @Override
  public void takeLeadership(LeaderElection election) throws Exception {
    if (statsUpdater != null) {
      throw new IllegalStateException("There should be no running stats updater before taking the leadership!");
    }

    this.statsUpdater = getStatsUpdaterThread();
    if (statsUpdater.isPresent()) {
      try {
        MetaStoreThread thread = statsUpdater.get();
        thread.setConf(configuration);
        stop = new AtomicBoolean(false);
        thread.init(stop);
        HiveMetaStore.LOG.info("Starting metastore thread of type " + thread.getClass().getName());
        thread.start();
      } catch (Exception e) {
        HiveMetaStore.LOG.error("Error while starting stats updater", e);
      }
    }
  }

  @Override
  public void lossLeadership(LeaderElection election) throws Exception {
    if (statsUpdater != null) {
      statsUpdater.ifPresent(statsUpdater -> {
        stop.set(true);
        ((Thread) statsUpdater).interrupt();
        HiveMetaStore.LOG.info("Stopped the stats updater tasks.");
      });
      statsUpdater = null;
      stop = null;
    }
  }

}
