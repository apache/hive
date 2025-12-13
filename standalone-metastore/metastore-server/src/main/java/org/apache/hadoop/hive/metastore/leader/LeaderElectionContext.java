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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.leader.LeaderElection.LeadershipStateListener;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LeaderElectionContext {

  /**
   * Types of tasks that exactly have one instance in a given warehouse.
   * For those tasks which belong to the same type, they will be running in the same leader.
   */
  public enum TTYPE {
    HOUSEKEEPING(new TableName(Warehouse.DEFAULT_CATALOG_NAME, "__METASTORE_LEADER_ELECTION__",
        "metastore_housekeeping"), "housekeeping"),
    ALWAYS_TASKS(new TableName(Warehouse.DEFAULT_CATALOG_NAME, "__METASTORE_LEADER_ELECTION__",
        "metastore_always_tasks"), "always_tasks");
    // Mutex of TTYPE, which can be a nonexistent table
    private final TableName mutex;
    // Name of TTYPE
    private final String name;

    TTYPE(TableName tableName, String name) {
      this.mutex = tableName;
      this.name  = name;
    }
    public TableName getTableName() {
      return mutex;
    }
    public String getName() {
      return name;
    }
  }

  private final Configuration conf;
  private final String servHost;
  // Whether the context should be started as a daemon
  private final boolean startAsDaemon;
  // Audit the event of election
  private AuditLeaderListener auditLeaderListener;
  // State change listeners group by type
  private final Map<TTYPE, List<LeadershipStateListener>> listeners;
  // Collection of leader candidates
  private final List<LeaderElection<?>> leaderElections = new ArrayList<>();
  // Property for testing, a single leader will be created

  private LeaderElectionContext(String servHost, Configuration conf,
      Map<TTYPE, List<LeadershipStateListener>> listeners,
      boolean startAsDaemon, IHMSHandler handler) throws Exception {
    requireNonNull(conf, "conf is null");
    requireNonNull(listeners, "listeners is null");
    this.servHost = servHost;
    this.conf = new Configuration(conf);
    this.startAsDaemon = startAsDaemon;
    String tableName = MetastoreConf.getVar(conf,
        MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_AUDITTABLE);
    if (StringUtils.isNotEmpty(tableName)) {
      TableName table = TableName.fromString(tableName, MetaStoreUtils.getDefaultCatalog(conf),
          Warehouse.DEFAULT_DATABASE_NAME);
      auditLeaderListener = new AuditLeaderListener(table, handler);
    }
    this.listeners = listeners;
  }

  public void start() throws Exception {
    List<TTYPE> ttypes = new ArrayList<>(listeners.keySet());
    Collections.shuffle(ttypes);
    for (TTYPE ttype : ttypes) {
      List<LeadershipStateListener> listenerList = listeners.get(ttype);
      if (listenerList.isEmpty()) {
        continue;
      }
      if (auditLeaderListener != null) {
        listenerList.addFirst(auditLeaderListener);
      }
      LeaderElection leaderElection = LeaderElectionFactory.create(conf);
      leaderElection.setName(ttype.name);
      listenerList.forEach(leaderElection::addStateListener);
      leaderElections.add(leaderElection);
      Thread daemon = new Thread(() -> {
        try {
          Object mutex = LeaderElectionFactory.getMutex(conf, ttype, servHost);
          leaderElection.tryBeLeader(conf, mutex);
        } catch (LeaderException e) {
          throw new RuntimeException("Error claiming to be leader: " + leaderElection.getName(), e);
        }
      });

      if (startAsDaemon) {
        daemon.setName("Metastore Election " + leaderElection.getName());
        daemon.setDaemon(true);
        daemon.start();
      } else {
        daemon.run();
      }
    }
  }

  public void close() {
    leaderElections.forEach(le -> {
      try {
        le.close();
      } catch (Exception e) {
        HiveMetaStore.LOG.warn("Error closing election: " + le.getName(), e);
      }
    });
  }

  public static class ContextBuilder {
    private Configuration configuration;
    private boolean startAsDaemon;
    private String servHost;
    private IHMSHandler handler;
    private TTYPE ttype = TTYPE.HOUSEKEEPING;
    private Map<TTYPE, List<LeadershipStateListener>> listeners;

    public ContextBuilder(Configuration conf) {
      this.configuration = conf;
      this.listeners = new HashMap<>();
      for (TTYPE type : TTYPE.values()) {
        listeners.put(type, new ArrayList<>());
      }
    }

    public ContextBuilder servHost(String hostName) {
      this.servHost = hostName;
      return this;
    }

    public ContextBuilder setHMSHandler(IHMSHandler handler) {
      this.handler = handler;
      return this;
    }

    public ContextBuilder setTType(TTYPE type) {
      this.ttype = type;
      return this;
    }

    public ContextBuilder addListener(
        LeadershipStateListener listener) {
      listeners.get(ttype).add(listener);
      return this;
    }

    public ContextBuilder addListener(
        LeadershipStateListener listener, boolean condition) {
      if (condition) {
        addListener(listener);
      }
      return this;
    }

    public ContextBuilder startAsDaemon(boolean daemon) {
      this.startAsDaemon = daemon;
      return this;
    }

    public LeaderElectionContext build() throws Exception {
      return new LeaderElectionContext(servHost, configuration,
          listeners, startAsDaemon, handler);
    }
  }
}
