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
    WORKER(new TableName(Warehouse.DEFAULT_CATALOG_NAME, "__METASTORE_LEADER_ELECTION__",
        "metastore_compactor_worker"), "compactor_worker"),
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
  private final List<LeaderElection> leaderElections = new ArrayList<>();
  // Property for testing, a single leader will be created
  public final static String LEADER_IN_TEST = "metastore.leader.election.in.test";

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
    Map<TTYPE, List<LeadershipStateListener>> listenerMap = this.listeners;
    if (conf.getBoolean(LEADER_IN_TEST, false)) {
      Map<TTYPE, List<LeadershipStateListener>> newListeners = new HashMap<>();
      newListeners.put(TTYPE.HOUSEKEEPING, new ArrayList<>());
      listenerMap.forEach((k, v) -> newListeners.get(TTYPE.HOUSEKEEPING).addAll(v));
      listenerMap = newListeners;
    }
    for (Map.Entry<TTYPE, List<LeadershipStateListener>> entry :
        listenerMap.entrySet()) {
      List<LeadershipStateListener> listenerList = entry.getValue();
      if (listenerList.isEmpty()) {
        continue;
      }
      if (auditLeaderListener != null) {
        listenerList.add(0, auditLeaderListener);
      }
      TTYPE ttype = entry.getKey();
      LeaderElection leaderElection = LeaderElectionFactory.create(conf);
      leaderElection.setName(ttype.name);
      listenerList.forEach(listener -> leaderElection.addStateListener(listener));
      leaderElections.add(leaderElection);
      
      Thread daemon = new Thread(() -> {
        try {
          Object mutex = getLeaderMutex(conf, ttype, servHost);
          leaderElection.tryBeLeader(conf, mutex);
        } catch (LeaderException e) {
          throw new RuntimeException("Error claiming to be leader: " + leaderElection.getName(), e);
        }
      });
      daemon.setName("Metastore Election " + leaderElection.getName());
      daemon.setDaemon(true);

      if (startAsDaemon) {
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

  public static Object getLeaderMutex(Configuration conf, TTYPE ttype, String servHost) {
    String method =
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_ELECTION);
    switch (method.toLowerCase()) {
    case "host":
      return servHost;
    case "lock":
      TableName mutex = ttype.getTableName();
      String namespace =
          MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_LOCK_NAMESPACE);
      if (StringUtils.isNotEmpty(namespace)) {
        return new TableName(mutex.getCat(), namespace, mutex.getTable());
      }
      return mutex;
    default:
      throw new UnsupportedOperationException(method + " not supported for leader election");
    }
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
