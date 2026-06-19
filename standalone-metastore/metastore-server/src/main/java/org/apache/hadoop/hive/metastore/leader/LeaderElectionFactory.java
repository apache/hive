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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

/**
 * Simple factory for creating the elector
 */
public class LeaderElectionFactory {
  public enum Method {
    HOST, LOCK
  }

  private static final Map<Method, ElectionCreator> ELECTION_CREATOR_MAP = new ConcurrentHashMap<>();
  static {
    addElectionCreator(Method.HOST, conf -> new StaticLeaderElection());
    addElectionCreator(Method.LOCK, conf -> new LeaseLeaderElection());
  }

  private LeaderElectionFactory() {
    throw new AssertionError("The constructor shouldn't be called");
  }

  public static LeaderElection create(Configuration conf) throws IOException {
    String method =
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_ELECTION);
    Method m = EnumUtils.getEnum(Method.class, method.toUpperCase());
    ElectionCreator creator = null;
    if (m != null) {
      creator = ELECTION_CREATOR_MAP.get(m);
    }
    if (creator == null) {
      throw new UnsupportedOperationException(method + " not supported for electing the leader");
    }
    return creator.createElector(conf);
  }

  public static Object getMutex(Configuration conf, LeaderElectionContext.TTYPE ttype, String servHost) {
    String method =
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_ELECTION);
    Method m = EnumUtils.getEnum(Method.class, method.toUpperCase());
    if (m != null) {
      switch (m) {
      case HOST:
        return servHost;
      case LOCK:
        TableName mutex = ttype.getTableName();
        String namespace =
            MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_LOCK_NAMESPACE);
        if (StringUtils.isNotEmpty(namespace)) {
          return new TableName(mutex.getCat(), namespace, mutex.getTable());
        }
        return mutex;
      }
    }
    throw new UnsupportedOperationException(method + " not supported for leader election");
  }

  @VisibleForTesting
  public static void addElectionCreator(Method method, ElectionCreator creator) {
    ELECTION_CREATOR_MAP.put(method, creator);
  }

  public interface ElectionCreator {
    LeaderElection createElector(Configuration conf) throws IOException;
  }

}
