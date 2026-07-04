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

package org.apache.hive.search.metastore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.leader.LeaderElection;
import org.apache.hadoop.hive.metastore.leader.LeaderException;
import org.apache.hadoop.hive.metastore.leader.LeaseLeaderElection;

@SuppressWarnings("rawtypes, unchecked")
public class MetastoreCluster implements AutoCloseable {
  private static final String CLUSTER_ELECTION_METHOD = "metastore.index.election.method";
  private static final Map<String, Pair<LeaderElection, Object>> CACHED = new ConcurrentHashMap<>();

  static {
    try {
      CACHED.put("default", Pair.of(new LeaseLeaderElection(),
          new TableName("__INDEX_CATALOG__", "__INDEX_DATABASE__", "__INDEX_TABLE__")));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final LeaderElection election;

  public MetastoreCluster(Configuration configuration,
      LeaderElection.LeadershipStateListener... listeners) throws IOException, LeaderException {
    String methodName = configuration.get(CLUSTER_ELECTION_METHOD, "default");
    Pair<LeaderElection, Object> leo = CACHED.get(methodName);
    if (leo == null) {
      throw new IOException("No elector configured for " + methodName);
    }
    this.election = leo.getKey();
    if (listeners != null) {
      Arrays.stream(listeners).forEach(this.election::addStateListener);
    }

    this.election.tryBeLeader(configuration, leo.getValue());
  }

  boolean isLeader() {
    return this.election.isLeader();
  }

  @Override
  public void close() throws Exception {
    if (election != null) {
      election.close();
    }
  }

  public static <T> void injectElection(Configuration configuration,
      String methodName, LeaderElection<T> election, T mutex) {
    configuration.set(CLUSTER_ELECTION_METHOD, methodName);
    CACHED.put(methodName, Pair.of(election, mutex));
  }
}
