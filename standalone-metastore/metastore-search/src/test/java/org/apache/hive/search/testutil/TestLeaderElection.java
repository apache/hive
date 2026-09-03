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

package org.apache.hive.search.testutil;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.leader.LeaderElection;
import org.apache.hadoop.hive.metastore.leader.LeaderException;

/** Fixed leader election for metastore-search unit and integration tests. */
public final class TestLeaderElection implements LeaderElection<Object> {
  private final boolean leader;
  private final List<LeadershipStateListener> listeners = new ArrayList<>();
  private String name;

  public TestLeaderElection(boolean leader) {
    this.leader = leader;
  }

  @Override
  public void tryBeLeader(Configuration conf, Object mutex) throws LeaderException {
    if (leader) {
      for (LeadershipStateListener listener : listeners) {
        try {
          listener.takeLeadership(this);
        } catch (Exception e) {
          throw new LeaderException("failed to notify test leader listener", e);
        }
      }
    }
  }

  @Override
  public boolean isLeader() {
    return leader;
  }

  @Override
  public void addStateListener(LeadershipStateListener listener) {
    listeners.add(listener);
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void close() {
  }
}
