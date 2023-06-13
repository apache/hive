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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * URL based leader election via metastore.housekeeping.leader.hostname.
 * If the value is empty, means no election happening and all instances are leaders,
 * otherwise the instance matching the configured hostname will be selected as a leader.
 */
public class StaticLeaderElection implements LeaderElection<String> {

  private static final Logger LOG = LoggerFactory.getLogger(StaticLeaderElection.class);
  private volatile boolean isLeader;
  private String name;
  private List<LeadershipStateListener> listeners = new ArrayList<>();

  @Override
  public void tryBeLeader(Configuration conf, String hostName)
      throws LeaderException {
    String leaderHost = MetastoreConf.getVar(conf,
        MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME);

    // For the sake of backward compatibility, when the current HMS becomes the leader when no
    // leader is specified.
    if (leaderHost == null || leaderHost.isEmpty()) {
      LOG.info(MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME + " is empty. Start all the " +
          "housekeeping threads.");
      isLeader = true;
    } else {
      LOG.info(MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME + " is set to " + leaderHost);
      isLeader = leaderHost.trim().equals(hostName);
    }

    if (isLeader) {
      listeners.forEach(listener -> {
        try {
          listener.takeLeadership(this);
        } catch (Exception e) {
          LOG.warn("Error while notifying the listener: " + listener, e);
        }
      });
    }
  }

  @Override
  public boolean isLeader() {
    return isLeader;
  }

  @Override
  public void addStateListener(LeadershipStateListener listener) {
    requireNonNull(listener, "listener is null");
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
    if (isLeader) {
      isLeader = false;
      listeners.forEach(listener -> {
        try {
          listener.lossLeadership(this);
        } catch (Exception e) {
          LOG.warn("Error while notifying the listener: " + listener, e);
        }
      });
    }
  }

}
