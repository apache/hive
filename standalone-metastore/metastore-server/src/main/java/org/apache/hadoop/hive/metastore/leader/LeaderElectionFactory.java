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

/**
 * Simple factory for creating the elector
 */
public class LeaderElectionFactory {

  public static LeaderElection create(Configuration conf) {
    String method =
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_ELECTION);
    switch (method.toLowerCase()) {
      case "host":
        return new StaticLeaderElection();
      case "lock":
        return new LeaseLeaderElection();
      default:
        throw new UnsupportedOperationException("Do not support " + method + " now");
    }
  }

}
