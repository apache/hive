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

package org.apache.hadoop.hive.ql.stats;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * An test implementation for StatsPublisher.
 * The method corresponding to the configuration parameter
 * hive.test.dummystats.publisher fail, whereas all
 * other methods succeed
 */

public class DummyStatsPublisher implements StatsPublisher {

  String errorMethod = null;

  // This is a test. The parameter hive.test.dummystats.publisher's value
  // denotes the method which needs to throw an error.
  @Override
  public boolean init(StatsCollectionContext context) {
    errorMethod = HiveConf.getVar(context.getHiveConf(), HiveConf.ConfVars.HIVETESTMODEDUMMYSTATPUB);
    if (errorMethod.equalsIgnoreCase("init")) {
      return false;
    }

    return true;
  }

  @Override
  public boolean connect(StatsCollectionContext context) {
    errorMethod = HiveConf.getVar(context.getHiveConf(), HiveConf.ConfVars.HIVETESTMODEDUMMYSTATPUB);
    if (errorMethod.equalsIgnoreCase("connect")) {
      return false;
    }

    return true;
  }

  @Override
  public boolean publishStat(String fileID, Map<String, String> stats) {
    if (errorMethod.equalsIgnoreCase("publishStat")) {
      return false;
    }
    return true;
  }

  @Override
  public boolean closeConnection(StatsCollectionContext context) {
    if (errorMethod.equalsIgnoreCase("closeConnection")) {
      return false;
    }
    return true;
  }
}
