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

package org.apache.hadoop.hive.ql.stats.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class FSStatsAggregator implements StatsAggregator {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());
  private List<Map<String,Map<String,String>>> statsList;
  private Map<String, Map<String,String>> statsMap;
  private FileSystem fs;

  @Override
  public boolean connect(StatsCollectionContext scc) {
    List<String> statsDirs = scc.getStatsTmpDirs();
    assert statsDirs.size() == 1 : "Found multiple stats dirs: " + statsDirs;
    Path statsDir = new Path(statsDirs.get(0));
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("About to read stats from : " + statsDir);
    }
    statsMap  = new HashMap<String, Map<String,String>>();

    try {
      fs = statsDir.getFileSystem(scc.getHiveConf());
      statsList = new ArrayList<Map<String,Map<String,String>>>();
      FileStatus[] status = fs.listStatus(statsDir, new PathFilter() {
        @Override
        public boolean accept(Path file) {
          return file.getName().startsWith(StatsSetupConst.STATS_FILE_PREFIX);
        }
      });
      for (FileStatus file : status) {
        Input in = new Input(fs.open(file.getPath()));
        Kryo kryo = SerializationUtilities.borrowKryo();
        try {
          statsMap = kryo.readObject(in, statsMap.getClass());
        } finally {
          SerializationUtilities.releaseKryo(kryo);
        }
        statsList.add(statsMap);
        in.close();
      }
      return true;
    } catch (IOException e) {
      Utilities.FILE_OP_LOGGER.error("Failed to read stats from filesystem ", e);
      return false;
    }
  }


  @Override
  public String aggregateStats(String partID, String statType) {
    long counter = 0;
    LOG.debug("Part ID: " + partID + "\t" + statType);
    for (Map<String,Map<String,String>> statsMap : statsList) {
      Map<String,String> partStat = statsMap.get(partID);
      if (null == partStat) { // not all partitions are scanned in all mappers, so this could be null.
        continue;
      }
      String statVal = partStat.get(statType);
      if (null == statVal) { // partition was found, but was empty.
        continue;
      }
      counter += Long.parseLong(statVal);
    }
    LOG.info("Read stats for : " + partID + "\t" + statType + "\t" + counter);

    return String.valueOf(counter);
  }

  @Override
  public boolean closeConnection(StatsCollectionContext scc) {
    List<String> statsDirs = scc.getStatsTmpDirs();
    assert statsDirs.size() == 1 : "Found multiple stats dirs: " + statsDirs;
    Path statsDir = new Path(statsDirs.get(0));

    LOG.debug("About to delete stats tmp dir :" + statsDir);

    try {
      fs.delete(statsDir,true);
      return true;
    } catch (IOException e) {
      LOG.error("Failed to delete stats dir", e);
      return true;
    }
  }
}
