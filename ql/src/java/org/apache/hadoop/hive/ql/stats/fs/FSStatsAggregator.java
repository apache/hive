/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsCollectionTaskIndependent;

import com.esotericsoftware.kryo.io.Input;

public class FSStatsAggregator implements StatsAggregator, StatsCollectionTaskIndependent {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private List<Map<String,Map<String,String>>> statsList;
  private Map<String, Map<String,String>> statsMap;
  private FileSystem fs;
  private Configuration conf;

  @Override
  public boolean connect(Configuration hconf, Task sourceTask) {
    conf = hconf;
    Path statsDir = new Path(hconf.get(StatsSetupConst.STATS_TMP_LOC));
    LOG.debug("About to read stats from : " + statsDir);
    statsMap  = new HashMap<String, Map<String,String>>();

    try {
      fs = statsDir.getFileSystem(hconf);
      statsList = new ArrayList<Map<String,Map<String,String>>>();
      FileStatus[] status = fs.listStatus(statsDir, new PathFilter() {
        @Override
        public boolean accept(Path file) {
          return file.getName().startsWith(StatsSetupConst.STATS_FILE_PREFIX);
        }
      });
      for (FileStatus file : status) {
        Input in = new Input(fs.open(file.getPath()));
        statsMap = Utilities.runtimeSerializationKryo.get().readObject(in, statsMap.getClass());
        LOG.info("Read stats : " +statsMap);
        statsList.add(statsMap);
        in.close();
      }
      return true;
    } catch (IOException e) {
      LOG.error(e);
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
      counter += Long.valueOf(statVal);
    }
    LOG.info("Read stats for : " + partID + "\t" + statType + "\t" + counter);

    return String.valueOf(counter);
  }

  @Override
  public boolean closeConnection() {
    LOG.debug("About to delete stats tmp dir");

    try {
      fs.delete(new Path(conf.get(StatsSetupConst.STATS_TMP_LOC)),true);
      return true;
    } catch (IOException e) {
      LOG.error(e);
      return true;
    }
  }

  @Override
  public boolean cleanUp(String keyPrefix) {
    return true;
  }
}
