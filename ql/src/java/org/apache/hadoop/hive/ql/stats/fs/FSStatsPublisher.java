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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.stats.StatsCollectionTaskIndependent;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;

import com.esotericsoftware.kryo.io.Output;

public class FSStatsPublisher implements StatsPublisher, StatsCollectionTaskIndependent {

  private Configuration conf;
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private Map<String, Map<String,String>> statsMap; // map from partID -> (statType->value)

  @Override
  public boolean init(Configuration hconf) {
    Path statsDir = new Path(hconf.get(StatsSetupConst.STATS_TMP_LOC));
    LOG.debug("Initing FSStatsPublisher with : " + statsDir);
    try {
      statsDir.getFileSystem(hconf).mkdirs(statsDir);
      LOG.info("created : " + statsDir);
      return true;
    } catch (IOException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public boolean connect(Configuration hconf) {
    conf = hconf;
    Path statsDir = new Path(hconf.get(StatsSetupConst.STATS_TMP_LOC));
    LOG.debug("Connecting to : " + statsDir);
    statsMap = new HashMap<String, Map<String,String>>();
    try {
      return statsDir.getFileSystem(conf).exists(statsDir);
    } catch (IOException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public boolean publishStat(String partKV, Map<String, String> stats) {
    LOG.debug("Putting in map : " + partKV + "\t" + stats);
    // we need to do new hashmap, since stats object is reused across calls.
    Map<String,String> cpy = new HashMap<String, String>(stats);
    Map<String,String> statMap = statsMap.get(partKV);
    if (null != statMap) {
      // In case of LB, we might get called repeatedly.
      for (Entry<String, String> e : statMap.entrySet()) {
        cpy.put(e.getKey(), String.valueOf(Long.valueOf(e.getValue()) + Long.valueOf(cpy.get(e.getKey()))));
      }
    }
    statsMap.put(partKV, cpy);
    return true;
  }

  @Override
  public boolean closeConnection() {
    Path statsDir = new Path(conf.get(StatsSetupConst.STATS_TMP_LOC));
    try {
      Path statsFile = new Path(statsDir,StatsSetupConst.STATS_FILE_PREFIX +conf.getInt("mapred.task.partition",0));
      LOG.debug("About to create stats file for this task : " + statsFile);
      Output output = new Output(statsFile.getFileSystem(conf).create(statsFile,true));
      LOG.info("Created file : " + statsFile);
      LOG.info("Writing stats in it : " + statsMap);
      Utilities.runtimeSerializationKryo.get().writeObject(output, statsMap);
      output.close();
      return true;
    } catch (IOException e) {
      LOG.error(e);
      return false;
    }
  }
}
