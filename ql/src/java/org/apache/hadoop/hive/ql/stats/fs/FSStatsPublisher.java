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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class FSStatsPublisher implements StatsPublisher {

  private Configuration conf;
  private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());
  private Map<String, Map<String,String>> statsMap; // map from partID -> (statType->value)

  @Override
  public boolean init(StatsCollectionContext context) {
    try {
      for (String tmpDir : context.getStatsTmpDirs()) {
        Path statsDir = new Path(tmpDir);
        LOG.debug("Initing FSStatsPublisher with : " + statsDir);
        statsDir.getFileSystem(context.getHiveConf()).mkdirs(statsDir);
        LOG.info("created : " + statsDir);
      }
      return true;
    } catch (IOException e) {
      LOG.error("Failed to create dir", e);
      return false;
    }
  }

  @Override
  public boolean connect(StatsCollectionContext context) {
    conf = context.getHiveConf();
    List<String> statsDirs = context.getStatsTmpDirs();
    assert statsDirs.size() == 1 : "Found multiple stats dirs: " + statsDirs;
    Path statsDir = new Path(statsDirs.get(0));
    LOG.debug("Connecting to : " + statsDir);
    statsMap = new HashMap<String, Map<String,String>>();
    try {
      return statsDir.getFileSystem(conf).exists(statsDir);
    } catch (IOException e) {
      LOG.error("Failed to check if dir exists", e);
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
        cpy.put(e.getKey(),
            String.valueOf(Long.parseLong(e.getValue()) + Long.parseLong(cpy.get(e.getKey()))));
      }
    }
    statsMap.put(partKV, cpy);
    return true;
  }

  @Override
  public boolean closeConnection(StatsCollectionContext context) {
    List<String> statsDirs = context.getStatsTmpDirs();
    assert statsDirs.size() == 1 : "Found multiple stats dirs: " + statsDirs;
    Path statsDir = new Path(statsDirs.get(0));
    try {
      Path statsFile = null;
      if (context.getIndexForTezUnion() != -1) {
        statsFile = new Path(statsDir, StatsSetupConst.STATS_FILE_PREFIX
            + conf.getInt("mapred.task.partition", 0) + "_" + context.getIndexForTezUnion());
      } else {
        statsFile = new Path(statsDir, StatsSetupConst.STATS_FILE_PREFIX
            + conf.getInt("mapred.task.partition", 0));
      }
      LOG.debug("About to create stats file for this task : " + statsFile);
      Output output = new Output(statsFile.getFileSystem(conf).create(statsFile,true));
      LOG.debug("Created file : " + statsFile);
      LOG.debug("Writing stats in it : " + statsMap);
      Kryo kryo = SerializationUtilities.borrowKryo();
      try {
        kryo.writeObject(output, statsMap);
      } finally {
        SerializationUtilities.releaseKryo(kryo);
      }
      output.close();
      return true;
    } catch (IOException e) {
      LOG.error("Failed to persist stats on filesystem",e);
      return false;
    }
  }
}
