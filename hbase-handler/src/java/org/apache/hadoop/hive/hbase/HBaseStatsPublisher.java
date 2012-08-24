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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;

/**
 * A class that implements the StatsPublisher interface through HBase.
 */
public class HBaseStatsPublisher implements StatsPublisher {

  private HTable htable;
  private final Log LOG = LogFactory.getLog(this.getClass().getName());

  /**
   * Does the necessary HBase initializations.
   */
  public boolean connect(Configuration hiveconf) {

    try {
      htable = new HTable(HBaseConfiguration.create(hiveconf),
        HBaseStatsSetupConstants.PART_STAT_TABLE_NAME);
      // for performance reason, defer update until the closeConnection
      htable.setAutoFlush(false);
    } catch (IOException e) {
      LOG.error("Error during HBase connection. " + e);
      return false;
    }

    return true;
  }

  /**
   * Writes temporary statistics into HBase;
   */
  public boolean publishStat(String rowID, Map<String, String> stats) {

    // Write in HBase

    if (stats.isEmpty()) {
      // If there are no stats to publish, nothing to do.
      return true;
    }

    if (!HBaseStatsUtils.isValidStatisticSet(stats.keySet())) {
      LOG.warn("Warning. Invalid statistic: " + stats.keySet().toString()
          + ", supported stats: "
          + HBaseStatsUtils.getSupportedStatistics());
      return false;
    }

    try {

      // check the basic stat (e.g., row_count)

      Get get = new Get(Bytes.toBytes(rowID));
      Result result = htable.get(get);

      byte[] family = HBaseStatsUtils.getFamilyName();
      byte[] column = HBaseStatsUtils.getColumnName(HBaseStatsUtils.getBasicStat());

      long val = Long.parseLong(HBaseStatsUtils.getStatFromMap(HBaseStatsUtils.getBasicStat(),
          stats));
      long oldVal = 0;

      if (!result.isEmpty()) {
        oldVal = Long.parseLong(Bytes.toString(result.getValue(family, column)));
      }

      if (oldVal >= val) {
        return true; // we do not need to publish anything
      }

      // we need to update
      Put row = new Put(Bytes.toBytes(rowID));
      for (String statType : HBaseStatsUtils.getSupportedStatistics()) {
        column = HBaseStatsUtils.getColumnName(statType);
        row.add(family, column, Bytes.toBytes(HBaseStatsUtils.getStatFromMap(statType, stats)));
      }

      htable.put(row);
      return true;

    } catch (IOException e) {
      LOG.error("Error during publishing statistics. " + e);
      return false;
    }
  }

  public boolean closeConnection() {
    // batch update
    try {
      htable.flushCommits();
      return true;
    } catch (IOException e) {
      LOG.error("Cannot commit changes in stats publishing.", e);
      return false;
    }
  }


  /**
   * Does the necessary HBase initializations.
   */
  public boolean init(Configuration hiveconf) {
    try {
      HBaseAdmin hbase = new HBaseAdmin(HBaseConfiguration.create(hiveconf));

      // Creating table if not exists
      if (!hbase.tableExists(HBaseStatsSetupConstants.PART_STAT_TABLE_NAME)) {
        HTableDescriptor table = new HTableDescriptor(HBaseStatsSetupConstants.PART_STAT_TABLE_NAME);
        HColumnDescriptor family = new HColumnDescriptor(HBaseStatsUtils.getFamilyName());
        table.addFamily(family);
        hbase.createTable(table);
      }
    } catch (IOException e) {
      LOG.error("Error during HBase initialization. " + e);
      return false;
    }

    return true;
  }
}
