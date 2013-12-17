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

package org.apache.hadoop.hive.ql.stats;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.StatsSetupConst.StatDB;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSDBCLASS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_STATS_KEY_PREFIX_MAX_LENGTH;

/**
 * A factory of stats publisher and aggregator implementations of the
 * StatsPublisher and StatsAggregator interfaces.
 */
public final class StatsFactory {

  static final private Log LOG = LogFactory.getLog(StatsFactory.class.getName());

  private Class <? extends Serializable> publisherImplementation;
  private Class <? extends Serializable> aggregatorImplementation;
  private Configuration jobConf;

  public static int getMaxPrefixLength(Configuration conf) {
    int maxPrefixLength = HiveConf.getIntVar(conf, HIVE_STATS_KEY_PREFIX_MAX_LENGTH);
    if (HiveConf.getVar(conf, HIVESTATSDBCLASS).equalsIgnoreCase(StatDB.counter.name())) {
      // see org.apache.hadoop.mapred.Counter or org.apache.hadoop.mapreduce.MRJobConfig
      int groupNameMax = conf.getInt("mapreduce.job.counters.group.name.max", 128);
      maxPrefixLength = maxPrefixLength < 0 ? groupNameMax :
          Math.min(maxPrefixLength, groupNameMax);
    }
    return maxPrefixLength;
  }

  public static StatsFactory newFactory(Configuration conf) {
    return newFactory(HiveConf.getVar(conf, HIVESTATSDBCLASS), conf);
  }

  /**
   * Sets the paths of the implementation classes of publishing
   * and aggregation (IStatsPublisher and IStatsAggregator interfaces).
   * The paths are determined according to a configuration parameter which
   * is passed as the user input for choosing the implementation as MySQL, HBase, ...
   */
  public static StatsFactory newFactory(String configurationParam, Configuration conf) {
    StatsFactory factory = new StatsFactory(conf);
    if (factory.initialize(configurationParam.toLowerCase())) {
      return factory;
    }
    return null;
  }

  private StatsFactory(Configuration conf) {
    this.jobConf = conf;
  }

  private boolean initialize(String type) {
    ClassLoader classLoader = JavaUtils.getClassLoader();
    try {
      StatDB statDB = type.startsWith("jdbc") ? StatDB.jdbc : StatDB.valueOf(type);
      publisherImplementation = (Class<? extends Serializable>)
          Class.forName(statDB.getPublisher(jobConf), true, classLoader);
      aggregatorImplementation = (Class<? extends Serializable>)
          Class.forName(statDB.getAggregator(jobConf), true, classLoader);
    } catch (Exception e) {
      LOG.error(type + " Publisher/Aggregator classes cannot be loaded.", e);
      return false;
    }
    return true;
  }

  /**
   * Returns a Stats publisher implementation class for the IStatsPublisher interface
   * For example HBaseStatsPublisher for the HBase implementation
   */
  public StatsPublisher getStatsPublisher() {

    return (StatsPublisher) ReflectionUtils.newInstance(publisherImplementation, jobConf);
  }

  /**
   * Returns a Stats Aggregator implementation class for the IStatsAggregator interface
   * For example HBaseStatsAggregator for the HBase implementation
   */
  public StatsAggregator getStatsAggregator() {

    return (StatsAggregator) ReflectionUtils.newInstance(aggregatorImplementation, jobConf);
  }

}
