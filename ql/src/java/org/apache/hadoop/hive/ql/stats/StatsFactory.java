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

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst.StatDB;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_STATS_DBCLASS;

/**
 * A factory of stats publisher and aggregator implementations of the
 * StatsPublisher and StatsAggregator interfaces.
 */
public final class StatsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StatsFactory.class.getName());

  private Class <? extends Serializable> publisherImplementation;
  private Class <? extends Serializable> aggregatorImplementation;
  private final Configuration jobConf;

  public static StatsFactory newFactory(Configuration conf) {
    return newFactory(HiveConf.getVar(conf, HIVE_STATS_DBCLASS), conf);
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
    ClassLoader classLoader = Utilities.getSessionSpecifiedClassLoader();
    try {
      StatDB statDB = StatDB.valueOf(type);
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
