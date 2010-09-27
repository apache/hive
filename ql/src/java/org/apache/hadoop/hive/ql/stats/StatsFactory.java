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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A factory of stats publisher and aggregator implementations of the
 * StatsPublisher and StatsAggregator interfaces.
 */
public final class StatsFactory {

  static final private Log LOG = LogFactory.getLog(StatsFactory.class.getName());

  private static Class <? extends Serializable> publisherImplementation;
  private static Class <? extends Serializable> aggregatorImplementation;
  private static Configuration jobConf;

  /**
   * Sets the paths of the implementation classes of publishing
   * and aggregation (IStatsPublisher and IStatsAggregator interfaces).
   * The paths are determined according to a configuration parameter which
   * is passed as the user input for choosing the implementation as MySQL, HBase, ...
   */
  public static boolean setImplementation(String configurationParam, Configuration conf) {

    ClassLoader classLoader = JavaUtils.getClassLoader();
    if (configurationParam.equals(StatsSetupConst.HBASE_IMPL_CLASS_VAL)) {
      // Case: hbase
      try {
        publisherImplementation = (Class<? extends Serializable>)
          Class.forName("org.apache.hadoop.hive.hbase.HBaseStatsPublisher", true, classLoader);

        aggregatorImplementation = (Class<? extends Serializable>)
          Class.forName("org.apache.hadoop.hive.hbase.HBaseStatsAggregator", true, classLoader);
      } catch (ClassNotFoundException e) {
        LOG.error("HBase Publisher/Aggregator classes cannot be loaded.", e);
        return false;
      }
    } else if (configurationParam.contains(StatsSetupConst.JDBC_IMPL_CLASS_VAL)) {
      // Case: jdbc:mysql or jdbc:derby
      try {
        publisherImplementation = (Class<? extends Serializable>)
          Class.forName("org.apache.hadoop.hive.ql.stats.jdbc.JDBCStatsPublisher", true, classLoader);

        aggregatorImplementation = (Class<? extends Serializable>)
          Class.forName("org.apache.hadoop.hive.ql.stats.jdbc.JDBCStatsAggregator", true, classLoader);
      } catch (ClassNotFoundException e) {
        LOG.error("JDBC Publisher/Aggregator classes cannot be loaded.", e);
        return false;
      }
    } else {
      // ERROR
      return false;
    }

    jobConf = conf;
    return true;
  }

  /**
   * Returns a Stats publisher implementation class for the IStatsPublisher interface
   * For example HBaseStatsPublisher for the HBase implementation
   */
  public static StatsPublisher getStatsPublisher() {

    return (StatsPublisher) ReflectionUtils.newInstance(publisherImplementation, jobConf);
  }

  /**
   * Returns a Stats Aggregator implementation class for the IStatsAggregator interface
   * For example HBaseStatsAggregator for the HBase implementation
   */
  public static StatsAggregator getStatsAggregator() {

    return (StatsAggregator) ReflectionUtils.newInstance(aggregatorImplementation, jobConf);
  }

}
