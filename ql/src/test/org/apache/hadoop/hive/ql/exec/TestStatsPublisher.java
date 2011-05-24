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

package org.apache.hadoop.hive.ql.exec;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;
import org.apache.hadoop.mapred.JobConf;

/**
 * TestOperators.
 *
 */
public class TestStatsPublisher extends TestCase {

  protected Configuration conf;
  protected String statsImplementationClass;

  public TestStatsPublisher(String name) {
    super(name);
  }

  @Override
  protected void setUp() {
    System.out.println("StatPublisher Test");
    conf = new JobConf(TestStatsPublisher.class);

    statsImplementationClass = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
    StatsFactory.setImplementation(statsImplementationClass, conf);
  }

  public void testStatsPublisherOneStat() throws Throwable {
    try {
      System.out.println("StatsPublisher - one stat published per key - aggregating matching key");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher((JobConf) conf);
      assertNotNull(statsPublisher);
      assertTrue(statsPublisher.init(conf));
      assertTrue(statsPublisher.connect(conf));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = StatsFactory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(conf));

      // publish stats
      assertTrue(statsPublisher.publishStat("file_00000", StatsSetupConst.ROW_COUNT, "200"));
      assertTrue(statsPublisher.publishStat("file_00001", StatsSetupConst.ROW_COUNT, "400"));

      // aggregate existing stats for prefixes
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("200", rows0);
      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("400", rows1);

      // close connections
      assertTrue(statsPublisher.closeConnection());
      assertTrue(statsAggregator.closeConnection());

      System.out.println("StatsPublisher - one stat published per key - aggregating matching key - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }


  public void testStatsPublisher() throws Throwable {
    try {
      System.out.println("StatsPublisher - basic functionality");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher((JobConf) conf);
      assertNotNull(statsPublisher);
      assertTrue(statsPublisher.init(conf));
      assertTrue(statsPublisher.connect(conf));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = StatsFactory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(conf));

      // publish stats
      assertTrue(statsPublisher.publishStat("file_00000_a", StatsSetupConst.ROW_COUNT, "200"));
      assertTrue(statsPublisher.publishStat("file_00000_b", StatsSetupConst.ROW_COUNT, "300"));
      assertTrue(statsPublisher.publishStat("file_00001_a", StatsSetupConst.ROW_COUNT, "400"));
      assertTrue(statsPublisher.publishStat("file_00001_b", StatsSetupConst.ROW_COUNT, "500"));

      // aggregate existing stats for prefixes
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("500", rows0);
      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("900", rows1);

      // aggregate non-existent stats
      String rowsX = statsAggregator.aggregateStats("file_00002", StatsSetupConst.ROW_COUNT);
      assertEquals("0", rowsX);

      // close connections
      assertTrue(statsPublisher.closeConnection());
      assertTrue(statsAggregator.closeConnection());

      System.out.println("StatsPublisher - basic functionality - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStatsPublisherMultipleUpdates() throws Throwable {
    try {
      System.out.println("StatsPublisher - multiple updates");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher((JobConf) conf);
      assertNotNull(statsPublisher);
      assertTrue(statsPublisher.init(conf));
      assertTrue(statsPublisher.connect(conf));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = StatsFactory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(conf));

      // publish stats
      assertTrue(statsPublisher.publishStat("file_00000_a", StatsSetupConst.ROW_COUNT, "200"));
      assertTrue(statsPublisher.publishStat("file_00000_b", StatsSetupConst.ROW_COUNT, "300"));
      assertTrue(statsPublisher.publishStat("file_00001_a", StatsSetupConst.ROW_COUNT, "400"));
      assertTrue(statsPublisher.publishStat("file_00001_b", StatsSetupConst.ROW_COUNT, "500"));

      // repetitive update - should not change the stored value - as the published values are
      // smaller than the current ones
      assertTrue(statsPublisher.publishStat("file_00000_a", StatsSetupConst.ROW_COUNT, "100"));
      assertTrue(statsPublisher.publishStat("file_00000_b", StatsSetupConst.ROW_COUNT, "150"));

      // should change the stored value - the published values are greater than the current values
      assertTrue(statsPublisher.publishStat("file_00001_a", StatsSetupConst.ROW_COUNT, "500"));
      assertTrue(statsPublisher.publishStat("file_00001_b", StatsSetupConst.ROW_COUNT, "600"));

      // aggregate stats
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("500", rows0);
      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("1100", rows1);

      // close connections
      assertTrue(statsPublisher.closeConnection());
      assertTrue(statsAggregator.closeConnection());

      System.out.println("StatsPublisher - multiple updates - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStatsPublisherAfterAggregationCleanUp() throws Throwable {
    try {
      System.out.println("StatsPublisher - clean-up after aggregation");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher((JobConf) conf);
      assertNotNull(statsPublisher);
      assertTrue(statsPublisher.init(conf));
      assertTrue(statsPublisher.connect(conf));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = StatsFactory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(conf));

      // publish stats
      assertTrue(statsPublisher.publishStat("file_00000_a", StatsSetupConst.ROW_COUNT, "200"));
      assertTrue(statsPublisher.publishStat("file_00000_b", StatsSetupConst.ROW_COUNT, "300"));
      assertTrue(statsPublisher.publishStat("file_00001_a", StatsSetupConst.ROW_COUNT, "400"));
      assertTrue(statsPublisher.publishStat("file_00001_b", StatsSetupConst.ROW_COUNT, "500"));

      // aggregate stats
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("500", rows0);
      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("900", rows1);

      // now the table should be empty
      rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("0", rows0);
      rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("0", rows1);

      // close connections
      assertTrue(statsPublisher.closeConnection());
      assertTrue(statsAggregator.closeConnection());

      System.out.println("StatsPublisher - clean-up after aggregation - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStatsAggregatorCleanUp() throws Throwable {
    try {
      System.out.println("StatsAggregator - clean-up");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher((JobConf) conf);
      assertNotNull(statsPublisher);
      assertTrue(statsPublisher.init(conf));
      assertTrue(statsPublisher.connect(conf));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = StatsFactory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(conf));

      // publish stats
      assertTrue(statsPublisher.publishStat("file_00000_a", StatsSetupConst.ROW_COUNT, "200"));
      assertTrue(statsPublisher.publishStat("file_00000_b", StatsSetupConst.ROW_COUNT, "300"));

      // cleanUp (closes the connection)
      assertTrue(statsAggregator.cleanUp("file_00000"));

      // now the connection should be closed (aggregator will report an error)
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertNull(rows0);

      // close connections
      assertTrue(statsPublisher.closeConnection());
      assertTrue(statsAggregator.closeConnection());

      System.out.println("StatsAggregator - clean-up - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
