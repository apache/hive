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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.mapred.JobConf;

/**
 * TestPublisher jdbc.
 *
 */
public class TestStatsPublisherEnhanced extends TestCase {

  protected Configuration conf;
  protected String statsImplementationClass;
  protected Map<String, String> stats;

  protected StatsFactory factory;

  public TestStatsPublisherEnhanced(String name) {
    super(name);
    conf = new JobConf(TestStatsPublisherEnhanced.class);
    conf.set("hive.stats.dbclass", "jdbc:derby");
    factory = StatsFactory.newFactory(conf);
    assert factory != null;
  }

  @Override
  protected void setUp() {
    stats = new HashMap<String, String>();
  }

  @Override
  protected void tearDown() {
    StatsAggregator sa = factory.getStatsAggregator();
    assertNotNull(sa);
    StatsCollectionContext sc = new StatsCollectionContext(conf);
    assertTrue(sa.connect(sc));
    assertTrue(sa.cleanUp("file_0"));
    assertTrue(sa.closeConnection(sc));
  }

  private void fillStatMap(String numRows, String rawDataSize) {
    stats.clear();
    stats.put(StatsSetupConst.ROW_COUNT, numRows);
    if (!rawDataSize.equals("")) {
      stats.put(StatsSetupConst.RAW_DATA_SIZE, rawDataSize);
    }
  }

  public void testStatsPublisherOneStat() throws Throwable {
    try {
      System.out.println("StatsPublisher - one stat published per key - aggregating matching key");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher((JobConf) conf);
      assertNotNull(statsPublisher);
      StatsCollectionContext sc = new StatsCollectionContext(conf);
      assertTrue(statsPublisher.init(sc));
      assertTrue(statsPublisher.connect(sc));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = factory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(sc));

      // publish stats
      fillStatMap("200", "1000");
      assertTrue(statsPublisher.publishStat("file_00000", stats));
      fillStatMap("400", "3000");
      assertTrue(statsPublisher.publishStat("file_00001", stats));


      // aggregate existing stats
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("200", rows0);
      String usize0 = statsAggregator.aggregateStats("file_00000",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("1000", usize0);

      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("400", rows1);
      String usize1 = statsAggregator.aggregateStats("file_00001",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("3000", usize1);

      // close connections
      assertTrue(statsPublisher.closeConnection(sc));
      assertTrue(statsAggregator.closeConnection(sc));

      System.out
          .println("StatsPublisher - one stat published per key - aggregating matching key - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStatsPublisher() throws Throwable {
    try {
      System.out.println("StatsPublisher - basic functionality");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher(
          (JobConf) conf);
      assertNotNull(statsPublisher);
      StatsCollectionContext sc = new StatsCollectionContext(conf);
      assertTrue(statsPublisher.init(sc));
      assertTrue(statsPublisher.connect(sc));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = factory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(sc));
      // statsAggregator.cleanUp("file_0000");
      // assertTrue(statsAggregator.connect(conf));

      // publish stats
      fillStatMap("200", "1000");
      assertTrue(statsPublisher.publishStat("file_00000_a", stats));
      fillStatMap("300", "2000");
      assertTrue(statsPublisher.publishStat("file_00000_b", stats));

      fillStatMap("400", "3000");
      assertTrue(statsPublisher.publishStat("file_00001_a", stats));
      fillStatMap("500", "4000");
      assertTrue(statsPublisher.publishStat("file_00001_b", stats));

      // aggregate existing stats
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("500", rows0);
      String usize0 = statsAggregator.aggregateStats("file_00000",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("3000", usize0);

      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("900", rows1);
      String usize1 = statsAggregator.aggregateStats("file_00001",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("7000", usize1);

      // aggregate non-existent stats
      String rowsX = statsAggregator.aggregateStats("file_00002", StatsSetupConst.ROW_COUNT);
      assertEquals("0", rowsX);
      String usizeX = statsAggregator.aggregateStats("file_00002",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("0", usizeX);

      assertTrue(statsAggregator.cleanUp("file_0000"));

      // close connections
      assertTrue(statsPublisher.closeConnection(sc));
      assertTrue(statsAggregator.closeConnection(sc));

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
      StatsCollectionContext sc = new StatsCollectionContext(conf);
      assertTrue(statsPublisher.init(sc));
      assertTrue(statsPublisher.connect(sc));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = factory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(sc));

      // publish stats
      fillStatMap("200", "1000");
      assertTrue(statsPublisher.publishStat("file_00000_a", stats));
      fillStatMap("300", "2000");
      assertTrue(statsPublisher.publishStat("file_00000_b", stats));

      fillStatMap("400", "3000");
      assertTrue(statsPublisher.publishStat("file_00001_a", stats));
      fillStatMap("500", "4000");
      assertTrue(statsPublisher.publishStat("file_00001_b", stats));

      // update which should not take any effect
      fillStatMap("190", "1000");
      assertTrue(statsPublisher.publishStat("file_00000_a", stats));
      fillStatMap("290", "2000");
      assertTrue(statsPublisher.publishStat("file_00000_b", stats));

      // update that should take effect
      fillStatMap("500", "5000");
      assertTrue(statsPublisher.publishStat("file_00001_a", stats));
      fillStatMap("600", "6000");
      assertTrue(statsPublisher.publishStat("file_00001_b", stats));

      // aggregate existing stats
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("500", rows0);
      String usize0 = statsAggregator.aggregateStats("file_00000",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("3000", usize0);

      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("1100", rows1);
      String usize1 = statsAggregator.aggregateStats("file_00001",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("11000", usize1);

      assertTrue(statsAggregator.cleanUp("file_0000"));

      // close connections
      assertTrue(statsPublisher.closeConnection(sc));
      assertTrue(statsAggregator.closeConnection(sc));

      System.out.println("StatsPublisher - multiple updates - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStatsPublisherMultipleUpdatesSubsetStatistics() throws Throwable {
    try {
      System.out
          .println("StatsPublisher - (multiple updates + publishing subset of supported statistics)");

      // instantiate stats publisher
      StatsPublisher statsPublisher = Utilities.getStatsPublisher((JobConf) conf);
      assertNotNull(statsPublisher);
      StatsCollectionContext sc = new StatsCollectionContext(conf);
      assertTrue(statsPublisher.init(sc));
      assertTrue(statsPublisher.connect(sc));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = factory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(sc));

      // publish stats
      fillStatMap("200", "");
      assertTrue(statsPublisher.publishStat("file_00000_a", stats));
      fillStatMap("300", "2000");
      assertTrue(statsPublisher.publishStat("file_00000_b", stats));


      // aggregate existing stats
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("500", rows0);
      String usize0 = statsAggregator.aggregateStats("file_00000",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("2000", usize0);

      // update which should not take any effect - plus the map published is a supset of supported
      // stats
      fillStatMap("190", "");
      assertTrue(statsPublisher.publishStat("file_00000_a", stats));
      fillStatMap("290", "");
      assertTrue(statsPublisher.publishStat("file_00000_b", stats));

      // nothing changed
      rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("500", rows0);
      usize0 = statsAggregator.aggregateStats("file_00000",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("2000", usize0);

      fillStatMap("500", "");
      assertTrue(statsPublisher.publishStat("file_00000_a", stats));
      fillStatMap("500", "");
      assertTrue(statsPublisher.publishStat("file_00000_b", stats));

      // changed + the rawDataSize size was overwriten !!!
      rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("1000", rows0);
      usize0 = statsAggregator.aggregateStats("file_00000",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("0", usize0);

      assertTrue(statsAggregator.cleanUp("file_0000"));

      // close connections
      assertTrue(statsPublisher.closeConnection(sc));
      assertTrue(statsAggregator.closeConnection(sc));

      System.out
          .println("StatsPublisher - (multiple updates + publishing subset of supported statistics) - OK");
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
      StatsCollectionContext sc = new StatsCollectionContext(conf);
      assertTrue(statsPublisher.init(sc));
      assertTrue(statsPublisher.connect(sc));

      // instantiate stats aggregator
      StatsAggregator statsAggregator = factory.getStatsAggregator();
      assertNotNull(statsAggregator);
      assertTrue(statsAggregator.connect(sc));

      // publish stats
      fillStatMap("200", "1000");
      assertTrue(statsPublisher.publishStat("file_00000_a", stats));
      fillStatMap("300", "2000");
      assertTrue(statsPublisher.publishStat("file_00000_b", stats));

      fillStatMap("400", "3000");
      assertTrue(statsPublisher.publishStat("file_00001_a", stats));
      fillStatMap("500", "4000");
      assertTrue(statsPublisher.publishStat("file_00001_b", stats));

      // cleanUp
      assertTrue(statsAggregator.cleanUp("file_00000"));

      // now clean-up just for one key
      String rows0 = statsAggregator.aggregateStats("file_00000", StatsSetupConst.ROW_COUNT);
      assertEquals("0", rows0);
      String usize0 = statsAggregator.aggregateStats("file_00000",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("0", usize0);

      // this should still be in the table
      String rows1 = statsAggregator.aggregateStats("file_00001", StatsSetupConst.ROW_COUNT);
      assertEquals("900", rows1);
      String usize1 = statsAggregator.aggregateStats("file_00001",
          StatsSetupConst.RAW_DATA_SIZE);
      assertEquals("7000", usize1);

      assertTrue(statsAggregator.cleanUp("file_0000"));

      // close connections
      assertTrue(statsPublisher.closeConnection(sc));
      assertTrue(statsAggregator.closeConnection(sc));

      System.out.println("StatsAggregator - clean-up - OK");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
