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
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.logging.log4j.core.util.ReflectionUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * Test if the small table cache is evicted, when a new query is executed.
 */
@Ignore("HIVE-22944: Kryo 5 upgrade conflicts with Spark, which is not supported anymore")
public class TestSmallTableCacheEviction {

  private String smallTableName1;
  private String smallTableName2;
  private String largeTableName;
  private SmallTableCache.SmallTableLocalCache<String, MapJoinTableContainer> innerCache;
  private HiveConf conf;

  @Before
  public void setUp() throws Exception {
    String confDir = "../../data/conf/spark/local/hive-site.xml";
    HiveConf.setHiveSiteLocation(new File(confDir).toURI().toURL());
    conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN_TOMAPJOIN, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN, true);
    conf.set("spark.local.dir", Paths.get(System.getProperty("test.tmp.dir"),
            "TestSmallTableCacheEviction-local-dir").toString());

    SessionState.start(conf);

    largeTableName = "large" + UUID.randomUUID().toString().replace("-", "");
    smallTableName1 = "small" + UUID.randomUUID().toString().replace("-", "");
    smallTableName2 = "small" + UUID.randomUUID().toString().replace("-", "");

    innerCache = (SmallTableCache.SmallTableLocalCache<String, MapJoinTableContainer>) ReflectionUtil
            .getStaticFieldValue(SmallTableCache.class.getDeclaredField("TABLE_CONTAINER_CACHE"));

    // Create test data
    Driver driver = createDriver();
    MockDataBuilder.builder(smallTableName1).numberOfRows(1).create(driver);
    MockDataBuilder.builder(smallTableName2).numberOfRows(2).create(driver);
    MockDataBuilder.builder(largeTableName).numberOfRows(3).create(driver);
    driver.destroy();
  }

  @After
  public void tearDown() throws CommandProcessorException {
    Driver driver = createDriver();
    driver.run("drop table if exists " + smallTableName1);
    driver.run("drop table if exists " + smallTableName2);
    driver.run("drop table if exists " + largeTableName);
    driver.destroy();
  }

  @Test
  public void testSmallTableEvictionIfNewQueryIsExecuted() throws Exception {

    for (int i = 0; i < 2; i++) {
      Driver driver = null;
      try {
        driver = createDriver();
        String simpleJoinQuery = "select large.col, s1.col, s2.col from " + largeTableName + " large join " +
                smallTableName1 + " s1 on s1.col = large.col join " + smallTableName2 + " s2 on s2.col = large.col";
        driver.run(simpleJoinQuery);
        Assert.assertEquals(2, innerCache.size());
      } finally {
        if (driver != null) {
          driver.destroy();
        }
      }
    }
  }

  private Driver createDriver() {
    return new Driver(new QueryState.Builder()
            .withGenerateNewQueryId(true)
            .withHiveConf(conf).build(),
            null);
  }

  /**
   * Helper class to create a simple table with n number of rows in it.
   */
  private static final class MockDataBuilder {

    private final String tableName;
    private int numberOfRows;

    private MockDataBuilder(String tableName) {
      this.tableName = tableName;
    }

    public MockDataBuilder numberOfRows(int numberOfRows) {
      this.numberOfRows = numberOfRows;
      return this;
    }

    public void create(Driver driver) throws CommandProcessorException {
      driver.run("create table " + tableName + " (col int)");

      if (numberOfRows > 0) {
        StringJoiner query = new StringJoiner(",", "insert into " + tableName + " values ", "");
        for (int i = 0; i < numberOfRows; i++) {
          query.add("(" + Integer.toString(i + 1) + ")");
        }
        driver.run(query.toString());
      }
    }

    public static MockDataBuilder builder(String tableName) {
      return new MockDataBuilder(tableName);
    }

  }
}
