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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This test tries different methods of lookup to try and determine the most efficient in
 * the particular usage case related to checking tables against permissions.
 * Note that a meaningful performance test requires at least 20 dbs, 10000 tables per db and 40 loops
 * on a 2022 laptop.
 */
public class TestTablePermissionFilterAlgos {
  private static final Logger LOGGER = LogManager.getLogger(TestHiveMetaStoreAuthorizer.class);
  // reduce constants to speed up tests when true, set to false for benchmarking
  private static final boolean TEST = true;
  // Whether we shuffle table names or not: this has an important effect on sort speed - mostly sorted
  // arrays sort faster than completely unsorted ones.
  // A hashset of concatenated strings seem to be faster than a sort in that case.
  // I suspect something fishy in the cost of string creation (intern()?) since a hashset of pairs is much slower.
  // We'll make the semi-educated assumption that most of the time, tablenames come back mostly ordered.
  private static final boolean SHUFFLE_TABLENAMES = !TEST;
  private static final int NUM_DB = TEST? 2 : 20;
  private static final int NUM_TBL = TEST? 100 : 10000;
  private static final int NLOOPS = TEST? 1 : 40;

  private final List<TestTable> tables = createTables(NUM_DB, NUM_TBL);
  // lookup some
  private final List<TestTable> filtered = tables.stream()
      .filter(t -> t.getTableName().endsWith("0"))
      .collect(Collectors.toList());

  /**
   * Fake tables to test.
   */
  private static class TestTable {
    private int hashCode = -1;
    private final String dbName;
    private final String tableName;

    private TestTable(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    @Override
    public String toString() {
      return tableFullName(this);
    }

    public String getDbName() {
      return dbName;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof TestTable) {
        TestTable tt = (TestTable) o;
        return dbName.equals(tt.dbName) && tableName.equals(tt.tableName);
      }
      return false;
    }

    @Override
    public int hashCode() {
       int h = hashCode;
       if (h < 0) {
         hashCode = h = Math.abs((31 * dbName.hashCode()) + tableName.hashCode());
       }
       return h;
    }
  }

  /**
   * Creates a list of tables.
   * @param dbs number of databases
   * @param tbls number of tables per database
   * @return a list of tables
   */
  private static List<TestTable> createTables(int dbs, int tbls) {
    Random rnd = new Random(20230815L);
    List<Integer> dbis = new ArrayList<>(dbs);
    for(int i = 0; i < dbs; ++i) {
      dbis.add(i);
    }
    Collections.shuffle(dbis, rnd);
    List<Integer> tblis = new ArrayList<>(tbls);
    for(int i = 0; i < tbls; ++i) {
      tblis.add(i);
    }
    if (SHUFFLE_TABLENAMES) {
      Collections.shuffle(tblis, rnd);
    }

    List<TestTable> tables  = new ArrayList<>(dbs * tbls);
    for(int d : dbis) {
      for(int t : tblis) {
        String dbname = String.format("db%05x", d);
        String tblName = String.format("tbl%05x", t);
        tables.add(new TestTable(dbname, tblName));
      }
    }
    return tables;
  }

  /**
   * Allows timing code blocks execution precisely.
   */
  static class Chrono {
    long start;
    long end;

    void start() {
      start = System.currentTimeMillis();
    }
    void stop() {
      end = System.currentTimeMillis();
    }
    String elapse() {
      return String.format("%.3f", (double) (end - start) / 1000d);
    }
  }

  /**
   * Specialized for TestTable purpose.
   */
  private static class TestLookup extends HivePrivilegeObjectUtils.TableLookup<TestTable> {
    protected TestLookup(List<TestTable> tables) {
      super(tables);
    }

    @Override protected String getDbName(TestTable o) {
      return o.getDbName();
    }

    @Override protected String getTableName(TestTable o) {
      return o.getTableName();
    }
  };

  @Test
  public void testIndexedLookup() {
    System.gc();
    Chrono chrono = new Chrono();
    chrono.start();
    int cnt = 0;
    for(int l = 0; l < NLOOPS; ++l) {
      TestLookup index = new TestLookup(tables);
      List<TestTable> tts = l % 2 == 1
          ? tables
          : filtered;
        cnt = 0;
        // lookup all
        for (TestTable tt : tts) {
          if (index.contains(tt)) {
            cnt += 1;
          }
        }
      Assert.assertEquals(cnt, tts.size());
    }
    chrono.stop();
    LOGGER.info("lookup elapse " + chrono.elapse());
  }

  private static ImmutablePair<String,String> tableKey(TestTable tt) {
    return new ImmutablePair<>(tt.getDbName(), tt.getTableName());
  }

  @Test
  public void testKeyPairLookup() {
    System.gc();
    Chrono chrono = new Chrono();
    chrono.start();
    for(int l = 0; l < NLOOPS; ++l) {
      Set<ImmutablePair<String, String>> index = tables.stream()
          .map(TestTablePermissionFilterAlgos::tableKey)
          .collect(Collectors.toSet());
      List<TestTable> tts = l % 2 == 1
          ? tables
          : filtered;
      int cnt = 0;
      // lookup all
      for (TestTable tt : tts) {
        if (index.contains(tableKey(tt))) {
          cnt += 1;
        }
      }
      Assert.assertEquals(cnt, tts.size());
    }
    chrono.stop();
    LOGGER.info("set pair elapse " + chrono.elapse());
  }

  @Test
  public void testTestTableLookup() {
    System.gc();
    Chrono chrono = new Chrono();
    chrono.start();
    for(int l = 0; l < NLOOPS; ++l) {
      Set<TestTable> index = new HashSet<>(tables);
      List<TestTable> tts = l % 2 == 1
          ? tables
          : filtered;
      int cnt = 0;
      // lookup all
      for (TestTable tt : tts) {
        if (index.contains(tt)) {
          cnt += 1;
        }
      }
      Assert.assertEquals(cnt, tts.size());
    }
    chrono.stop();
    LOGGER.info("set table elapse " + chrono.elapse());
  }


  private static String tableFullName(TestTable tt) {
    return tt.getDbName() + "." + tt.getTableName();
  }

  @Test
  public void testFullNameLookup() {
    System.gc();
    Chrono chrono = new Chrono();
    chrono.start();
    for(int l = 0; l < NLOOPS; ++l) {
      Set<String> index = tables.stream()
          .map(TestTablePermissionFilterAlgos::tableFullName)
          .collect(Collectors.toSet());
      List<TestTable> tts = l % 2 == 1
          ? tables
          : filtered;
      int cnt = 0;
      // lookup all
      for (TestTable tt : tts) {
        if (index.contains(tableFullName(tt))) {
          cnt += 1;
        }
      }
      Assert.assertEquals(cnt, tts.size());
    }
    chrono.stop();
    LOGGER.info("set fullname elapse " + chrono.elapse());
  }
}
