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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for TxnUtils
 */
public class TestTxnUtils {
  private HiveConf conf;

  public TestTxnUtils() throws Exception {
  }

  @Test
  public void testBuildQueryWithINClause() throws Exception {
    List<String> queries = new ArrayList<String>();

    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();

    // Note, this is a "real" query that depends on one of the metastore tables
    prefix.append("select count(*) from TXNS where ");
    suffix.append(" and TXN_STATE = 'o'");

    // Case 1 - Max in list members: 10; Max query string length: 1KB
    //          The first query happens to have 2 full batches.
    conf.setIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_QUERY_LENGTH, 1);
    conf.setIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE, 10);
    List<Long> inList = new ArrayList<Long>();
    for (long i = 1; i <= 200; i++) {
      inList.add(i);
    }
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, inList, "TXN_ID", true, false);
    Assert.assertEquals(1, queries.size());
    runAgainstDerby(queries);

    // Case 2 - Max in list members: 10; Max query string length: 1KB
    //          The first query has 2 full batches, and the second query only has 1 batch which only contains 1 member
    queries.clear();
    inList.add((long)201);
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, inList, "TXN_ID", true, false);
    Assert.assertEquals(2, queries.size());
    runAgainstDerby(queries);

    // Case 3 - Max in list members: 1000; Max query string length: 5KB
    conf.setIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_QUERY_LENGTH, 10);
    conf.setIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE, 1000);
    queries.clear();
    for (long i = 202; i <= 4321; i++) {
      inList.add(i);
    }
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, inList, "TXN_ID", true, false);
    Assert.assertEquals(3, queries.size());
    runAgainstDerby(queries);

    // Case 4 - NOT IN list
    queries.clear();
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, inList, "TXN_ID", true, true);
    Assert.assertEquals(3, queries.size());
    runAgainstDerby(queries);

    // Case 5 - No parenthesis
    queries.clear();
    suffix.setLength(0);
    suffix.append("");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, inList, "TXN_ID", false, false);
    Assert.assertEquals(3, queries.size());
    runAgainstDerby(queries);
  }

  /** Verify queries can run against Derby DB.
   *  As long as Derby doesn't complain, we assume the query is syntactically/semantically correct.
   */
  private void runAgainstDerby(List<String> queries) throws Exception {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;

    try {
      conn = TxnDbUtil.getConnection();
      stmt = conn.createStatement();
      for (String query : queries) {
        rs = stmt.executeQuery(query);
        Assert.assertTrue("The query is not valid", rs.next());
      }
    } finally {
      TxnDbUtil.closeResources(conn, stmt, rs);
    }
  }

  @Before
  public void setUp() throws Exception {
    tearDown();
    conf = new HiveConf(this.getClass());
    TxnDbUtil.setConfValues(conf);
    TxnDbUtil.prepDb();
  }

  @After
  public void tearDown() throws Exception {
    TxnDbUtil.cleanDb();
  }
}
