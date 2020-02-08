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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// this test is to ensure number of calls to metastore server by query compiler
public class TestNumMetastoreCalls {
  static HiveConf hConf = null;
  static Driver driver = null;

  @BeforeClass
  public static void Setup() throws Exception {
    hConf = new HiveConf(Driver.class);
    driver = setUpImpl(hConf);
    driver.run("create table t1(id1 int, name1 string)");
    driver.run("create table t2(id2 int, id1 int, name2 string)");
    driver.run("create database db1");
    driver.run("create table db1.tdb1(id2 int, id1 int, name2 string)");
    driver.run("create table tpart(id2 int, id1 int)"
        + " partitioned by (name string)");
    driver.run("alter table tpart add partition (name='p1')") ;
    driver.run("alter table tpart add partition (name='p2')") ;
  }

  @AfterClass
  public static void Teardown() throws Exception {
    driver.run("drop table t1");
    driver.run("drop table t2");
    driver.run("drop table db1.tdb1");
    driver.run("drop database db1 cascade");
  }

  private static Driver setUpImpl(HiveConf hiveConf) throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.RAW_STORE_IMPL,
        "org.apache.hadoop.hive.ql.TestNumMetastoreCallsObjectStore");
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.SCHEDULED_QUERIES_ENABLED, false);
    SessionState.start(hiveConf);
    return new Driver(hiveConf);
  }

  // compiler should do 6 metastore calls for each table reference
  // get table, get table col statistics
  // pk, fk, unique, not null constraints
  // for partitioned table there would be an extra call to get partitions
  @Test
  public void testSelectQuery() {
    int numCallsBefore = TestNumMetastoreCallsObjectStore.getNumCalls();
    int numCallsAfter = 0;

    // simple select *
    String query1 = "select * from t1";
    int rc = driver.compile(query1, true);
    assert(rc==0);
    numCallsAfter = TestNumMetastoreCallsObjectStore.getNumCalls();
    assert((numCallsAfter - numCallsBefore) == 6);

    // single table
    numCallsBefore = TestNumMetastoreCallsObjectStore.getNumCalls();
    String query2 = "select count(distinct id1) from t1 group by name1";
    rc = driver.compile(query2, true);
    assert(rc==0);
    numCallsAfter = TestNumMetastoreCallsObjectStore.getNumCalls();
    assert((numCallsAfter - numCallsBefore) == 6);

    // two different tables
    numCallsBefore = TestNumMetastoreCallsObjectStore.getNumCalls();
    String query3 = "select count(*) from t1 join t2 on t1.id1 = t2.id1 "
        + "where t2.id2 > 0 group by t1.name1, t2.name2";
    rc = driver.compile(query3, true);
    assert(rc==0);
    numCallsAfter = TestNumMetastoreCallsObjectStore.getNumCalls();
    assert((numCallsAfter - numCallsBefore) == 12 );

    //from different dbs
    numCallsBefore = TestNumMetastoreCallsObjectStore.getNumCalls();
    String query4 = "select count(*) from t1 join db1.tdb1 as t2 on t1.id1 = t2.id1 "
        + "where t2.id2 > 0 group by t1.name1, t2.name2";
    rc = driver.compile(query4, true);
    assert(rc==0);
    numCallsAfter = TestNumMetastoreCallsObjectStore.getNumCalls();
    assert((numCallsAfter - numCallsBefore) == 12);

    // three table join
    numCallsBefore = TestNumMetastoreCallsObjectStore.getNumCalls();
    String query5 = "select count(*) from t1 join db1.tdb1 as dbt2 on t1.id1 = dbt2.id1 "
        + "join t2 on t1.id1 = t2.id1 "
        + "where t2.id2 > 0 group by t1.name1, t2.name2";
    rc = driver.compile(query5, true);
    assert(rc==0);
    numCallsAfter = TestNumMetastoreCallsObjectStore.getNumCalls();
    assert((numCallsAfter - numCallsBefore) == 18);

    // single partitioned table
    numCallsBefore = TestNumMetastoreCallsObjectStore.getNumCalls();
    String query6 = "select count(distinct id1) from tpart group by name";
    rc = driver.compile(query6, true);
    assert(rc==0);
    numCallsAfter = TestNumMetastoreCallsObjectStore.getNumCalls();
    assert((numCallsAfter - numCallsBefore) == 7);

    // two different tables
    numCallsBefore = TestNumMetastoreCallsObjectStore.getNumCalls();
    String query7 = "select count(*) from t1 join tpart on t1.id1 = tpart.id1 "
        + "where tpart.id2 > 0 group by t1.name1, tpart.name";
    rc = driver.compile(query7, true);
    assert(rc==0);
    numCallsAfter = TestNumMetastoreCallsObjectStore.getNumCalls();
    assert((numCallsAfter - numCallsBefore)  == 13);
  }
}
