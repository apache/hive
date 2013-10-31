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

package org.apache.hadoop.hive.metastore;



import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
/**
 * TestMetaStoreEventListener. Test case for
 * {@link org.apache.hadoop.hive.metastore.MetaStoreEndFunctionListener}
 */
public class TestMetaStoreEndFunctionListener extends TestCase {
  private HiveConf hiveConf;
  private HiveMetaStoreClient msc;
  private Driver driver;

  @Override
  protected void setUp() throws Exception {

    super.setUp();
    System.setProperty("hive.metastore.event.listeners",
        DummyListener.class.getName());
    System.setProperty("hive.metastore.pre.event.listeners",
        DummyPreListener.class.getName());
    System.setProperty("hive.metastore.end.function.listeners",
        DummyEndFunctionListener.class.getName());
    int port = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());
    hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(new CliSessionState(hiveConf));
    msc = new HiveMetaStoreClient(hiveConf, null);
    driver = new Driver(hiveConf);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testEndFunctionListener() throws Exception {
    /* Objective here is to ensure that when exceptions are thrown in HiveMetaStore in API methods
     * they bubble up and are stored in the MetaStoreEndFunctionContext objects
     */
    String dbName = "hive3524";
    String tblName = "tmptbl";
    int listSize = 0;

    driver.run("create database " + dbName);

    try {
      msc.getDatabase("UnknownDB");
    }
    catch (Exception e) {
    }
    listSize = DummyEndFunctionListener.funcNameList.size();
    String func_name = DummyEndFunctionListener.funcNameList.get(listSize-1);
    MetaStoreEndFunctionContext context = DummyEndFunctionListener.contextList.get(listSize-1);
    assertEquals(func_name,"get_database");
    assertFalse(context.isSuccess());
    Exception e = context.getException();
    assertTrue((e!=null));
    assertTrue((e instanceof NoSuchObjectException));
    assertEquals(context.getInputTableName(), null);

    driver.run("use " + dbName);
    driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));
    String tableName = "Unknown";
    try {
      msc.getTable(dbName, tableName);
    }
    catch (Exception e1) {
    }
    listSize = DummyEndFunctionListener.funcNameList.size();
    func_name = DummyEndFunctionListener.funcNameList.get(listSize-1);
    context = DummyEndFunctionListener.contextList.get(listSize-1);
    assertEquals(func_name,"get_table");
    assertFalse(context.isSuccess());
    e = context.getException();
    assertTrue((e!=null));
    assertTrue((e instanceof NoSuchObjectException));
    assertEquals(context.getInputTableName(), tableName);

    try {
      msc.getPartition("hive3524", tblName, "b=2012");
    }
    catch (Exception e2) {
    }
    listSize = DummyEndFunctionListener.funcNameList.size();
    func_name = DummyEndFunctionListener.funcNameList.get(listSize-1);
    context = DummyEndFunctionListener.contextList.get(listSize-1);
    assertEquals(func_name,"get_partition_by_name");
    assertFalse(context.isSuccess());
    e = context.getException();
    assertTrue((e!=null));
    assertTrue((e instanceof NoSuchObjectException));
    assertEquals(context.getInputTableName(), tblName);
    try {
      driver.run("drop table Unknown");
    }
    catch (Exception e4) {
    }
    listSize = DummyEndFunctionListener.funcNameList.size();
    func_name = DummyEndFunctionListener.funcNameList.get(listSize-1);
    context = DummyEndFunctionListener.contextList.get(listSize-1);
    assertEquals(func_name,"get_table");
    assertFalse(context.isSuccess());
    e = context.getException();
    assertTrue((e!=null));
    assertTrue((e instanceof NoSuchObjectException));
    assertEquals(context.getInputTableName(), "Unknown");

  }

}
