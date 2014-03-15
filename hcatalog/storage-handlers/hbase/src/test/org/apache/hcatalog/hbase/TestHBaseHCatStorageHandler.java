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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerConfiguration;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseHCatStorageHandler extends SkeletonHBaseTest {

  private static HiveConf   hcatConf;
  private static HCatDriver hcatDriver;
  private static Warehouse  wh;

  @BeforeClass
  public static void setup() throws Throwable {
    setupSkeletonHBaseTest();
  }

  public void Initialize() throws Exception {

    hcatConf = getHiveConf();
    hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HCatSemanticAnalyzer.class.getName());
    URI fsuri = getFileSystem().getUri();
    Path whPath = new Path(fsuri.getScheme(), fsuri.getAuthority(),
        getTestDir());
    hcatConf.set(HiveConf.ConfVars.HADOOPFS.varname, fsuri.toString());
    hcatConf.set(ConfVars.METASTOREWAREHOUSE.varname, whPath.toString());

    //Add hbase properties
    for (Map.Entry<String, String> el : getHbaseConf()) {
      if (el.getKey().startsWith("hbase.")) {
        hcatConf.set(el.getKey(), el.getValue());
      }
    }
    HBaseConfiguration.merge(
        hcatConf,
        RevisionManagerConfiguration.create());

    SessionState.start(new CliSessionState(hcatConf));
    hcatDriver = new HCatDriver();

  }

  @Test
  public void testTableCreateDrop() throws Exception {
    Initialize();

    hcatDriver.run("drop table test_table");
    CommandProcessorResponse response = hcatDriver
        .run("create table test_table(key int, value string) STORED BY " +
                     "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
          + "TBLPROPERTIES ('hbase.columns.mapping'=':key,cf1:val')");

    assertEquals(0, response.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists("test_table");

    assertTrue(doesTableExist);

    RevisionManager rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(hcatConf);
    rm.open();
    //Should be able to successfully query revision manager
    rm.getAbortedWriteTransactions("test_table", "cf1");

    hcatDriver.run("drop table test_table");
    doesTableExist = hAdmin.tableExists("test_table");
    assertTrue(doesTableExist == false);

    try {
      rm.getAbortedWriteTransactions("test_table", "cf1");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof NoNodeException);
    }
    rm.close();

  }

  @Test
  public void testTableCreateDropDifferentCase() throws Exception {
    Initialize();

    hcatDriver.run("drop table test_Table");
    CommandProcessorResponse response = hcatDriver
        .run("create table test_Table(key int, value string) STORED BY " +
               "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
          + "TBLPROPERTIES ('hbase.columns.mapping'=':key,cf1:val')");

    assertEquals(0, response.getResponseCode());

    //HBase table gets created with lower case unless specified as a table property.
    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists("test_table");

    assertTrue(doesTableExist);

    RevisionManager rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(hcatConf);
    rm.open();
    //Should be able to successfully query revision manager
    rm.getAbortedWriteTransactions("test_table", "cf1");

    hcatDriver.run("drop table test_table");
    doesTableExist = hAdmin.tableExists("test_table");
    assertTrue(doesTableExist == false);

    try {
      rm.getAbortedWriteTransactions("test_table", "cf1");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof NoNodeException);
    }
    rm.close();

  }

  @Test
  public void testTableCreateDropCaseSensitive() throws Exception {
    Initialize();

    hcatDriver.run("drop table test_Table");
    CommandProcessorResponse response = hcatDriver
        .run("create table test_Table(key int, value string) STORED BY " +
               "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
          + "TBLPROPERTIES ('hbase.columns.mapping'=':key,cf1:val'," +
          " 'hbase.table.name'='CaseSensitiveTable')");

    assertEquals(0, response.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists("CaseSensitiveTable");

    assertTrue(doesTableExist);

    RevisionManager rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(hcatConf);
    rm.open();
    //Should be able to successfully query revision manager
    rm.getAbortedWriteTransactions("CaseSensitiveTable", "cf1");

    hcatDriver.run("drop table test_table");
    doesTableExist = hAdmin.tableExists("CaseSensitiveTable");
    assertTrue(doesTableExist == false);

    try {
      rm.getAbortedWriteTransactions("CaseSensitiveTable", "cf1");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof NoNodeException);
    }
    rm.close();

  }

  @Test
  public void testTableDropNonExistent() throws Exception {
    Initialize();

    hcatDriver.run("drop table mytable");
    CommandProcessorResponse response = hcatDriver
        .run("create table mytable(key int, value string) STORED BY " +
           "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
          + "TBLPROPERTIES ('hbase.columns.mapping'=':key,cf1:val')");

    assertEquals(0, response.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists("mytable");
    assertTrue(doesTableExist);

    //Now delete the table from hbase
    if (hAdmin.isTableEnabled("mytable")) {
      hAdmin.disableTable("mytable");
    }
    hAdmin.deleteTable("mytable");
    doesTableExist = hAdmin.tableExists("mytable");
    assertTrue(doesTableExist == false);

    CommandProcessorResponse responseTwo = hcatDriver.run("drop table mytable");
    assertTrue(responseTwo.getResponseCode() == 0);

  }

  @Test
  public void testTableCreateExternal() throws Exception {

    String tableName = "testTable";
    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());

    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("key")));
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("familyone")));
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("familytwo")));

    hAdmin.createTable(tableDesc);
    boolean doesTableExist = hAdmin.tableExists(tableName);
    assertTrue(doesTableExist);

    hcatDriver.run("drop table mytabletwo");
    CommandProcessorResponse response = hcatDriver
        .run("create external table mytabletwo(key int, valueone string, valuetwo string) STORED BY " +
           "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
          + "TBLPROPERTIES ('hbase.columns.mapping'=':key,familyone:val,familytwo:val'," +
          "'hbase.table.name'='testTable')");

    assertEquals(0, response.getResponseCode());

  }


}
