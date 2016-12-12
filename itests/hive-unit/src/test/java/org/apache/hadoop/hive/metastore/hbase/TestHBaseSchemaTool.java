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
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestHBaseSchemaTool extends HBaseIntegrationTests {

  private final String lsep = System.getProperty("line.separator");

  @BeforeClass
  public static void startup() throws Exception {
    HBaseIntegrationTests.startMiniCluster();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    HBaseIntegrationTests.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    setupHBaseStore();
  }

  @Test
  public void listTables() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(true, null, null, null, conf, out, err);
    Assert.assertEquals(StringUtils.join(HBaseReadWrite.tableNames, lsep) + lsep,
        outStr.toString());
  }

  @Test
  public void bogusTable() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, "nosuch", null, null, conf, out, err);
    Assert.assertEquals("Unknown table: nosuch" + lsep, errStr.toString());
  }

  @Test
  public void noSuchDb() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.DB_TABLE, "nosuch", null, conf, out, err);
    Assert.assertEquals("No such database: nosuch" + lsep, outStr.toString());
  }

  @Test
  public void noMatchingDb() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.DB_TABLE, null, "nomatch", conf, out, err);
    Assert.assertEquals("No matching database: nomatch" + lsep, outStr.toString());
  }

  @Test
  public void noSuchRole() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.ROLE_TABLE, "nosuch", null, conf, out, err);
    Assert.assertEquals("No such role: nosuch" + lsep, outStr.toString());
  }

  @Test
  public void noMatchingRole() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.ROLE_TABLE, null, "nomatch", conf, out, err);
    Assert.assertEquals("No matching role: nomatch" + lsep, outStr.toString());
  }

  @Test
  public void noSuchUser() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.USER_TO_ROLE_TABLE, "nosuch", null, conf, out, err);
    Assert.assertEquals("No such user: nosuch" + lsep, outStr.toString());
  }

  @Test
  public void noMatchingUser() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.USER_TO_ROLE_TABLE, null, "nomatch", conf, out, err);
    Assert.assertEquals("No matching user: nomatch" + lsep, outStr.toString());
  }

  @Test
  public void noSuchFunction() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.FUNC_TABLE, "nosuch", null, conf, out,  err);
    Assert.assertEquals("No such function: nosuch" + lsep, outStr.toString());
  }

  @Test
  public void noMatchingFunction() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.FUNC_TABLE, null, "nomatch", conf, out,
        err);
    Assert.assertEquals("No matching function: nomatch" + lsep, outStr.toString());
  }

  @Test
  public void noSuchTable() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.TABLE_TABLE, "nosuch", null, conf, out, err);
    Assert.assertEquals("No such table: nosuch" + lsep, outStr.toString());
  }

  @Test
  public void noMatchingTable() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.TABLE_TABLE, null, "nomatch", conf, out, err);
    Assert.assertEquals("No matching table: nomatch" + lsep, outStr.toString());
  }

  @Test
  public void noSuchPart() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.PART_TABLE, "nosuch", null, conf, out, err);
    Assert.assertEquals("No such partition: nosuch" + lsep, outStr.toString());
  }

  @Test
  public void noSuchPartValidFormat() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);
    // Test with something that looks like a valid entry
    new HBaseSchemaTool().go(false, HBaseReadWrite.PART_TABLE, "default.nosuch.nosuch", null, conf,
        out, err);
    Assert.assertEquals("No such partition: default.nosuch.nosuch" + lsep, outStr.toString());
  }


  @Test
  public void noMatchingPart() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.PART_TABLE, null, "nomatch", conf, out, err);
    Assert.assertEquals("No matching partition: nomatch" + lsep, outStr.toString());
  }

  @Test
  public void noMatchingPartValidFormat() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    new HBaseSchemaTool().go(false, HBaseReadWrite.PART_TABLE, null, "nomatch.a.b", conf, out, err);
    Assert.assertEquals("No matching partition: nomatch.a.b" + lsep, outStr.toString());
  }

  @Test
  public void noSuchStorageDescriptor() throws Exception {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    // Strangly enough things don't come back quite the same when going through the Base64
    // encode/decode.
    new HBaseSchemaTool().go(false, HBaseReadWrite.SD_TABLE, "nosuch", null, conf, out, err);
    Assert.assertEquals("No such storage descriptor: nosucg" + lsep, outStr.toString());
  }

  @Test
  public void oneMondoTest() throws Exception {
    // This is a pain to do in one big test, but we have to control the order so that we have tests
    // without dbs, etc.
    HBaseSchemaTool tool = new HBaseSchemaTool();

    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    // This needs to be up front before we create any tables or partitions
    tool.go(false, HBaseReadWrite.SD_TABLE, null, "whatever", conf, out, err);
    Assert.assertEquals("No storage descriptors" + lsep, outStr.toString());

    // This one needs to be up front too
    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.SEQUENCES_TABLE, null, "whatever", conf, out, err);
    Assert.assertEquals("No sequences" + lsep, outStr.toString());

    // Create some databases
    String[] dbNames = new String[3];
    for (int i = 0; i < dbNames.length; i++) {
      dbNames[i] = "db" + i;
      Database db = new Database(dbNames[i], "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
    }

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.DB_TABLE, "db0", null, conf, out, err);
    Assert.assertEquals("{\"name\":\"db0\",\"description\":\"no description\"," +
        "\"locationUri\":\"file:///tmp\",\"parameters\":{}}" + lsep, outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.DB_TABLE, null, ".*", conf, out, err);
    Assert.assertEquals("{\"name\":\"db0\",\"description\":\"no description\"," +
          "\"locationUri\":\"file:///tmp\",\"parameters\":{}}" + lsep +
          "{\"name\":\"db1\",\"description\":\"no description\"," +
          "\"locationUri\":\"file:///tmp\",\"parameters\":{}}" + lsep +
          "{\"name\":\"db2\",\"description\":\"no description\"," +
          "\"locationUri\":\"file:///tmp\",\"parameters\":{}}" + lsep,
        outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.DB_TABLE, null, "db[12]", conf, out, err);
    Assert.assertEquals("{\"name\":\"db1\",\"description\":\"no description\"," +
            "\"locationUri\":\"file:///tmp\",\"parameters\":{}}" + lsep +
            "{\"name\":\"db2\",\"description\":\"no description\"," +
            "\"locationUri\":\"file:///tmp\",\"parameters\":{}}" + lsep,
        outStr.toString());

    String[] roleNames = new String[2];
    for (int i = 0; i < roleNames.length; i++) {
      roleNames[i] = "role" + i;
      store.addRole(roleNames[i], "me");
    }
    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.ROLE_TABLE, null, "role.", conf, out, err);
    Assert.assertEquals("{\"roleName\":\"role0\",\"createTime\":now,\"ownerName\":\"me\"}" +
        lsep +  "{\"roleName\":\"role1\",\"createTime\":now,\"ownerName\":\"me\"}" + lsep,
        outStr.toString().replaceAll("createTime\":[0-9]+", "createTime\":now"));

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.ROLE_TABLE, "role1", null, conf, out, err);
    Assert.assertEquals("{\"roleName\":\"role1\",\"createTime\":now,\"ownerName\":\"me\"}" + lsep,
        outStr.toString().replaceAll("createTime\":[0-9]+", "createTime\":now"));

    Role role1 = store.getRole("role1");
    store.grantRole(role1, "fred", PrincipalType.USER, "me", PrincipalType.USER, false);
    store.grantRole(role1, "joanne", PrincipalType.USER, "me", PrincipalType.USER, false);

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.USER_TO_ROLE_TABLE, null, ".*", conf, out, err);
    Assert.assertEquals("fred: role1" + lsep + "joanne: role1" + lsep, outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.USER_TO_ROLE_TABLE, "joanne", null, conf, out, err);
    Assert.assertEquals("role1" + lsep, outStr.toString());

    String[] funcNames = new String[3];
    for (int i = 0; i < funcNames.length; i++) {
      funcNames[i] = "func" + i;
      Function function = new Function(funcNames[i], "db1", "Function", "me", PrincipalType.USER, 0,
          FunctionType.JAVA, null);
      store.createFunction(function);
    }
    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.FUNC_TABLE, "db1.func0", null, conf, out, err);
    Assert.assertEquals("{\"functionName\":\"func0\",\"dbName\":\"db1\"," +
        "\"className\":\"Function\",\"ownerName\":\"me\",\"ownerType\":1,\"createTime\":0," +
        "\"functionType\":1}" + lsep, outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.FUNC_TABLE, null, ".*", conf, out, err);
    Assert.assertEquals("{\"functionName\":\"func0\",\"dbName\":\"db1\"," +
        "\"className\":\"Function\",\"ownerName\":\"me\",\"ownerType\":1,\"createTime\":0," +
        "\"functionType\":1}" + lsep +
        "{\"functionName\":\"func1\",\"dbName\":\"db1\"," +
        "\"className\":\"Function\",\"ownerName\":\"me\",\"ownerType\":1,\"createTime\":0," +
        "\"functionType\":1}" + lsep +
        "{\"functionName\":\"func2\",\"dbName\":\"db1\"," +
        "\"className\":\"Function\",\"ownerName\":\"me\",\"ownerType\":1,\"createTime\":0," +
        "\"functionType\":1}" + lsep, outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.FUNC_TABLE, null, "db1.func[12]", conf, out, err);
    Assert.assertEquals("{\"functionName\":\"func1\",\"dbName\":\"db1\"," +
        "\"className\":\"Function\",\"ownerName\":\"me\",\"ownerType\":1,\"createTime\":0," +
        "\"functionType\":1}" + lsep +
        "{\"functionName\":\"func2\",\"dbName\":\"db1\"," +
        "\"className\":\"Function\",\"ownerName\":\"me\",\"ownerType\":1,\"createTime\":0," +
        "\"functionType\":1}" + lsep, outStr.toString());


    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.GLOBAL_PRIVS_TABLE, null, null, conf, out, err);
    Assert.assertEquals("No global privileges" + lsep, outStr.toString());

    List<HiveObjectPrivilege> privileges = new ArrayList<>();
    HiveObjectRef hiveObjRef = new HiveObjectRef(HiveObjectType.GLOBAL, "db0", "tab0", null,
        null);
    PrivilegeGrantInfo grantInfo =
        new PrivilegeGrantInfo("read", 0, "me", PrincipalType.USER, false);
    HiveObjectPrivilege hop = new HiveObjectPrivilege(hiveObjRef, "user", PrincipalType.USER,
        grantInfo);
    privileges.add(hop);

    grantInfo = new PrivilegeGrantInfo("create", 0, "me", PrincipalType.USER, true);
    hop = new HiveObjectPrivilege(hiveObjRef, "user", PrincipalType.USER, grantInfo);
    privileges.add(hop);

    PrivilegeBag pBag = new PrivilegeBag(privileges);
    store.grantPrivileges(pBag);

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.GLOBAL_PRIVS_TABLE, null, null, conf, out, err);
    Assert.assertEquals(
        "{\"userPrivileges\":{\"user\":[{\"privilege\":\"read\",\"createTime\":0," +
            "\"grantor\":\"me\",\"grantorType\":1,\"grantOption\":0},{\"privilege\":\"create\"," +
            "\"createTime\":0,\"grantor\":\"me\",\"grantorType\":1,\"grantOption\":1}]}}" + lsep,
        outStr.toString());


    String[] tableNames = new String[3];
    for (int i = 0; i < tableNames.length; i++) {
      tableNames[i] = "tab" + i;
      StorageDescriptor sd = new StorageDescriptor(Arrays.asList(new FieldSchema("col1", "int",
          ""), new FieldSchema("col2", "varchar(32)", "")),
          "/tmp", null, null, false, 0, null,  null, null, Collections.<String, String>emptyMap());
      Table tab = new Table(tableNames[i], dbNames[0], "me", 0, 0, 0, sd,
          Arrays.asList(new FieldSchema("pcol1", "string", ""),
              new FieldSchema("pcol2", "string", "")),
          Collections.<String, String>emptyMap(), null, null, null);
      store.createTable(tab);
    }

    ColumnStatisticsDesc tableStatsDesc = new ColumnStatisticsDesc(false, "db0", "tab0");
    ColumnStatisticsData tcsd = new ColumnStatisticsData();
    LongColumnStatsData tlcsd = new LongColumnStatsData(1, 2);
    tlcsd.setLowValue(-95);
    tlcsd.setHighValue(95);
    tcsd.setLongStats(tlcsd);
    ColumnStatisticsData tcsd2 = new ColumnStatisticsData();
    tcsd2.setStringStats(new StringColumnStatsData(97, 18.78, 29, 397));
    List<ColumnStatisticsObj> tcsos = Arrays.asList(
        new ColumnStatisticsObj("col1", "int", tcsd),
        new ColumnStatisticsObj("col2", "varchar(32)", tcsd2));
    ColumnStatistics tStatObj = new ColumnStatistics(tableStatsDesc, tcsos);
    store.updateTableColumnStatistics(tStatObj);

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.TABLE_TABLE, "db0.tab1", null, conf, out, err);
    Assert.assertEquals("{\"tableName\":\"tab1\",\"dbName\":\"db0\",\"owner\":\"me\"," +
        "\"createTime\":0,\"lastAccessTime\":0,\"retention\":0," +
        "\"partitionKeys\":[{\"name\":\"pcol1\",\"type\":\"string\",\"comment\":\"\"}," +
        "{\"name\":\"pcol2\",\"type\":\"string\",\"comment\":\"\"}],\"parameters\":{}," +
        "\"tableType\":\"\",\"rewriteEnabled\":0} sdHash: qQTgZAi5VzgpozzFGmIVTQ stats:" + lsep,
        outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.TABLE_TABLE, null, "db0.*", conf, out, err);
    Assert.assertEquals("{\"tableName\":\"tab0\",\"dbName\":\"db0\",\"owner\":\"me\"," +
        "\"createTime\":0,\"lastAccessTime\":0,\"retention\":0," +
        "\"partitionKeys\":[{\"name\":\"pcol1\",\"type\":\"string\",\"comment\":\"\"}," +
        "{\"name\":\"pcol2\",\"type\":\"string\",\"comment\":\"\"}],\"parameters\":{\"COLUMN_STATS_ACCURATE\":\"{\\\"COLUMN_STATS\\\":{\\\"col1\\\":\\\"true\\\",\\\"col2\\\":\\\"true\\\"}}\"}," +
            "\"tableType\":\"\",\"rewriteEnabled\":0} sdHash: qQTgZAi5VzgpozzFGmIVTQ stats: column " +
            "col1: {\"colName\":\"col1\",\"colType\":\"int\"," +
            "\"statsData\":{\"longStats\":{\"lowValue\":-95,\"highValue\":95,\"numNulls\":1," +
            "\"numDVs\":2,\"bitVectors\":\"\"}}} column col2: {\"colName\":\"col2\",\"colType\":\"varchar(32)\"," +
            "\"statsData\":{\"stringStats\":{\"maxColLen\":97,\"avgColLen\":18.78," +
        "\"numNulls\":29,\"numDVs\":397,\"bitVectors\":\"\"}}}" + lsep +
        "{\"tableName\":\"tab1\",\"dbName\":\"db0\",\"owner\":\"me\",\"createTime\":0," +
        "\"lastAccessTime\":0,\"retention\":0,\"partitionKeys\":[{\"name\":\"pcol1\"," +
            "\"type\":\"string\",\"comment\":\"\"},{\"name\":\"pcol2\",\"type\":\"string\"," +
            "\"comment\":\"\"}],\"parameters\":{},\"tableType\":\"\",\"rewriteEnabled\":0} sdHash: " +
            "qQTgZAi5VzgpozzFGmIVTQ stats:" + lsep +
        "{\"tableName\":\"tab2\",\"dbName\":\"db0\",\"owner\":\"me\",\"createTime\":0," +
        "\"lastAccessTime\":0,\"retention\":0,\"partitionKeys\":[{\"name\":\"pcol1\"," +
        "\"type\":\"string\",\"comment\":\"\"},{\"name\":\"pcol2\",\"type\":\"string\"," +
        "\"comment\":\"\"}],\"parameters\":{},\"tableType\":\"\",\"rewriteEnabled\":0} sdHash: " +
        "qQTgZAi5VzgpozzFGmIVTQ stats:" + lsep, outStr.toString());

    List<List<String>> partVals = Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c", "d"));
    for (List<String> pv : partVals) {
      StorageDescriptor sd = new StorageDescriptor(Arrays.asList(new FieldSchema("col1", "int",
          ""), new FieldSchema("col2", "varchar(32)", "")),
          "/tmp", null, null, false, 0, null,  null, null, Collections.<String, String>emptyMap());
      Partition p = new Partition(pv, "db0", "tab1", 0, 0, sd, Collections.<String, String>emptyMap());
      store.addPartition(p);
    }
    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.PART_TABLE, "db0.tab1.a.b", null, conf, out, err);
    Assert.assertEquals("{\"values\":[\"a\",\"b\"],\"dbName\":\"db0\",\"tableName\":\"tab1\"," +
        "\"createTime\":0,\"lastAccessTime\":0,\"parameters\":{}} sdHash: " +
        "qQTgZAi5VzgpozzFGmIVTQ stats:" + lsep,  outStr.toString());

    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, "db0", "tab1");
    statsDesc.setPartName("pcol1=c/pcol2=d");
    ColumnStatisticsData csd1 = new ColumnStatisticsData();
    LongColumnStatsData lcsd = new LongColumnStatsData(1, 2);
    lcsd.setLowValue(-95);
    lcsd.setHighValue(95);
    csd1.setLongStats(lcsd);
    ColumnStatisticsData csd2 = new ColumnStatisticsData();
    csd2.setStringStats(new StringColumnStatsData(97, 18.78, 29, 397));
    List<ColumnStatisticsObj> csos = Arrays.asList(
        new ColumnStatisticsObj("col1", "int", csd1),
        new ColumnStatisticsObj("col2", "varchar(32)", csd2));
    ColumnStatistics statsObj = new ColumnStatistics(statsDesc, csos);
    store.updatePartitionColumnStatistics(statsObj, partVals.get(1));

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.PART_TABLE, "db0.tab1.c.d", null, conf, out, err);
    Assert.assertEquals("{\"values\":[\"c\",\"d\"],\"dbName\":\"db0\",\"tableName\":\"tab1\"," +
        "\"createTime\":0,\"lastAccessTime\":0,\"parameters\":{\"COLUMN_STATS_ACCURATE\":\"{\\\"COLUMN_STATS\\\":{\\\"col1\\\":\\\"true\\\",\\\"col2\\\":\\\"true\\\"}}\"}} sdHash: qQTgZAi5VzgpozzFGmIVTQ " +
        "stats: column col1: {\"colName\":\"col1\",\"colType\":\"int\"," +
        "\"statsData\":{\"longStats\":{\"lowValue\":-95,\"highValue\":95,\"numNulls\":1," +
        "\"numDVs\":2,\"bitVectors\":\"\"}}} column col2: {\"colName\":\"col2\",\"colType\":\"varchar(32)\"," +
        "\"statsData\":{\"stringStats\":{\"maxColLen\":97,\"avgColLen\":18.78,\"numNulls\":29," +
        "\"numDVs\":397,\"bitVectors\":\"\"}}}" + lsep,  outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.PART_TABLE, null, "db0.tab1", conf, out, err);
    Assert.assertEquals("{\"values\":[\"a\",\"b\"],\"dbName\":\"db0\",\"tableName\":\"tab1\"," +
        "\"createTime\":0,\"lastAccessTime\":0,\"parameters\":{}} sdHash: qQTgZAi5VzgpozzFGmIVTQ " +
        "stats:" + lsep +
        "{\"values\":[\"c\",\"d\"],\"dbName\":\"db0\",\"tableName\":\"tab1\",\"createTime\":0," +
        "\"lastAccessTime\":0,\"parameters\":{\"COLUMN_STATS_ACCURATE\":\"{\\\"COLUMN_STATS\\\":{\\\"col1\\\":\\\"true\\\",\\\"col2\\\":\\\"true\\\"}}\"}} sdHash: qQTgZAi5VzgpozzFGmIVTQ stats: column " +
        "col1: {\"colName\":\"col1\",\"colType\":\"int\"," +
        "\"statsData\":{\"longStats\":{\"lowValue\":-95,\"highValue\":95,\"numNulls\":1," +
        "\"numDVs\":2,\"bitVectors\":\"\"}}} column col2: {\"colName\":\"col2\",\"colType\":\"varchar(32)\"," +
        "\"statsData\":{\"stringStats\":{\"maxColLen\":97,\"avgColLen\":18.78,\"numNulls\":29," +
        "\"numDVs\":397,\"bitVectors\":\"\"}}}" + lsep, outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.PART_TABLE, null, "db0.tab1.a", conf, out, err);
    Assert.assertEquals("{\"values\":[\"a\",\"b\"],\"dbName\":\"db0\",\"tableName\":\"tab1\"," +
        "\"createTime\":0,\"lastAccessTime\":0,\"parameters\":{}} sdHash: qQTgZAi5VzgpozzFGmIVTQ " +
        "stats:" + lsep, outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.SD_TABLE, "qQTgZAi5VzgpozzFGmIVTQ", null, conf, out, err);
    Assert.assertEquals("{\"cols\":[{\"name\":\"col1\",\"type\":\"int\",\"comment\":\"\"}," +
        "{\"name\":\"col2\",\"type\":\"varchar(32)\",\"comment\":\"\"}],\"compressed\":0," +
        "\"numBuckets\":0,\"bucketCols\":[],\"sortCols\":[],\"storedAsSubDirectories\":0}" + lsep,
        outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.SD_TABLE, null, "whatever", conf, out, err);
    Assert.assertEquals("qQTgZAi5VzgpozzFGmIVTQ: {\"cols\":[{\"name\":\"col1\",\"type\":\"int\"," +
            "\"comment\":\"\"}," +
            "{\"name\":\"col2\",\"type\":\"varchar(32)\",\"comment\":\"\"}],\"compressed\":0," +
            "\"numBuckets\":0,\"bucketCols\":[],\"sortCols\":[],\"storedAsSubDirectories\":0}" + lsep,
        outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.SECURITY_TABLE, null, "whatever", conf, out, err);
    Assert.assertEquals("No security related entries" + lsep, outStr.toString());

    store.addMasterKey("this be a key");
    store.addToken("tokenid", "delegation token");
    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.SECURITY_TABLE, null, "whatever", conf, out, err);
    Assert.assertEquals("Master key 0: this be a key" + lsep +
        "Delegation token tokenid: delegation token" + lsep, outStr.toString());

    outStr = new ByteArrayOutputStream();
    out = new PrintStream(outStr);
    tool.go(false, HBaseReadWrite.SEQUENCES_TABLE, null, "whatever", conf, out, err);
    Assert.assertEquals("master_key: 1" + lsep, outStr.toString());
  }
}
