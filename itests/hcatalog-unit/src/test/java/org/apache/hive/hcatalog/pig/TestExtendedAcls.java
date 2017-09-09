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
package org.apache.hive.hcatalog.pig;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.HcatTestUtils;
import org.apache.hive.hcatalog.MiniCluster;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class TestExtendedAcls {
  private static final Logger LOG = LoggerFactory.getLogger(TestExtendedAcls.class);
  private static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir")
                                                + File.separator
                                                + TestExtendedAcls.class.getCanonicalName()
                                                + "-" + System.currentTimeMillis());
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final String TEXT_DATA_FILE = TEST_DATA_DIR + "/basic.input.data";
  private static final String DEFAULT_DATABASE_NAME = "test_acls";
  private static final String TABLE_NAME_PREFIX = "test_acls_";

  private static MiniCluster cluster = null;
  private static FileSystem clusterFS = null;
  private static Driver driver;
  private static Random random = new Random();
  private static Set<String> dbsCreated = Sets.newHashSet();

  private static ImmutableList<AclEntry> dirSpec = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, OTHER, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.ALL),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar", FsAction.ALL));

  private static ImmutableList<AclEntry> restrictedDirSpec = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.NONE),
      aclEntry(ACCESS, OTHER, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.NONE),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE));

  private static ImmutableList<AclEntry> dirSpec2 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "foo", FsAction.ALL),
      aclEntry(ACCESS, USER, "foo2", FsAction.ALL), // No DEFAULT, so child should not inherit.
      aclEntry(ACCESS, GROUP, "bar", FsAction.ALL),
      aclEntry(ACCESS, GROUP, "bar2", FsAction.ALL), // No DEFAULT, so child should not inherit.
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar", FsAction.ALL));

  private static ImmutableList<AclEntry> expectedDirSpec = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, OTHER, FsAction.NONE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.ALL),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar", FsAction.ALL));

  private static ImmutableList<AclEntry> expectedRestrictedDirSpec = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.NONE),
      aclEntry(ACCESS, OTHER, FsAction.NONE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.NONE),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE));

  private static ImmutableList<AclEntry> expectedDirSpec2 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.NONE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "foo", FsAction.ALL),
      aclEntry(ACCESS, GROUP, "bar", FsAction.ALL),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar", FsAction.ALL));

  public TestExtendedAcls() {
  }

  @BeforeClass
  public static void setupAllTests() throws Exception {
    setUpCluster();
    setUpLocalFileSystemDirectories();
    setUpClusterFileSystemDirectories();
    setUpHiveDriver();
    createTextData();
  }

  @Before
  public void setupSingleTest() throws Exception {
  }

  @AfterClass
  public static void tearDownAllTests() throws Exception {
    for (String db : dbsCreated) {
      dropDatabase(db);
    }
    tearDownCluster();
    cleanUpLocalFileSystemDirectories();
  }

  @Test
  public void testUnpartitionedTable() throws IOException, CommandNeedRetryException, FileNotFoundException {
    String dbName = DEFAULT_DATABASE_NAME;
    String tableName = TABLE_NAME_PREFIX + "1";

    Path warehouseDir = new Path(TEST_WAREHOUSE_DIR);
    Path dbDir = new Path(warehouseDir, dbName + ".db");
    Path tblDir = new Path(dbDir, tableName);
    FileSystem fs = cluster.getFileSystem();

    if (! dbsCreated.contains(dbName)) {
      createDatabase(dbName);
      dbsCreated.add(dbName);
    }

    // No extended ACLs on the database directory.
    fs.setPermission(dbDir, new FsPermission((short) 0750));

    createUnpartitionedTable(dbName, tableName);

    // Extended ACLs on the table directory.
    fs.setAcl(tblDir, dirSpec);

    FileStatus fstat = fs.getFileStatus(tblDir);
    List<AclEntry> acls = fs.getAclStatus(tblDir).getEntries();

    verify(tblDir, new FsPermission((short) 0775), fstat.getPermission(), dirSpec, acls, false);

    Properties props = new Properties();
    props.setProperty("dfs.namenode.acls.enabled", "true");

    PigServer server = getPigServer(props);
    server.setBatchOn();
    int i = 0;
    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FOREACH A GENERATE a, b, c;", ++i);
    server.registerQuery("STORE B INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.executeBatch();

    FileStatus[] fstats = fs.listStatus(tblDir);

    // All files in the table directory should inherit table ACLs.
    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }

    for (FileStatus fstat1 : fstats) {
      fs.delete(fstat1.getPath(), true);
    }

    fs.setAcl(tblDir, dirSpec2);

    fstat = fs.getFileStatus(tblDir);
    acls = fs.getAclStatus(tblDir).getEntries();

    verify(tblDir, new FsPermission((short) 0775), fstat.getPermission(), dirSpec2, acls, false);

    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FOREACH A GENERATE a, b, c;", ++i);
    server.registerQuery("STORE B INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.executeBatch();

    fstats = fs.listStatus(tblDir);

    // All files in the table directory should inherit table ACLs.
    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }
  }

  @Test
  public void testPartitionedTableStatic() throws IOException, CommandNeedRetryException, FileNotFoundException {
    String dbName = DEFAULT_DATABASE_NAME;
    String tableName = TABLE_NAME_PREFIX + "2";

    Path warehouseDir = new Path(TEST_WAREHOUSE_DIR);
    Path dbDir = new Path(warehouseDir, dbName + ".db");
    Path tblDir = new Path(dbDir, tableName);
    FileSystem fs = cluster.getFileSystem();

    if (! dbsCreated.contains(dbName)) {
      createDatabase(dbName);
      dbsCreated.add(dbName);
    }

    // No extended ACLs on the database directory.
    fs.setPermission(dbDir, new FsPermission((short) 0750));

    createPartitionedTable(dbName, tableName);

    // Extended ACLs on the table directory.
    fs.setAcl(tblDir, dirSpec);

    FileStatus fstat = fs.getFileStatus(tblDir);
    List<AclEntry> acls = fs.getAclStatus(tblDir).getEntries();

    verify(tblDir, new FsPermission((short) 0775), fstat.getPermission(), dirSpec, acls, false);

    Properties props = new Properties();
    props.setProperty("dfs.namenode.acls.enabled", "true");

    PigServer server = getPigServer(props);
    server.setBatchOn();
    int i = 0;
    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FOREACH A GENERATE a, b, c;", ++i);
    server.registerQuery("STORE B INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer('dt=1,ds=1');", ++i);
    server.executeBatch();

    // Partition directories (dt=1/ds=1) and all files in the table directory should inherit table ACLs.
    Path partDir = new Path(tblDir, "dt=1");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    partDir = new Path(partDir, "ds=1");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    FileStatus[] fstats = fs.listStatus(partDir);

    Assert.assertTrue("Expected to find files in " + partDir, fstats.length > 0);

    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }

    // Parent directory (dt=1) has restrictive ACLs compared to table.
    // Child directory (dt=1/ds=2) should inherit from table instead of parent.
    // NOTE: this behavior is different from hive and should be corrected to inherit from the parent instead.
    partDir = new Path(tblDir, "dt=1");
    fs.setAcl(partDir, restrictedDirSpec);

    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0775), fstat.getPermission(), restrictedDirSpec, acls, false);

    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FOREACH A GENERATE a, b, c;", ++i);
    server.registerQuery("STORE B INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer('dt=1,ds=2');", ++i);
    server.executeBatch();

    partDir = new Path(partDir, "ds=2");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    fstats = fs.listStatus(partDir);

    Assert.assertTrue("Expected to find files in " + partDir, fstats.length > 0);

    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }

    partDir = new Path(tblDir, "dt=1");
    fs.setAcl(partDir, dirSpec2);

    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0775), fstat.getPermission(), dirSpec2, acls, false);

    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FOREACH A GENERATE a, b, c;", ++i);
    server.registerQuery("STORE B INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer('dt=1,ds=3');", ++i);
    server.executeBatch();

    partDir = new Path(partDir, "ds=3");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    fstats = fs.listStatus(partDir);

    Assert.assertTrue("Expected to find files in " + partDir, fstats.length > 0);

    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }
  }

  @Test
  public void testPartitionedTableDynamic() throws IOException, CommandNeedRetryException, FileNotFoundException {
    String dbName = DEFAULT_DATABASE_NAME;
    String tableName = TABLE_NAME_PREFIX + "3";

    Path warehouseDir = new Path(TEST_WAREHOUSE_DIR);
    Path dbDir = new Path(warehouseDir, dbName + ".db");
    Path tblDir = new Path(dbDir, tableName);
    FileSystem fs = cluster.getFileSystem();

    if (! dbsCreated.contains(dbName)) {
      createDatabase(dbName);
      dbsCreated.add(dbName);
    }

    // No extended ACLs on the database directory.
    fs.setPermission(dbDir, new FsPermission((short) 0750));

    createPartitionedTable(dbName, tableName);

    // Extended ACLs on the table directory.
    fs.setAcl(tblDir, dirSpec);

    FileStatus fstat = fs.getFileStatus(tblDir);
    List<AclEntry> acls = fs.getAclStatus(tblDir).getEntries();

    verify(tblDir, new FsPermission((short) 0775), fstat.getPermission(), dirSpec, acls, false);

    Properties props = new Properties();
    props.setProperty("hive.exec.dynamic.partition", "true");
    props.setProperty("hive.exec.dynamic.partition.mode", "nonstrict");
    props.setProperty("dfs.namenode.acls.enabled", "true");

    PigServer server = getPigServer(props);
    server.setBatchOn();
    int i = 0;
    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FILTER A BY dt == '1' AND ds == '1';", ++i);
    server.registerQuery("C = FOREACH B GENERATE a, b, c, dt, ds;", ++i);
    server.registerQuery("STORE C INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.executeBatch();


    // Currently, partition directories created on the dynamic partitioned code path are incorrect.
    // Partition directories (dt=1/ds=1) and all files in the table directory should inherit table ACLs.
    Path partDir = new Path(tblDir, "dt=1");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    // TODO: Must check this separately because intermediate partition directories are not created properly.
    Assert.assertEquals("Expected permission to be 0750", new FsPermission((short) 0755), fstat.getPermission());
    verifyAcls(partDir, dirSpec, acls, false);
    //verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    // This directory is moved from the temporary location, so should match dirSpec.
    partDir = new Path(partDir, "ds=1");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    FileStatus[] fstats = fs.listStatus(partDir);

    Assert.assertTrue("Expected to find files in " + partDir, fstats.length > 0);

    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }

    // Parent directory (dt=1) has restrictive ACLs compared to table.
    // Child directory (dt=1/ds=2) should inherit from table instead of parent.
    // NOTE: this behavior is different from hive and should be corrected to inherit from the parent instead.
    partDir = new Path(tblDir, "dt=1");
    fs.setAcl(partDir, restrictedDirSpec);

    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0775), fstat.getPermission(), restrictedDirSpec, acls, false);

    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FILTER A BY dt == '1' AND ds == '2';", ++i);
    server.registerQuery("C = FOREACH B GENERATE a, b, c, dt, ds;", ++i);
    server.registerQuery("STORE C INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.executeBatch();

    partDir = new Path(partDir, "ds=2");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    fstats = fs.listStatus(partDir);

    Assert.assertTrue("Expected to find files in " + partDir, fstats.length > 0);

    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }

    partDir = new Path(tblDir, "dt=1");
    fs.setAcl(partDir, dirSpec2);

    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0775), fstat.getPermission(), dirSpec2, acls, false);

    server.registerQuery("A = LOAD '" + TEXT_DATA_FILE + "' AS (a:int, b:chararray, c:chararray, dt:chararray, ds:chararray);", ++i);
    server.registerQuery("B = FILTER A BY dt == '1' AND ds == '3';", ++i);
    server.registerQuery("C = FOREACH B GENERATE a, b, c, dt, ds;", ++i);
    server.registerQuery("STORE C INTO '" + dbName + "." + tableName + "' USING org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.executeBatch();

    partDir = new Path(partDir, "ds=3");
    fstat = fs.getFileStatus(partDir);
    acls = fs.getAclStatus(partDir).getEntries();

    verify(partDir, new FsPermission((short) 0770), fstat.getPermission(), expectedDirSpec, acls, false);

    fstats = fs.listStatus(partDir);

    Assert.assertTrue("Expected to find files in " + partDir, fstats.length > 0);

    for (FileStatus fstat1 : fstats) {
      acls = fs.getAclStatus(fstat1.getPath()).getEntries();
      verify(fstat1.getPath(), new FsPermission((short) 0770), fstat1.getPermission(), expectedDirSpec, acls, true);
    }
  }

  private static void setUpCluster() throws Exception {
    Configuration config = new Configuration();
    config.set("dfs.namenode.acls.enabled", "true");

    cluster = MiniCluster.buildCluster(config);
    clusterFS = cluster.getFileSystem();
  }

  private static void tearDownCluster() throws Exception {
    cluster.shutDown();
  }

  private static void setUpLocalFileSystemDirectories() {
    File f = new File(TEST_DATA_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if(!(new File(TEST_DATA_DIR).mkdirs())) {
      throw new RuntimeException("Could not create test-directory " + TEST_DATA_DIR + " on local filesystem.");
    }
  }

  private static void cleanUpLocalFileSystemDirectories() {
    File f = new File(TEST_DATA_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
  }

  private static void setUpClusterFileSystemDirectories() throws IOException {
    FileSystem clusterFS = cluster.getFileSystem();
    Path warehouseDir = new Path(TEST_WAREHOUSE_DIR);
    if (clusterFS.exists(warehouseDir)) {
      clusterFS.delete(warehouseDir, true);
    }
    clusterFS.mkdirs(warehouseDir);
  }

  private static void setUpHiveDriver() throws IOException {
    HiveConf hiveConf = createHiveConf();
    driver = new Driver(hiveConf);
    driver.setMaxRows(1000);
    SessionState.start(new CliSessionState(hiveConf));
  }

  private static HiveConf createHiveConf() {
    HiveConf hiveConf = new HiveConf(cluster.getConfiguration(), TestExtendedAcls.class);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    return hiveConf;
  }

  /**
   * Create data with schema:
   *    number \t string \t filler_string
   * @throws Exception
   */
  private static void createTextData() throws Exception {
    int LOOP_SIZE = 100;
    ArrayList<String> input = Lists.newArrayListWithExpectedSize((LOOP_SIZE+1) * LOOP_SIZE);
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      String sk = i % 2 == 0 ? "1" : "2";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        String sj = "S" + j + "S";
        String sm = Integer.toString((j % 3) + 1);
        input.add(si + "\t" + (i*j) + "\t" + sj + "\t" + sk + "\t" + sm);
      }
    }

    // Add nulls.
    for (int i=0; i<LOOP_SIZE; ++i) {
      String sk = i % 2 == 0 ? "1" : "2";
      String sm = Integer.toString(((i % 6) >> 1) + 1);
      input.add("\t" + "\t" + "S" + "_null_" + i + "_S" + "\t" + sk + "\t" + sm);
    }

    HcatTestUtils.createTestDataFile(TEXT_DATA_FILE, input.toArray(new String[input.size()]));
    FileSystem fs = cluster.getFileSystem();
    fs.copyFromLocalFile(new Path(TEXT_DATA_FILE), new Path(TEXT_DATA_FILE));
  }

  private void createDatabase(String dbName) throws IOException, CommandNeedRetryException {
    String cmd = "CREATE DATABASE IF NOT EXISTS " + dbName;
    executeStatementOnDriver(cmd);

    cmd = "DESC DATABASE " + dbName;
    executeStatementOnDriver(cmd);
  }

  private void createUnpartitionedTable(String dbName, String tableName) throws IOException, CommandNeedRetryException {
    createTable(dbName, tableName, "a INT, b STRING, c STRING", null);
  }

  private void createPartitionedTable(String dbName, String tableName) throws IOException, CommandNeedRetryException {
    createTable(dbName, tableName, "a INT, b STRING, c STRING", "dt STRING, ds STRING");
  }

  private void createTable(String dbName, String tableName, String schema, String partitionedBy)
      throws IOException, CommandNeedRetryException {
    String cmd = "CREATE TABLE " + dbName + "." + tableName + "(" + schema + ") ";
    if ((partitionedBy != null) && (!partitionedBy.trim().isEmpty())) {
      cmd = cmd + "PARTITIONED BY (" + partitionedBy + ") ";
    }
    executeStatementOnDriver(cmd);

    cmd = "DESC FORMATTED " + dbName + "." + tableName;
    executeStatementOnDriver(cmd);
  }

  /**
   * Execute Hive CLI statement
   * @param cmd arbitrary statement to execute
   */
  private void executeStatementOnDriver(String cmd) throws IOException, CommandNeedRetryException {
    LOG.debug("Executing: " + cmd);
    CommandProcessorResponse cpr = driver.run(cmd);
    if(cpr.getResponseCode() != 0) {
      throw new IOException("Failed to execute \"" + cmd + "\". " +
          "Driver returned " + cpr.getResponseCode() +
          " Error: " + cpr.getErrorMessage());
    }
  }

  private static void dropDatabase(String dbName) throws IOException, CommandNeedRetryException {
    driver.run("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
  }

  private PigServer getPigServer() throws IOException {
    return getPigServer(null);
  }

  private PigServer getPigServer(Properties props) throws IOException {
    if (props != null) {
      return new PigServer(ExecType.LOCAL, props);
    } else {
      return new PigServer(ExecType.LOCAL);
    }
  }

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @param permission FsAction set of permissions in the ACL entry
   * @return new AclEntry
   */
  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type)
        .setPermission(permission).build();
  }

  /**
   * Create a new AclEntry with scope, type, name and permission.
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @param name String optional ACL entry name
   * @param permission FsAction set of permissions in the ACL entry
   * @return new AclEntry
   */
  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      String name, FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type).setName(name)
        .setPermission(permission).build();
  }

  private void verify(Path path, FsPermission expectedPerm, FsPermission actualPerm,
      List<AclEntry> expectedAcls, List<AclEntry> actualAcls, boolean isFile) {
    FsAction maskPerm = null;

    LOG.debug("Verify permissions on " + path + " expected=" + expectedPerm + " " + expectedAcls + " actual=" + actualPerm + " " + actualAcls);

    for (AclEntry expected : expectedAcls) {
      if (expected.getType() == MASK && expected.getScope() == DEFAULT) {
        maskPerm = expected.getPermission();
        break;
      }
    }

    Assert.assertTrue("Permissions on " + path + " differ: expected=" + expectedPerm + " actual=" + actualPerm,
        expectedPerm.getUserAction() == actualPerm.getUserAction());
    Assert.assertTrue("Permissions on " + path + " differ: expected=" + expectedPerm + " actual=" + actualPerm,
        expectedPerm.getOtherAction() == actualPerm.getOtherAction());
    Assert.assertTrue("Mask permissions on " + path + " differ: expected=" + maskPerm + " actual=" + actualPerm,
        maskPerm == actualPerm.getGroupAction());
    verifyAcls(path, expectedAcls, actualAcls, isFile);
  }

  private void verifyAcls(Path path, List<AclEntry> expectedAcls, List<AclEntry> actualAcls, boolean isFile) {
    ArrayList<AclEntry> acls = new ArrayList<AclEntry>(actualAcls);

    for (AclEntry expected : expectedAcls) {
      if (! isFile && expected.getScope() == DEFAULT) {
        boolean found = false;
        for (AclEntry acl : acls) {
          if (acl.equals(expected)) {
            acls.remove(acl);
            found = true;
            break;
          }
        }

        Assert.assertTrue("ACLs on " + path + " differ: " + expected + " expected=" + expectedAcls + " actual=" + actualAcls, found);
      } else if (expected.getName() != null || expected.getType() == GROUP) {
        // Check file permissions for non-named ACLs, except for GROUP.
        if (isFile && expected.getScope() == DEFAULT) {
          // Files should not have DEFAULT ACLs.
          continue;
        }

        boolean found = false;
        for (AclEntry acl : acls) {
          if (acl.equals(expected)) {
            acls.remove(acl);
            found = true;
            break;
          }
        }

        Assert.assertTrue("ACLs on " + path + " differ: " + expected + " expected=" + expectedAcls + " actual=" + actualAcls, found);
      }
    }

    Assert.assertTrue("More ACLs on " + path + " than expected: actual=" + acls, acls.size() == 0);
  }
}
