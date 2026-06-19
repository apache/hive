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
package org.apache.hadoop.hive.ql.util;

import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.ACIDTBL;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.ACIDTBLPART;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.NONACIDNONBUCKET;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.NONACIDORCTBL;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.NONACIDORCTBL2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

public class TestHiveStrictManagedMigration extends TxnCommandsBaseForTests {
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestHiveStrictManagedMigration.class.getCanonicalName() + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  private static final String EXTERNAL_TABLE_LOCATION = new File(TEST_DATA_DIR, "tmp").getPath();

  @Test
  public void testUpgrade() throws Exception {
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    runStatementOnDriver("DROP TABLE IF EXISTS test.TAcid");
    runStatementOnDriver("DROP DATABASE IF EXISTS test");

    runStatementOnDriver("CREATE DATABASE test");
    runStatementOnDriver(
      "CREATE TABLE test.TAcid (a int, b int) CLUSTERED BY (b) INTO 2 BUCKETS STORED AS orc TBLPROPERTIES" +
        " ('transactional'='true')");
    runStatementOnDriver("INSERT INTO test.TAcid" + makeValuesClause(data));

    runStatementOnDriver(
      "CREATE EXTERNAL TABLE texternal (a int, b int)");

    // Case for table having null location
    runStatementOnDriver("CREATE EXTERNAL TABLE test.sysdbtest(tbl_id bigint)");
    org.apache.hadoop.hive.ql.metadata.Table table = Hive.get(hiveConf).getTable("test", "sysdbtest");
    table.getSd().unsetLocation();
    Hive.get(hiveConf).alterTable(table, false,
      new EnvironmentContext(ImmutableMap.of(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE)), false);

    String oldWarehouse = getWarehouseDir();
    String[] args = {"--hiveconf", "hive.strict.managed.tables=true", "-m",  "automatic", "--modifyManagedTables",
      "--oldWarehouseRoot", oldWarehouse};
    HiveConf newConf = new HiveConf(hiveConf);
    File newWarehouseDir = new File(getTestDataDir(), "newWarehouse");
    newConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, newWarehouseDir.getAbsolutePath());
    newConf.set("strict.managed.tables.migration.owner", System.getProperty("user.name"));
    runMigrationTool(newConf, args);

    Assert.assertTrue(newWarehouseDir.exists());
    Assert.assertTrue(new File(newWarehouseDir, ACIDTBL.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, ACIDTBLPART.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, NONACIDNONBUCKET.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, NONACIDORCTBL.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, NONACIDORCTBL2.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(new File(newWarehouseDir, "test.db"), "tacid").exists());
    Assert.assertTrue(new File(oldWarehouse, "texternal").exists());

    // Tear down
    runStatementOnDriver("drop database test cascade");
    Database defaultDb = Hive.get().getDatabase("default");
    defaultDb.setLocationUri(oldWarehouse);
    Hive.get().alterDatabase("default", defaultDb);
    System.setProperty("hive.strict.managed.tables", "false");
  }

  /**
   * Tests shouldMoveExternal option on all possible scenarios of the following dimensions:
   * - managed or external table type?
   * - location in (old) warehouse or truly external location?
   * - is partitioned?
   * - is partition location default (under table directory) or custom external path?
   * - default or custom database?
   * @throws Exception
   */
/*
  @Test
  public void testExternalMove() throws Exception {
    setupExternalTableTest();
    String oldWarehouse = getWarehouseDir();
    String[] args = {"-m",  "external", "--shouldMoveExternal", "--tableRegex", "man.*|ext.*|custm.*|custe.*",
      "--oldWarehouseRoot", oldWarehouse};
    HiveConf newConf = new HiveConf(hiveConf);
    File newManagedWarehouseDir = new File(getTestDataDir(), "newManaged");
    File newExtWarehouseDir = new File(getTestDataDir(), "newExternal");
    newConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, newManagedWarehouseDir.getAbsolutePath());
    newConf.set(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname, newExtWarehouseDir.getAbsolutePath());
    runMigrationTool(newConf, args);
    Assert.assertTrue(newExtWarehouseDir.exists());
    assertExternalTableLocations(newExtWarehouseDir, new File(EXTERNAL_TABLE_LOCATION));
    assertSDLocationCorrect();
  }
*/

  @Test(expected = IllegalArgumentException.class)
  public void testExternalMoveFailsForIncorrectOptions() throws Throwable {
    try {
      String[] args = {"-m", "automatic", "--shouldMoveExternal"};
      runMigrationTool(new HiveConf(hiveConf), args);
    } catch (Exception e) {
      // Exceptions are re-packaged by the migration tool...
      throw e.getCause();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceptionForDbRegexPlusControlFile() throws Throwable {
    try {
      String[] args = {"-m", "automatic", "--dbRegex", "db0", "--controlFileUrl", "file:/tmp/file"};
      runMigrationTool(new HiveConf(hiveConf), args);
    } catch (Exception e) {
      // Exceptions are re-packaged by the migration tool...
      throw e.getCause();
    }
  }

  @Test
  public void testUsingControlFileUrl() throws Throwable {
    setupExternalTableTest();
    String oldWarehouse = getWarehouseDir();
    String[] args = {"-m",  "external", "--oldWarehouseRoot", oldWarehouse, "--controlFileUrl",
        "src/test/resources/hsmm/hsmm_cfg_01.yaml"};
    HiveConf newConf = new HiveConf(hiveConf);

    runMigrationTool(newConf, args);

    verifySubsetOfTablesBecameExternal(
        Sets.newHashSet("manwhwh", "manwhnone")
    );
  }

  @Test
  public void testUsingControlDirUrl() throws Throwable {
    setupExternalTableTest();
    String oldWarehouse = getWarehouseDir();
    String[] args = {"-m",  "external", "--oldWarehouseRoot", oldWarehouse, "--controlFileUrl",
        "src/test/resources/hsmm"};
    HiveConf newConf = new HiveConf(hiveConf);

    runMigrationTool(newConf, args);

    verifySubsetOfTablesBecameExternal(
        Sets.newHashSet("manwhwh", "manwhnone", "custdb.custmanwhwh")
    );
  }

  private void verifySubsetOfTablesBecameExternal(Set<String> expectedExternals) throws Throwable {
    Set<String> allTables = Sets.newHashSet("manwhnone", "manoutnone", "manwhwh", "manwhout", "manwhmixed",
        "manoutout", "custdb.custmanwhwh");

    for (String expectedExternalTableName : expectedExternals) {
      assertEquals(TableType.EXTERNAL_TABLE, Hive.get().getTable(expectedExternalTableName).getTableType());
    }

    for (String expectedLeftAsManagedTableName : Sets.difference(allTables, expectedExternals)) {
      assertEquals(TableType.MANAGED_TABLE, Hive.get().getTable(expectedLeftAsManagedTableName).getTableType());
    }

  }

  /**
   * Should encounter a DB with an unset owner, and should try to chown the new dir path to 'hive' user.
   * This will always fail in this test, as we're never running it as root.
   * @throws Exception
   */
  @Test(expected = AssertionError.class)
  public void testExtDbDirOnFsIsCreatedAsHiveIfDbOwnerNull() throws Exception {
    runStatementOnDriver("drop database if exists ownerlessdb");
    runStatementOnDriver("create database ownerlessdb");
    Database db = Hive.get().getDatabase("ownerlessdb");
    db.setOwnerName(null);
    Hive.get().alterDatabase("ownerlessdb", db);

    String[] args = {"-m",  "external"};
    HiveConf newConf = new HiveConf(hiveConf);
    File newExtWarehouseDir = new File(getTestDataDir(), "newExternal");
    newConf.set(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname, newExtWarehouseDir.getAbsolutePath());
    runMigrationTool(newConf, args);
  }

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }


  private static void runMigrationTool(HiveConf hiveConf, String[] args) throws Exception {
    HiveStrictManagedMigration.hiveConf = hiveConf;
    HiveStrictManagedMigration.scheme = "file";
    HiveStrictManagedMigration.main(args);
    if (HiveStrictManagedMigration.RC != 0) {
      fail("HiveStrictManagedMigration failed with error(s)");
    }
  }

  private void setupExternalTableTest() throws Exception {
    runStatementOnDriver("drop table if exists manwhnone");
    runStatementOnDriver("drop table if exists manoutnone");
    runStatementOnDriver("drop table if exists manwhwh");
    runStatementOnDriver("drop table if exists manwhout");
    runStatementOnDriver("drop table if exists manwhmixed");
    runStatementOnDriver("drop table if exists manoutout");
    runStatementOnDriver("drop table if exists extwhnone");
    runStatementOnDriver("drop table if exists extoutnone");
    runStatementOnDriver("drop table if exists extwhwh");
    runStatementOnDriver("drop table if exists extwhout");
    runStatementOnDriver("drop table if exists extwhmixed");
    runStatementOnDriver("drop table if exists extoutout");
    runStatementOnDriver("drop table if exists custdb.custmanwhwh");
    runStatementOnDriver("drop table if exists custdb.custextwhwh");
    runStatementOnDriver("create table manwhnone (a string)");
    runStatementOnDriver("create table manoutnone (a string) location '" + EXTERNAL_TABLE_LOCATION
      + "/manoutnone'");
    runStatementOnDriver("create table manwhwh (a string) partitioned by (p string)");
    runStatementOnDriver("alter table manwhwh add partition (p='p1')");
    runStatementOnDriver("alter table manwhwh add partition (p='p2')");
    runStatementOnDriver("create table manwhout (a string) partitioned by (p string)");
    runStatementOnDriver("alter table manwhout add partition (p='p1') location '" + EXTERNAL_TABLE_LOCATION
      + "/manwhoutp1'");
    runStatementOnDriver("alter table manwhout add partition (p='p2') location '" + EXTERNAL_TABLE_LOCATION
      + "/manwhoutp2'");
    runStatementOnDriver("create table manwhmixed (a string) partitioned by (p string)");
    runStatementOnDriver("alter table manwhmixed add partition (p='p1') location '" + EXTERNAL_TABLE_LOCATION
      + "/manwhmixedp1'");
    runStatementOnDriver("alter table manwhmixed add partition (p='p2')");
    runStatementOnDriver("create table manoutout (a string) partitioned by (p string) location '" +
      EXTERNAL_TABLE_LOCATION + "/manoutout'");
    runStatementOnDriver("alter table manoutout add partition (p='p1')");
    runStatementOnDriver("alter table manoutout add partition (p='p2')");
    runStatementOnDriver("create external table extwhnone (a string)");
    runStatementOnDriver("create external table extoutnone (a string) location '" + EXTERNAL_TABLE_LOCATION
      + "/extoutnone'");
    runStatementOnDriver("create external table extwhwh (a string) partitioned by (p string)");
    runStatementOnDriver("alter table extwhwh add partition (p='p1')");
    runStatementOnDriver("alter table extwhwh add partition (p='p2')");
    runStatementOnDriver("create external table extwhout (a string) partitioned by (p string)");
    runStatementOnDriver("alter table extwhout add partition (p='p1') location '" + EXTERNAL_TABLE_LOCATION
      + "/extwhoutp1'");
    runStatementOnDriver("alter table extwhout add partition (p='p2') location '" + EXTERNAL_TABLE_LOCATION
      + "/extwhoutp2'");
    runStatementOnDriver("create external table extwhmixed (a string) partitioned by (p string)");
    runStatementOnDriver("alter table extwhmixed add partition (p='p1') location '" + EXTERNAL_TABLE_LOCATION
      + "/extwhmixedp1'");
    runStatementOnDriver("alter table extwhmixed add partition (p='p2')");
    runStatementOnDriver("create external table extoutout (a string) partitioned by (p string) location '"
      + EXTERNAL_TABLE_LOCATION + "/extoutout'");
    runStatementOnDriver("alter table extoutout add partition (p='p1')");
    runStatementOnDriver("alter table extoutout add partition (p='p2')");
    runStatementOnDriver("drop database if exists custdb");
    runStatementOnDriver("create database custdb");
    runStatementOnDriver("create table custdb.custmanwhwh (a string) partitioned by (p string)");
    runStatementOnDriver("alter table custdb.custmanwhwh add partition (p='p1')");
    runStatementOnDriver("alter table custdb.custmanwhwh add partition (p='p2')");
    runStatementOnDriver("create external table custdb.custextwhwh (a string) partitioned by (p string)");
    runStatementOnDriver("alter table custdb.custextwhwh add partition (p='p1')");
    runStatementOnDriver("alter table custdb.custextwhwh add partition (p='p2')");
  }

  private static void assertExternalTableLocations(File exteralWarehouseDir, File externalNonWhDir)
    throws IOException {
    Set<String> actualDirs = Files.find(Paths.get(exteralWarehouseDir.toURI()), Integer.MAX_VALUE, (p, a)->true)
      .map(p->p.toString().replaceAll(exteralWarehouseDir.getAbsolutePath(), ""))
      .filter(s->!s.isEmpty()).collect(toSet());
    Set<String> expectedDirs = new HashSet<>();
    expectedDirs.add("/extwhwh");
    expectedDirs.add("/extwhwh/p=p2");
    expectedDirs.add("/extwhwh/p=p1");
    expectedDirs.add("/extwhmixed");
    expectedDirs.add("/extwhmixed/p=p2");
    expectedDirs.add("/manwhwh");
    expectedDirs.add("/manwhwh/p=p2");
    expectedDirs.add("/manwhwh/p=p1");
    expectedDirs.add("/custdb.db");
    expectedDirs.add("/custdb.db/custmanwhwh");
    expectedDirs.add("/custdb.db/custmanwhwh/p=p2");
    expectedDirs.add("/custdb.db/custmanwhwh/p=p1");
    expectedDirs.add("/custdb.db/custextwhwh");
    expectedDirs.add("/custdb.db/custextwhwh/p=p2");
    expectedDirs.add("/custdb.db/custextwhwh/p=p1");
    expectedDirs.add("/manwhout");
    expectedDirs.add("/manwhnone");
    expectedDirs.add("/manwhmixed");
    expectedDirs.add("/manwhmixed/p=p2");
    expectedDirs.add("/extwhnone");
    expectedDirs.add("/extwhout");
    assertEquals("Unexpected external warehouse directory structure in " + exteralWarehouseDir, expectedDirs,
      actualDirs);

    actualDirs = Files.find(Paths.get(externalNonWhDir.toURI()), Integer.MAX_VALUE, (p, a)->true)
      .map(p->p.toString().replaceAll(externalNonWhDir.getAbsolutePath(), ""))
      .filter(s->!s.isEmpty()).collect(toSet());
    expectedDirs.clear();
    expectedDirs.add("/manoutout");
    expectedDirs.add("/extoutout/p=p2");
    expectedDirs.add("/extoutout/p=p1");
    expectedDirs.add("/extwhoutp2");
    expectedDirs.add("/extwhoutp1");
    expectedDirs.add("/manwhmixedp1");
    expectedDirs.add("/manwhoutp1");
    expectedDirs.add("/manoutout/p=p1");
    expectedDirs.add("/manoutout/p=p2");
    expectedDirs.add("/manwhoutp2");
    expectedDirs.add("/extoutnone");
    expectedDirs.add("/manoutnone");
    expectedDirs.add("/extoutout");
    expectedDirs.add("/extwhmixedp1");
    assertEquals("Unexpected external (non-warehouse) directory structure in " + externalNonWhDir, expectedDirs,
      actualDirs);
  }

  private static void assertSDLocationCorrect() throws HiveException {
    org.apache.hadoop.hive.ql.metadata.Table table = Hive.get().getTable("manwhwh");
    List<Partition> partitions = Hive.get().getPartitions(table);
    assertTrue(partitions.get(0).getLocation().contains("/newExternal/manwhwh/p=p1"));
    assertTrue(partitions.get(1).getLocation().contains("/newExternal/manwhwh/p=p2"));

    table = Hive.get().getTable("manwhout");
    partitions = Hive.get().getPartitions(table);
    assertTrue(partitions.get(0).getLocation().contains("/tmp/manwhoutp1"));
    assertTrue(partitions.get(1).getLocation().contains("/tmp/manwhoutp2"));

    table = Hive.get().getTable("manwhmixed");
    partitions = Hive.get().getPartitions(table);
    assertTrue(partitions.get(0).getLocation().contains("/tmp/manwhmixedp1"));
    assertTrue(partitions.get(1).getLocation().contains("/newExternal/manwhmixed/p=p2"));
  }
}
