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

package org.apache.hadoop.hive.ql.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.security.DummyHiveMetastoreAuthorizationProvider.AuthCallContext;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * TestAuthorizationPreEventListener. Test case for
 * {@link org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener} and
 * {@link org.apache.hadoop.hive.metastore.MetaStorePreEventListener}
 */
public class TestAuthorizationPreEventListener {
  private HiveConf clientHiveConf;
  private HiveMetaStoreClient msc;
  private IDriver driver;

  @Before
  public void setUp() throws Exception {

    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        AuthorizationPreEventListener.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname,
        DummyHiveMetastoreAuthorizationProvider.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname,
        HadoopDefaultMetastoreAuthenticator.class.getName());

    int port = MetaStoreTestUtils.startMetaStoreWithRetry();

    clientHiveConf = new HiveConf(this.getClass());

    clientHiveConf.setVar(HiveConf.ConfVars.METASTORE_URIS, "thrift://localhost:" + port);
    clientHiveConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    clientHiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    clientHiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");


    SessionState.start(new CliSessionState(clientHiveConf));
    msc = new HiveMetaStoreClient(clientHiveConf);
    driver = DriverFactory.newDriver(clientHiveConf);
  }

  @After
  public void tearDown() throws Exception {
  }

  private void validateCreateDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb.getName(), actualDb.getName());
    assertEquals(expectedDb.getLocationUri(), actualDb.getLocationUri());
  }

  private void validateTable(Table expectedTable, Table actualTable) {
    assertEquals(expectedTable.getTableName(), actualTable.getTableName());
    assertEquals(expectedTable.getDbName(), actualTable.getDbName());

    // We won't try to be too strict in checking this because we're comparing
    // table create intents with observed tables created.
    // If it does have a location though, we will compare, as with external tables
    if ((actualTable.getSd() != null) && (actualTable.getSd().getLocation() != null)){
      assertEquals(expectedTable.getSd().getLocation(), actualTable.getSd().getLocation());
    }
  }

  private void validateCreateTable(Table expectedTable, Table actualTable) {
    validateTable(expectedTable, actualTable);
  }

  private void validateAddPartition(Partition expectedPartition, Partition actualPartition) {
    validatePartition(expectedPartition, actualPartition);
  }

  private void validatePartition(Partition expectedPartition, Partition actualPartition) {
    assertEquals(expectedPartition.getValues(),
        actualPartition.getValues());
    assertEquals(expectedPartition.getDbName(),
        actualPartition.getDbName());
    assertEquals(expectedPartition.getTableName(),
        actualPartition.getTableName());

    // assertEquals(expectedPartition.getSd().getLocation(),
    //     actualPartition.getSd().getLocation());
    // we don't compare locations, because the location can still be empty in
    // the pre-event listener before it is created.

    assertEquals(expectedPartition.getSd().getInputFormat(),
        actualPartition.getSd().getInputFormat());
    assertEquals(expectedPartition.getSd().getOutputFormat(),
        actualPartition.getSd().getOutputFormat());
    assertEquals(expectedPartition.getSd().getSerdeInfo(),
        actualPartition.getSd().getSerdeInfo());

  }

  private void validateAlterPartition(Partition expectedOldPartition,
      Partition expectedNewPartition, String actualOldPartitionDbName,
      String actualOldPartitionTblName,List<String> actualOldPartitionValues,
      Partition actualNewPartition) {
    assertEquals(expectedOldPartition.getValues(), actualOldPartitionValues);
    assertEquals(expectedOldPartition.getDbName(), actualOldPartitionDbName);
    assertEquals(expectedOldPartition.getTableName(), actualOldPartitionTblName);

    validatePartition(expectedNewPartition, actualNewPartition);
  }

  private void validateAlterTable(Table expectedOldTable, Table expectedNewTable,
      Table actualOldTable, Table actualNewTable) {
    validateTable(expectedOldTable, actualOldTable);
    validateTable(expectedNewTable, actualNewTable);
  }

  private void validateDropPartition(Partition expectedPartition, Partition actualPartition) {
    validatePartition(expectedPartition, actualPartition);
  }

  private void validateDropTable(Table expectedTable, Table actualTable) {
    validateTable(expectedTable, actualTable);
  }

  private void validateDropDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb, actualDb);
  }

  private void validateAlterDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb, actualDb);
  }

  @Test
  public void testListener() throws Exception {
    String dbName = "hive3705";
    String tblName = "tmptbl";
    String renamed = "tmptbl2";
    int listSize = 0;

    List<AuthCallContext> authCalls = DummyHiveMetastoreAuthorizationProvider.authCalls;
    assertEquals(authCalls.size(),listSize);

    driver.run("create database " + dbName);
    listSize++;
    Database dbFromEvent = (Database)assertAndExtractSingleObjectFromEvent(listSize, authCalls,
        DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.DB);
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db,dbFromEvent);

    driver.run("use " + dbName);
    driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));
    listSize = authCalls.size();

    Table tblFromEvent = (
        (org.apache.hadoop.hive.ql.metadata.Table)
        assertAndExtractSingleObjectFromEvent(listSize, authCalls,
            DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.TABLE))
            .getTTable();
    Table tbl = msc.getTable(dbName, tblName);
    Assert.assertTrue(tbl.isSetId());
    tbl.unsetId();
    validateCreateTable(tbl, tblFromEvent);

    driver.run("alter table tmptbl add partition (b='2011')");
    listSize = authCalls.size();

    Partition ptnFromEvent = (
        (org.apache.hadoop.hive.ql.metadata.Partition)
        assertAndExtractSingleObjectFromEvent(listSize, authCalls,
            DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.PARTITION))
            .getTPartition();
    Partition part = msc.getPartition("hive3705", "tmptbl", "b=2011");
    validateAddPartition(part,ptnFromEvent);

    driver.run(String.format("alter table %s touch partition (%s)", tblName, "b='2011'"));
    listSize = authCalls.size();

    Partition ptnFromEventAfterAlter = (
        (org.apache.hadoop.hive.ql.metadata.Partition)
        assertAndExtractSingleObjectFromEvent(listSize, authCalls,
            DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.PARTITION))
            .getTPartition();

    //the partition did not change,
    // so the new partition should be similar to the original partition
    Partition modifiedP = msc.getPartition(dbName, tblName, "b=2011");
    validateAlterPartition(part, modifiedP, ptnFromEventAfterAlter.getDbName(),
        ptnFromEventAfterAlter.getTableName(), ptnFromEventAfterAlter.getValues(),
        ptnFromEventAfterAlter);


    List<String> part_vals = new ArrayList<String>();
    part_vals.add("c=2012");

    listSize = authCalls.size();
    Partition newPart = msc.appendPartition(dbName, tblName, part_vals);
    listSize++;

    Partition newPtnFromEvent = (
        (org.apache.hadoop.hive.ql.metadata.Partition)
        assertAndExtractSingleObjectFromEvent(listSize, authCalls,
            DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.PARTITION))
            .getTPartition();
    validateAddPartition(newPart,newPtnFromEvent);


    driver.run(String.format("alter table %s rename to %s", tblName, renamed));
    listSize = authCalls.size();

    Table renamedTableFromEvent = (
        (org.apache.hadoop.hive.ql.metadata.Table)
        assertAndExtractSingleObjectFromEvent(listSize, authCalls,
            DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.TABLE))
            .getTTable();

    Table renamedTable = msc.getTable(dbName, renamed);
    validateAlterTable(tbl, renamedTable, renamedTableFromEvent,
        renamedTable);
    assertFalse(tbl.getTableName().equals(renamedTable.getTableName()));

    //change the table name back
    driver.run(String.format("alter table %s rename to %s", renamed, tblName));
    driver.run(String.format("alter table %s drop partition (b='2011')", tblName));
    listSize = authCalls.size();

    Partition ptnFromDropPartition = (
        (org.apache.hadoop.hive.ql.metadata.Partition)
        assertAndExtractSingleObjectFromEvent(listSize, authCalls,
            DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.PARTITION))
            .getTPartition();

    validateDropPartition(modifiedP, ptnFromDropPartition);

    driver.run("drop table " + tblName);
    listSize = authCalls.size();
    Table tableFromDropTableEvent = (
        (org.apache.hadoop.hive.ql.metadata.Table)
        assertAndExtractSingleObjectFromEvent(listSize, authCalls,
            DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.TABLE))
            .getTTable();


    validateDropTable(tbl, tableFromDropTableEvent);

    // verify that we can create a table with IF/OF to some custom non-existent format
    Table tCustom = tbl.deepCopy();
    tCustom.getSd().setInputFormat("org.apache.hive.dummy.DoesNotExistInputFormat");
    tCustom.getSd().setOutputFormat("org.apache.hive.dummy.DoesNotExistOutputFormat");
    if (tCustom.getSd().getSerdeInfo() == null){
      tCustom.getSd().setSerdeInfo(
          new SerDeInfo(
              "dummy"
              ,"org.apache.hive.dummy.DoesNotExistSerDe"
              , new HashMap<String,String>()
          )
      );
    } else {
      tCustom.getSd().getSerdeInfo().setSerializationLib(
          "org.apache.hive.dummy.DoesNotExistSerDe");
    }

    tCustom.setTableName(tbl.getTableName() + "_custom");
    listSize = authCalls.size();
    msc.createTable(tCustom);
    listSize++;

    Table customCreatedTableFromEvent = (
        (org.apache.hadoop.hive.ql.metadata.Table)
            assertAndExtractSingleObjectFromEvent(listSize, authCalls,
                DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.TABLE))
        .getTTable();
    Table customCreatedTable = msc.getTable(tCustom.getDbName(), tCustom.getTableName());
    validateCreateTable(tCustom,customCreatedTable);
    validateCreateTable(tCustom,customCreatedTableFromEvent);

    assertEquals(tCustom.getSd().getInputFormat(),
        customCreatedTable.getSd().getInputFormat());
    assertEquals(tCustom.getSd().getOutputFormat(),
        customCreatedTable.getSd().getOutputFormat());
    assertEquals(tCustom.getSd().getSerdeInfo().getSerializationLib(),
        customCreatedTable.getSd().getSerdeInfo().getSerializationLib());
    assertEquals(tCustom.getSd().getInputFormat(),
        customCreatedTableFromEvent.getSd().getInputFormat());
    assertEquals(tCustom.getSd().getOutputFormat(),
        customCreatedTableFromEvent.getSd().getOutputFormat());
    assertEquals(tCustom.getSd().getSerdeInfo().getSerializationLib(),
        customCreatedTableFromEvent.getSd().getSerdeInfo().getSerializationLib());

    listSize = authCalls.size();
    msc.dropTable(tCustom.getDbName(), tCustom.getTableName());
    listSize += 2;

    Table table2FromDropTableEvent = (
        (org.apache.hadoop.hive.ql.metadata.Table)
            assertAndExtractSingleObjectFromEvent(listSize, authCalls,
                DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.TABLE))
        .getTTable();

    validateDropTable(tCustom, table2FromDropTableEvent);

    // Test ALTER DATABASE SET LOCATION.
    String oldDatabaseLocation = db.getLocationUri();
    String newDatabaseLocation = oldDatabaseLocation.replace(db.getName(), "new." + db.getName());
    driver.run("ALTER DATABASE " + dbName + " SET LOCATION \"" + newDatabaseLocation + "\"");
    listSize = authCalls.size();
    Database dbFromAlterDatabaseEvent =
        (Database)assertAndExtractSingleObjectFromEvent(listSize, authCalls,
        DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.DB);
    validateAlterDb(db, dbFromAlterDatabaseEvent);
    // Reset database location.
    driver.run("ALTER DATABASE " + dbName + " SET LOCATION \"" + oldDatabaseLocation + "\"");

    // Test DROP DATABASE.
    driver.run("drop database " + dbName);
    listSize = authCalls.size();
    Database dbFromDropDatabaseEvent =
        (Database)assertAndExtractSingleObjectFromEvent(listSize, authCalls,
        DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.DB);

    validateDropDb(db, dbFromDropDatabaseEvent);
  }

  public Object assertAndExtractSingleObjectFromEvent(int listSize,
      List<AuthCallContext> authCalls,
      DummyHiveMetastoreAuthorizationProvider.AuthCallContextType callType) {
    assertEquals(listSize, authCalls.size());
    assertEquals(1,authCalls.get(listSize-1).authObjects.size());

    assertEquals(callType,authCalls.get(listSize-1).type);
    return (authCalls.get(listSize-1).authObjects.get(0));
  }

}
