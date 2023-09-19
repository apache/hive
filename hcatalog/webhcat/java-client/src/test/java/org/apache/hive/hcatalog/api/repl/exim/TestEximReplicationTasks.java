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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.api.repl.exim;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.api.TestHCatClient;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.CommandTestUtils;
import org.apache.hive.hcatalog.api.repl.NoopReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider;
import org.apache.hive.hcatalog.api.repl.commands.DropDatabaseCommand;
import org.apache.hive.hcatalog.api.repl.commands.DropPartitionCommand;
import org.apache.hive.hcatalog.api.repl.commands.DropTableCommand;
import org.apache.hive.hcatalog.api.repl.commands.ExportCommand;
import org.apache.hive.hcatalog.api.repl.commands.ImportCommand;
import org.apache.hive.hcatalog.api.repl.commands.NoopCommand;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.messaging.MessageFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestEximReplicationTasks{

  private static MessageFactory msgFactory = MessageFactory.getInstance();
  private static StagingDirectoryProvider stagingDirectoryProvider =
      new StagingDirectoryProvider.TrivialImpl("/tmp","/");
  private static HCatClient client;

  @BeforeClass
  public static void setUpBeforeClass() throws HCatException {

    client = HCatClient.create(new HiveConf());

    ReplicationTask.resetFactory(EximReplicationTaskFactory.class);
  }

  // Dummy mapping used for all db and table name mappings
  static Function<String,String> debugMapping = new Function<String,String>(){
    @Nullable
    @Override
    public String apply(@Nullable String s) {
      if (s == null){
        return null;
      } else {
        StringBuilder sb = new StringBuilder(s);
        return sb.toString() + sb.reverse().toString();
      }
    }
  };

  @Test
  public void testDebugMapper(){
    assertEquals("BlahhalB",debugMapping.apply("Blah"));
    assertEquals(null, debugMapping.apply(null));
    assertEquals("", debugMapping.apply(""));
  }

  @Test
  public void testCreateDb(){
    Database db = new Database();
    db.setName("testdb");
    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_CREATE_DATABASE_EVENT, msgFactory.buildCreateDatabaseMessage(db).toString());
    event.setDbName(db.getName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyCreateDbReplicationTask(rtask); // CREATE DB currently replicated as Noop.
  }

  private static void verifyCreateDbReplicationTask(ReplicationTask rtask) {
    assertEquals(CreateDatabaseReplicationTask.class, rtask.getClass());
    assertTrue("CreateDatabaseReplicationTask should be a noop", rtask instanceof NoopReplicationTask);
    assertEquals(false, rtask.needsStagingDirs());
    assertEquals(true, rtask.isActionable());
    for (Command c : rtask.getSrcWhCommands()){
      assertEquals(NoopCommand.class,c.getClass());
    }
    for (Command c : rtask.getDstWhCommands()){
      assertEquals(NoopCommand.class,c.getClass());
    }
  }

  @Test
  public void testDropDb() throws IOException {
    Database db = new Database();
    db.setName("testdb");
    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_DROP_DATABASE_EVENT, msgFactory.buildCreateDatabaseMessage(db).toString());
    event.setDbName(db.getName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyDropDbReplicationTask(rtask);

  }

  private static void verifyDropDbReplicationTask(ReplicationTask rtask) throws IOException {
    assertEquals(DropDatabaseReplicationTask.class, rtask.getClass());
    assertEquals(false, rtask.needsStagingDirs());
    assertEquals(true, rtask.isActionable());

    rtask
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    for (Command c : rtask.getSrcWhCommands()){
      assertEquals(NoopCommand.class, c.getClass());
    }

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(1,dstCommands.size());
    assertEquals(DropDatabaseCommand.class, dstCommands.get(0).getClass());

    DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand(
        debugMapping.apply(rtask.getEvent().getDbName()),
        rtask.getEvent().getEventId());

    assertEquals(ReplicationUtils.serializeCommand(dropDatabaseCommand),
        ReplicationUtils.serializeCommand(dstCommands.get(0)));
  }

  @Test
  public void testCreateTable() throws IOException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_CREATE_TABLE_EVENT, msgFactory.buildCreateTableMessage(t).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyCreateTableReplicationTask(rtask);
  }

  private static void verifyCreateTableReplicationTask(ReplicationTask rtask) throws IOException {
    assertEquals(CreateTableReplicationTask.class, rtask.getClass());
    assertEquals(true, rtask.needsStagingDirs());
    assertEquals(false, rtask.isActionable());

    rtask
        .withSrcStagingDirProvider(stagingDirectoryProvider)
        .withDstStagingDirProvider(stagingDirectoryProvider)
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    assertEquals(true, rtask.isActionable());

    List<? extends Command> srcCommands = Lists.newArrayList(rtask.getSrcWhCommands());
    assertEquals(1,srcCommands.size());
    assertEquals(ExportCommand.class, srcCommands.get(0).getClass());

    ExportCommand exportCommand = getExpectedExportCommand(rtask, null,false);

    assertEquals(ReplicationUtils.serializeCommand(exportCommand),
        ReplicationUtils.serializeCommand(srcCommands.get(0)));

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(1,dstCommands.size());
    assertEquals(ImportCommand.class, dstCommands.get(0).getClass());

    ImportCommand importCommand = getExpectedImportCommand(rtask,null,false);

    assertEquals(ReplicationUtils.serializeCommand(importCommand),
        ReplicationUtils.serializeCommand(dstCommands.get(0)));
  }

  @Test
  public void testDropTable() throws IOException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_DROP_TABLE_EVENT, msgFactory.buildDropTableMessage(t).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyDropTableReplicationTask(rtask);
  }

  private static void verifyDropTableReplicationTask(ReplicationTask rtask) throws IOException {
    assertEquals(DropTableReplicationTask.class, rtask.getClass());
    assertEquals(false, rtask.needsStagingDirs());
    assertEquals(true, rtask.isActionable());

    rtask
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    for (Command c : rtask.getSrcWhCommands()){
      assertEquals(NoopCommand.class, c.getClass());
    }

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(1,dstCommands.size());
    assertEquals(DropTableCommand.class, dstCommands.get(0).getClass());

    DropTableCommand dropTableCommand = new DropTableCommand(
        debugMapping.apply(rtask.getEvent().getDbName()),
        debugMapping.apply(rtask.getEvent().getTableName()),
        true,
        rtask.getEvent().getEventId());

    assertEquals(ReplicationUtils.serializeCommand(dropTableCommand),
        ReplicationUtils.serializeCommand(dstCommands.get(0)));
  }

  @Test
  public void testAlterTable() throws IOException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_ALTER_TABLE_EVENT,
            msgFactory.buildAlterTableMessage(t, t, t.getWriteId()).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyAlterTableReplicationTask(rtask);
  }

  private static void verifyAlterTableReplicationTask(ReplicationTask rtask) throws IOException {
    assertEquals(AlterTableReplicationTask.class, rtask.getClass());
    assertEquals(true, rtask.needsStagingDirs());
    assertEquals(false, rtask.isActionable());

    rtask
        .withSrcStagingDirProvider(stagingDirectoryProvider)
        .withDstStagingDirProvider(stagingDirectoryProvider)
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    assertEquals(true, rtask.isActionable());

    List<? extends Command> srcCommands = Lists.newArrayList(rtask.getSrcWhCommands());
    assertEquals(1, srcCommands.size());
    assertEquals(ExportCommand.class, srcCommands.get(0).getClass());

    ExportCommand exportCommand = getExpectedExportCommand(rtask, null, true);

    assertEquals(ReplicationUtils.serializeCommand(exportCommand),
        ReplicationUtils.serializeCommand(srcCommands.get(0)));

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(1,dstCommands.size());
    assertEquals(ImportCommand.class, dstCommands.get(0).getClass());

    ImportCommand importCommand = getExpectedImportCommand(rtask, null, true);

    assertEquals(ReplicationUtils.serializeCommand(importCommand),
        ReplicationUtils.serializeCommand(dstCommands.get(0)));
  }

  @Test
  public void testAddPartition() throws IOException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    List<FieldSchema> pkeys = HCatSchemaUtils.getFieldSchemas(
        HCatSchemaUtils.getHCatSchema("a:int,b:string").getFields());
    t.setPartitionKeys(pkeys);

    List<Partition> addedPtns = new ArrayList<Partition>();
    addedPtns.add(createPtn(t, Arrays.asList("120", "abc")));
    addedPtns.add(createPtn(t, Arrays.asList("201", "xyz")));

    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_ADD_PARTITION_EVENT, msgFactory.buildAddPartitionMessage(t, addedPtns.iterator()).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyAddPartitionReplicationTask(rtask, t, addedPtns);

  }

  private static void verifyAddPartitionReplicationTask(ReplicationTask rtask, Table table, List<Partition> addedPtns) throws IOException {
    assertEquals(AddPartitionReplicationTask.class, rtask.getClass());
    assertEquals(true, rtask.needsStagingDirs());
    assertEquals(false,rtask.isActionable());

    rtask
        .withSrcStagingDirProvider(stagingDirectoryProvider)
        .withDstStagingDirProvider(stagingDirectoryProvider)
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    assertEquals(true,rtask.isActionable());

    List<? extends Command> srcCommands = Lists.newArrayList(rtask.getSrcWhCommands());
    assertEquals(2,srcCommands.size());
    assertEquals(ExportCommand.class, srcCommands.get(0).getClass());
    assertEquals(ExportCommand.class, srcCommands.get(1).getClass());

    ExportCommand exportCommand1 = getExpectedExportCommand(rtask, getPtnDesc(table,addedPtns.get(0)), false);
    ExportCommand exportCommand2 = getExpectedExportCommand(rtask, getPtnDesc(table,addedPtns.get(1)), false);

    CommandTestUtils.compareCommands(exportCommand1, srcCommands.get(0), true);
    CommandTestUtils.compareCommands(exportCommand2, srcCommands.get(1), true);

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(2,dstCommands.size());
    assertEquals(ImportCommand.class, dstCommands.get(0).getClass());
    assertEquals(ImportCommand.class, dstCommands.get(1).getClass());

    ImportCommand importCommand1 = getExpectedImportCommand(rtask, getPtnDesc(table,addedPtns.get(0)), false);
    ImportCommand importCommand2 = getExpectedImportCommand(rtask, getPtnDesc(table,addedPtns.get(1)), false);

    CommandTestUtils.compareCommands(importCommand1, dstCommands.get(0), true);
    CommandTestUtils.compareCommands(importCommand2, dstCommands.get(1), true);
  }

  private static Map<String, String> getPtnDesc(Table t, Partition p) {
    assertEquals(t.getPartitionKeysSize(),p.getValuesSize());
    Map<String,String> retval = new HashMap<String,String>();
    Iterator<String> pval = p.getValuesIterator();
    for (FieldSchema fs : t.getPartitionKeys()){
      retval.put(fs.getName(),pval.next());
    }
    return retval;
  }

  private static Partition createPtn(Table t, List<String> pvals) {
    Partition ptn = new Partition();
    ptn.setDbName(t.getDbName());
    ptn.setTableName(t.getTableName());
    ptn.setValues(pvals);
    return ptn;
  }

  @Test
  public void testDropPartition() throws HCatException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    List<FieldSchema> pkeys = HCatSchemaUtils.getFieldSchemas(
        HCatSchemaUtils.getHCatSchema("a:int,b:string").getFields());
    t.setPartitionKeys(pkeys);

    Partition p = createPtn(t, Arrays.asList("102", "lmn"));

    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_DROP_PARTITION_EVENT, msgFactory.buildDropPartitionMessage(
          t, Collections.singletonList(p).iterator()).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyDropPartitionReplicationTask(rtask, t, p);
  }

  private static void verifyDropPartitionReplicationTask(ReplicationTask rtask, Table table, Partition ptn) {
    assertEquals(DropPartitionReplicationTask.class, rtask.getClass());
    assertEquals(false, rtask.needsStagingDirs());
    assertEquals(true, rtask.isActionable());

    rtask
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    assertEquals(true,rtask.isActionable());

    for (Command c : rtask.getSrcWhCommands()){
      assertEquals(NoopCommand.class, c.getClass());
    }

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(1,dstCommands.size());
    assertEquals(DropPartitionCommand.class, dstCommands.get(0).getClass());

    DropPartitionCommand dropPartitionCommand = new DropPartitionCommand(
        debugMapping.apply(rtask.getEvent().getDbName()),
        debugMapping.apply(rtask.getEvent().getTableName()),
        getPtnDesc(table,ptn),
        true,
        rtask.getEvent().getEventId()
    );

    CommandTestUtils.compareCommands(dropPartitionCommand, dstCommands.get(0), true);
  }

  @Test
  public void testAlterPartition() throws HCatException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    List<FieldSchema> pkeys = HCatSchemaUtils.getFieldSchemas(
        HCatSchemaUtils.getHCatSchema("a:int,b:string").getFields());
    t.setPartitionKeys(pkeys);
    Partition p = createPtn(t, Arrays.asList("102", "lmn"));

    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_ALTER_PARTITION_EVENT, msgFactory.buildAlterPartitionMessage(t,
            p, p, p.getWriteId()).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyAlterPartitionReplicationTask(rtask, t, p);
  }

  private static void verifyAlterPartitionReplicationTask(ReplicationTask rtask, Table table, Partition ptn) {
    assertEquals(AlterPartitionReplicationTask.class, rtask.getClass());
    assertEquals(true, rtask.needsStagingDirs());
    assertEquals(false,rtask.isActionable());

    rtask
        .withSrcStagingDirProvider(stagingDirectoryProvider)
        .withDstStagingDirProvider(stagingDirectoryProvider)
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    assertEquals(true,rtask.isActionable());

    List<? extends Command> srcCommands = Lists.newArrayList(rtask.getSrcWhCommands());
    assertEquals(1,srcCommands.size());
    assertEquals(ExportCommand.class, srcCommands.get(0).getClass());

    ExportCommand exportCommand = getExpectedExportCommand(rtask, getPtnDesc(table, ptn), true);
    CommandTestUtils.compareCommands(exportCommand, srcCommands.get(0), true);

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(1,dstCommands.size());
    assertEquals(ImportCommand.class, dstCommands.get(0).getClass());

    ImportCommand importCommand = getExpectedImportCommand(rtask, getPtnDesc(table, ptn), true);
    CommandTestUtils.compareCommands(importCommand, dstCommands.get(0), true);
  }

  @Test
  public void testInsert() throws HCatException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    List<FieldSchema> pkeys = HCatSchemaUtils.getFieldSchemas(
        HCatSchemaUtils.getHCatSchema("a:int,b:string").getFields());
    t.setPartitionKeys(pkeys);
    Partition p = createPtn(t, Arrays.asList("102", "lmn"));
    List<String> files = Arrays.asList("/tmp/test123");

    NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
        HCatConstants.HCAT_INSERT_EVENT, msgFactory.buildInsertMessage(
        t.getDbName(),
        t.getTableName(),
        getPtnDesc(t,p),
        files
    ).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    HCatNotificationEvent hev = new HCatNotificationEvent(event);
    ReplicationTask rtask = ReplicationTask.create(client,hev);

    assertEquals(hev.toString(), rtask.getEvent().toString());
    verifyInsertReplicationTask(rtask, t, p);
  }

  private static void verifyInsertReplicationTask(ReplicationTask rtask, Table table, Partition ptn) {
    assertEquals(InsertReplicationTask.class, rtask.getClass());
    assertEquals(true, rtask.needsStagingDirs());
    assertEquals(false,rtask.isActionable());

    rtask
        .withSrcStagingDirProvider(stagingDirectoryProvider)
        .withDstStagingDirProvider(stagingDirectoryProvider)
        .withDbNameMapping(debugMapping)
        .withTableNameMapping(debugMapping);

    assertEquals(true,rtask.isActionable());

    List<? extends Command> srcCommands = Lists.newArrayList(rtask.getSrcWhCommands());
    assertEquals(1,srcCommands.size());
    assertEquals(ExportCommand.class, srcCommands.get(0).getClass());

    ExportCommand exportCommand = getExpectedExportCommand(rtask, getPtnDesc(table, ptn), false);
    CommandTestUtils.compareCommands(exportCommand, srcCommands.get(0), true);

    List<? extends Command> dstCommands = Lists.newArrayList(rtask.getDstWhCommands());
    assertEquals(1,dstCommands.size());
    assertEquals(ImportCommand.class, dstCommands.get(0).getClass());

    ImportCommand importCommand = getExpectedImportCommand(rtask, getPtnDesc(table, ptn), false);
    CommandTestUtils.compareCommands(importCommand, dstCommands.get(0), true);
  }

  private static long getEventId() {
    // Does not need to be unique, just non-zero distinct value to test against.
    return 42;
  }

  private static int getTime() {
    // Does not need to be actual time, just non-zero distinct value to test against.
    return 1729;
  }

  private static ImportCommand getExpectedImportCommand(ReplicationTask rtask, Map<String,String> ptnDesc, boolean isMetadataOnly) {
    String dbName = rtask.getEvent().getDbName();
    String tableName = rtask.getEvent().getTableName();
    return new ImportCommand(
        ReplicationUtils.mapIfMapAvailable(dbName, debugMapping),
        ReplicationUtils.mapIfMapAvailable(tableName, debugMapping),
        ptnDesc,
        stagingDirectoryProvider.getStagingDirectory(
            ReplicationUtils.getUniqueKey(
                rtask.getEvent().getEventId(),
                dbName,
                tableName,
                ptnDesc)
        ),
        isMetadataOnly,
        rtask.getEvent().getEventId()
    );
  }

  private static ExportCommand getExpectedExportCommand(ReplicationTask rtask, Map<String, String> ptnDesc, boolean isMetadataOnly) {
    String dbName = rtask.getEvent().getDbName();
    String tableName = rtask.getEvent().getTableName();
    return new ExportCommand(
        dbName,
        tableName,
        ptnDesc,
        stagingDirectoryProvider.getStagingDirectory(
            ReplicationUtils.getUniqueKey(
                rtask.getEvent().getEventId(),
                dbName,
                tableName,
                ptnDesc)
        ),
        isMetadataOnly,
        rtask.getEvent().getEventId()
    );
  }

}
