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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestReplicationIncrementalCheckpointing extends BaseReplicationAcrossInstances {

  String extraPrimaryDb;
  private LoggerConfig loggerConfig;
  private LoggerContext ctx;
  private Level oldLevel;
  private StringAppender appender;
  private Logger logger;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());
    overrides.put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false");
    overrides.put(HiveConf.ConfVars.HIVE_EXTERNALTABLE_PURGE_DEFAULT.varname, "true");

    internalBeforeClassSetup(overrides, TestReplicationScenarios.class);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
    logger = LogManager.getLogger("hive.ql.metadata.Hive");
    oldLevel = logger.getLevel();
    ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    loggerConfig = config.getLoggerConfig(logger.getName());
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.DEBUG);
    appender.start();
    extraPrimaryDb = "extra_" + primaryDbName;
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + extraPrimaryDb + " cascade");
    super.tearDown();
    loggerConfig.setLevel(oldLevel);
    ctx.updateLoggers();
    appender.removeFromLogger(logger.getName());
  }

  @Test
  public void testDropEventReplayExternalTable() throws Throwable {
    testDropEventReplay("external");
  }

  @Test
  public void testDropEventReplayManagedTable() throws Throwable {
    testDropEventReplay("");
  }

  public void testDropEventReplay(String tableType) throws Throwable {

    // Create an empty table and do a dump & load.
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create " + tableType + " table t1 (id int)")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Insert some data and drop the table, do a dump & load cycle post that
    tuple = primary
        .run("use " + primaryDbName)
        .run("insert into table t1 values(1),(2)")
        .run("drop table t1")
        .dump(primaryDbName);

    // Change the repl id in the database to one before the drop partition event.
    int replId = Integer.parseInt(tuple.lastReplicationId) - 1;
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Check the log if the event got replayed.
    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_DROP_TABLE"));

    // Create table with same name again and dump, to see drop event replay doesn't bother anything further.
    tuple = primary
        .run("use " + primaryDbName)
        .run("create " + tableType + " table t1 (id int)")
        .run("insert into table t1 values(1),(2)")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResults(new String[] {"t1"})
        .run("select id from t1")
        .verifyResults(new String[] {"1", "2"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Add some more data and delete the table & then try checkpointing.
    tuple = primary
        .run("use " + primaryDbName)
        .run("insert into table t1 values(3)")
        .run("drop table t1")
        .dump(primaryDbName);

    // Change repl id as one above drop table.
    replId = Integer.parseInt(tuple.lastReplicationId) - 1;
    // Check whether the table is deleted
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    // Check whether the table stays deleted and no failure.
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Check the log if the event got replayed.
    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_DROP_TABLE"));
  }

  @Test
  public void testCreateEventReplayExternalTable() throws Throwable {
    testCreateEventReplay("external");
  }

  @Test
  public void testCreateEventReplayManagedTable() throws Throwable {
    testCreateEventReplay("");
  }

  public void testCreateEventReplay(String tableType) throws Throwable {
    // Do an empty dump & load to move to incremental mode.
    WarehouseInstance.Tuple tuple = primary
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Create a table and do a dump & load.
    tuple = primary
        .run("use " + primaryDbName)
        .run("create " + tableType + " table t1 (id int)")
        .dump(primaryDbName);

    int replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResults(new String[] {"t1"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResults(new String[] {"t1"});

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_CREATE_TABLE"));

    // Drop & create a table with different schema.
    tuple = primary
        .run("use " + primaryDbName)
        .run("drop table t1")
        .run("create " + tableType + " table t1 (name string)")
        .dump(primaryDbName);

    replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResults(new String[] {"t1"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResults(new String[] {"t1"});

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_CREATE_TABLE"));
  }

  @Test
  public void testInsertEventReplay() throws Throwable {
    // Do an empty dump & load to move to incremental mode.
    WarehouseInstance.Tuple tuple = primary
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Create a table and do add two insert statements dump & load.
    tuple = primary
        .run("use " + primaryDbName)
        .run("create table t1 (id int)")
        .run("insert into table t1 values(1),(2)")
        .dump(primaryDbName);

    int replId = Integer.parseInt(tuple.lastReplicationId) - 3;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from t1")
        .verifyResults(new String[] {"1", "2"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from t1")
        .verifyResults(new String[] {"1", "2"});

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_INSERT"));
    // Drop & create a table with different schema.
    tuple = primary
        .run("use " + primaryDbName)
        .run("drop table t1")
        .run("create table t1 (name string)")
        .run("insert into table t1 values ('one'),('two')")
        .dump(primaryDbName);

    replId = Integer.parseInt(tuple.lastReplicationId) - 3;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select name from t1")
        .verifyResults(new String[] {"one", "two"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select name from t1")
        .verifyResults(new String[] {"one", "two"});

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_INSERT"));
  }

  @Test
  public void testAlterTableReplayExternalTable() throws Throwable {
    testAlterTableReplay("external");
  }

  @Test
  public void testAlterTableReplayManagedTable() throws Throwable {
    testAlterTableReplay("");
  }


  public void testAlterTableReplay(String tableType) throws Throwable {
    // create a partitioned table
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create " + tableType +" table t1 (id int)")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Do an alter table operation.
    tuple = primary
        .run("use " + primaryDbName)
        .run("alter table t1 set tblproperties('dummy_key'='dummy_val')")
        .dump(primaryDbName);

    int replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tblproperties t1('dummy_key')")
        .verifyResults(new String[] { "dummy_key\tdummy_val" })
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tblproperties t1('dummy_key')")
        .verifyResults(new String[] { "dummy_key\tdummy_val" });

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_ALTER_TABLE"));

    // Try Alter table rename.
    tuple = primary
        .run("use " + primaryDbName)
        .run("alter table t1 RENAME TO t1_renamed")
        .dump(primaryDbName);

    replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("show tables like 't1_renamed'")
        .verifyResults(new String[] {"t1_renamed"})
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("show tables like 't1_renamed'")
        .verifyResults(new String[] {"t1_renamed"});

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_RENAME_TABLE"));

    // Test truncate table.

    // Add some data and do a dump & load to the table
    primary
        .run("use " + primaryDbName)
        .run("insert into table t1_renamed values (1),(2),(3),(4)")
        .dump(primaryDbName);

    // Add some data and do a dump & load to the table
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from t1_renamed")
        .verifyResults(new String[] {"1", "2", "3", "4"});

    // Truncate the table
    tuple = primary
        .run("use " + primaryDbName)
        .run("truncate table t1_renamed")
        .dump(primaryDbName);

    replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("show tables like 't1_renamed'")
        .verifyResults(new String[] {"t1_renamed"})
        .run("use " + replicatedDbName)
        .run("select id from t1_renamed")
        .verifyFailure(new String[] {"1", "2", "3", "4"})
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("show tables like 't1_renamed'")
        .verifyResults(new String[] {"t1_renamed"})
        .run("use " + replicatedDbName)
        .run("select id from t1_renamed")
        .verifyFailure(new String[] {"1", "2", "3", "4"});
  }

  @Test
  public void testAddPartitionReplay() throws Throwable {
    String tableType = "";
    // create a partitioned table and do a dump & load.
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create " + tableType +" table t1 (place string) partitioned by (country string)")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId);

    // Do a add partition operation.
    tuple = primary
        .run("use " + primaryDbName)
        .run("alter table t1 add partition(country='india')")
        .run("show partitions t1")
        .verifyResults(new String[] {"country=india"})
        .dump(primaryDbName);

    int replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show partitions t1")
        .verifyResults(new String[] {"country=india" })
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show partitions t1")
        .verifyResults(new String[] {"country=india" });

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_ADD_PARTITION"));

  }

  @Test
  public void testDropPartitionReplay() throws Throwable {
    String tableType = "";
    // create a partitioned table and do a dump & load, with a partition
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create " + tableType +" table t1 (place string) partitioned by (country string)")
        .run("alter table t1 add partition(country='india')")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("show partitions t1")
        .verifyResults(new String[] {"country=india" });

    // Do a drop partition operation.
    tuple = primary
        .run("use " + primaryDbName)
        .run("alter table t1 drop partition(country='india')")
        .run("show partitions t1")
        .verifyFailure(new String[] {"country=india"})
        .dump(primaryDbName);

    int replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show partitions t1")
        .verifyFailure(new String[] {"country=india" })
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show partitions t1")
        .verifyFailure(new String[] {"country=india"});

    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_DROP_PARTITION"));
  }

  @Test
  public void testAlterPartitionReplay() throws Throwable {
    String tableType = "";
    // create a partitioned table and do a dump & load, with a partition
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create " + tableType +" table t1 (place string) partitioned by (country string)")
        .run("alter table t1 add partition(country='india')")
        .dump(primaryDbName);

    String whLocation = tableType.isEmpty() ? primary.getDatabase(primaryDbName).getManagedLocationUri() : primary
        .getDatabase(primaryDbName).getLocationUri();

    Path whPath = new Path(whLocation);
    Path newPartPath = new Path(whPath, "newLoc");

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("show partitions t1")
        .verifyResults(new String[] {"country=india" });

    // Do an alter partition operation to change location of the table.
    if (!tableType.isEmpty()) {
      tuple = primary.run("use " + primaryDbName)
          .run("alter table t1 partition(country='india') set location '" + newPartPath + "'")
          .dump(primaryDbName);

      int replId = Integer.parseInt(tuple.lastReplicationId) - 1;

      replica.load(replicatedDbName, primaryDbName)
          .run("use " + replicatedDbName).run("show partitions t1")
          .verifyResults(new String[] { "country=india" })
          .run("alter database " + replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

      assertTrue(replica.getPartition(replicatedDbName, "t1", Collections.singletonList("india")).getSd().getLocation()
          .contains("newLoc"));

      // Delete the load ACK.
      deleteLoadAck(tuple);

      // Reset the logger
      appender.reset();

      replica.load(replicatedDbName, primaryDbName)
          .run("use " + replicatedDbName)
          .run("show partitions t1")
          .verifyResults(new String[] { "country=india" });

      assertTrue(replica.getPartition(replicatedDbName, "t1", Collections.singletonList("india")).getSd().getLocation()
          .contains("newLoc"));

      assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_ALTER_PARTITION"));
    }

    // Do a rename partition and see if things stay same.
    tuple = primary.run("use " + primaryDbName)
        .run("alter table t1 partition (country='india') rename to partition(country='usa')")
        .dump(primaryDbName);

    int replId = Integer.parseInt(tuple.lastReplicationId) - 1;

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName).run("show partitions t1")
        .run("show partitions t1")
        .verifyResults(new String[] { "country=usa" })
        .run("alter database " + replicatedDbName + " set DBPROPERTIES('repl.last.id'='" + replId + "')");

    // Delete the load ACK.
    deleteLoadAck(tuple);

    // Reset the logger
    appender.reset();

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName).run("show partitions t1")
        .run("show partitions t1")
        .verifyResults(new String[] { "country=usa" });
    assertTrue(appender.getOutput(), appender.getOutput().contains("Loading event EVENT_RENAME_PARTITION"));

  }



  private void deleteLoadAck(WarehouseInstance.Tuple tuple) throws IOException {
    Path hiveDumpDir = new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path loadAck = new Path(hiveDumpDir, ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    fs.delete(loadAck, true);
  }
}
