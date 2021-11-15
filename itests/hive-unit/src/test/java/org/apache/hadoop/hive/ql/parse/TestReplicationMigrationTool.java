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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.ReplicationMigrationTool;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReplicationMigrationTool extends BaseReplicationAcrossInstances {

  String extraPrimaryDb;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

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

    internalBeforeClassSetup(overrides, TestReplicationScenarios.class);
  }

  @Before
  public void setup() throws Throwable {
    redirectStream();
    super.setup();
    extraPrimaryDb = "extra_" + primaryDbName;
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + extraPrimaryDb + " cascade");
    super.tearDown();
    resetStream();
  }

  private void redirectStream() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  private void resetStream() {
    out.reset();
    err.reset();
  }

  @Test
  public void testSuccessfulSync() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);

    // Create one normal table with 3 files, 1 partitioned table & one empty table.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName).run("create external table table1 (id int)")
        .run("insert into table table1 values (100)").run("insert into table table1 values (200),(300),(400)")
        .run("insert into table table1 values (500),(600)")
        .run("create external table table2 (place string) partitioned by (country string)")
        .run("insert into table table2 partition(country='india') values ('chennai')")
        .run("insert into table table2 partition(country='india') values ('jaipur'),('udaipur'),('kota')")
        .run("insert into table table2 partition(country='us') values ('new york')")
        .run("insert into table table2 partition(country='france') values ('paris')")
        .run("create external table table3 (id int) partitioned by (department string)")
        .dump(primaryDbName, withClause);

    verifySuccessfulResult(tuple);

    replica.load(replicatedDbName, primaryDbName, withClause).run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId).verifyReplTargetProperty(replicatedDbName);

    verifySuccessfulResult(tuple);

    // Insert some more data and new tables, and check the verification is success after incremental as well.

    tuple =
        primary.run("use " + primaryDbName).run("create external table table4 (id int) partitioned by (name string)")
            .run("insert into table table1 values (2000)").run("insert into table table1 values (880),(660),(440)")
            .run("insert into table table2 partition(country='nepal') values ('kathmandu'),('pokhra')").run(
            "insert into table table2 partition(country='india') values ('lucknow'),('kanpur'),('agra'),('varanasi'),"
                + "('delhi')").run("insert into table table3 partition(department='engg') values (10),(40),(50),(60)")
            .run("insert into table table3 partition(department='support') values (20),(350)")
            .run("insert into table table3 partition(department='support') values (20),(350)")
            .run("insert into table table4 partition(name='abc') values (120),(3150)").dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause).run("use " + replicatedDbName)
        .verifyReplTargetProperty(replicatedDbName);

    verifySuccessfulResult(tuple);
  }

  @Test
  public void testExtraAndMissingFilesAtSource() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    Path externalTableLocation = new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    //externally add data to location
    try (FSDataOutputStream outputStream = fs.create(new Path(externalTableLocation, "file1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table tablea (i int, j int) row format delimited fields terminated by ',' " + "location '"
            + externalTableLocation.toUri() + "'")
        .run("create external table tableb (id int)")
        .run("insert into table tableb values (25)")
        .run("insert into table tableb values (28),(36),(42)")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause).run("use " + replicatedDbName)
        .verifyReplTargetProperty(replicatedDbName);

    verifySuccessfulResult(tuple);

    //externally add one more file to location
    try (FSDataOutputStream outputStream = fs.create(new Path(externalTableLocation, "file2.txt"))) {
      outputStream.write("10,20\n".getBytes());
      outputStream.write("15,27\n".getBytes());
    }

    ReplicationMigrationTool replTool = new ReplicationMigrationTool();

    verifyFailures(tuple, replTool, "Directory Size mismatch", "Extra entry at source", "Extra entry at source");

    // Do a dump & load cycle so that source & target are again in sync, the script should return success.
    tuple = doAnEmptyDumpAndLoadCycle(withClause);

    verifySuccessfulResult(tuple);

    // Now delete one file at source & do a verification.
    fs.delete(new Path(externalTableLocation, "file1.txt"), true);

    verifyFailures(tuple, replTool, "Directory Size mismatch", "Source Or Target has an extra/less files",
        "Source Or Target has an extra/less files.");

    // Do a dump & load cycle so that source & target are again in sync, the script should return success.
    tuple = doAnEmptyDumpAndLoadCycle(withClause);

    verifySuccessfulResult(tuple);
  }

  @Test
  public void testModifiedContentAndOpenFiles() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    Path externalTableLocationa = new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocationa, new FsPermission("777"));

    Path externalTableLocationb = new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "b");
    fs.mkdirs(externalTableLocationb, new FsPermission("777"));

    //externally add data to location
    try (FSDataOutputStream outputStream = fs.create(new Path(externalTableLocationa, "filea1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }

    try (FSDataOutputStream outputStream = fs.create(new Path(externalTableLocationb, "fileb1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName).run(
        "create external table tablea (i int, j int) row format delimited fields terminated by ',' " + "location '"
            + externalTableLocationa.toUri() + "'").run(
        "create external table tableb (i int, j int) row format delimited fields terminated by ',' " + "location '"
            + externalTableLocationb.toUri() + "'").run("insert into table tableb values (25,26)")
        .run("insert into table tableb values (28,29),(36,37),(42,43)").dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause).run("use " + replicatedDbName)
        .verifyReplTargetProperty(replicatedDbName);

    // Everything is in sync, script should return success.
    verifySuccessfulResult(tuple);

    // Alter the content of one of the file, keeping the bytes same
    try (FSDataOutputStream outputStream = fs.create(new Path(externalTableLocationb, "fileb1.txt"))) {
      outputStream.write("1,4\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }

    // All other checks shall falsely pass, the checksum verification, should catch and fail.
    ReplicationMigrationTool replTool = new ReplicationMigrationTool();
    validateChecksumValidationFails(replTool, tuple, "File Checksum mismatch");

    // Do a dump & load cycle and check everything should be in sync.
    tuple = doAnEmptyDumpAndLoadCycle(withClause);

    verifySuccessfulResult(tuple);

    // Modify the content of a file without keeping the number of bytes same.
    try (FSDataOutputStream outputStream = fs.create(new Path(externalTableLocationa, "filea1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
      outputStream.write("25,50\n".getBytes());
    }
    validateFileLevelValidationFails(replTool, tuple, "File Size mismatch");
    validateChecksumValidationFails(replTool, tuple, "File Size mismatch");

    // Do a dump & load cycle and check everything should be in sync.
    tuple = doAnEmptyDumpAndLoadCycle(withClause);
    verifySuccessfulResult(tuple);

    // Open a file and check if it gets caught by the verify open file option.
    FSDataOutputStream stream = fs.append(new Path(externalTableLocationa, "filea1.txt"));
    ToolRunner.run(conf, replTool,
        new String[] { "-dumpFilePath", tuple.dumpLocation, "-fileLevelCheck", "-verifyOpenFiles" });
    fail("Script didn't fail despite having an open file.");
    // Make sure we get the exception.
    assertTrue(err.toString(), err.toString().contains("There are open files"));
    err.reset();
    out.reset();
    // Confirm that successful message is not printed, and failure message is printed.
    assertFalse(out.toString(), out.toString().contains("Completed verification. Source & Target are in Sync."));
    assertTrue(out.toString(), out.toString().contains("Completed verification. Source & Target are not in Sync."));

    // Close the file and check, the script should return success.
    stream.close();
    verifySuccessfulResult(tuple);
  }

  @Test
  public void testFilters() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    Path externalTableLocation = new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    //externally add data to location
    try (FSDataOutputStream outputStream = fs.create(new Path(externalTableLocation, "file1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }

    // Create file & dir that needs to be filtered out.
    Path random1 = new Path(externalTableLocation, "randomStuff1");
    DFSTestUtil.createFile(fs, random1, 1024L, (short) 1, 1024L);

    Path random2 = new Path(externalTableLocation, "randomStuff2");
    DFSTestUtil.createFile(fs, random2, 1024L, (short) 1, 1024L);

    // Create a filter file for DistCp
    String filterFilePath = "/tmp/filter";
    FileWriter myWriter = new FileWriter(filterFilePath);
    myWriter.write(".*randomStuff.*");
    myWriter.close();

    withClause.add("'distcp.options.filters'='" + filterFilePath + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName).run(
        "create external table tablea (i int, j int) row format delimited fields terminated by ',' " + "location '"
            + externalTableLocation.toUri() + "'").run("insert into table tablea values (215,226)")
        .run("insert into table tablea values (281,229),(336,347),(542,453)")
        .run("create external table tableb (id int)").run("insert into table tableb values (15)")
        .run("insert into table tableb values (24),(26),(82)").dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause).run("use " + replicatedDbName)
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the filtered files didn't get copied.
    assertFalse(fs.exists(new Path(REPLICA_EXTERNAL_BASE, random1.toUri().getPath().replaceFirst("/", ""))));
    assertFalse(fs.exists(new Path(REPLICA_EXTERNAL_BASE, random2.toUri().getPath().replaceFirst("/", ""))));

    ReplicationMigrationTool replTool = new ReplicationMigrationTool();
    // Verify at file level.
    assertEquals(0, ToolRunner.run(conf, replTool,
        new String[] { "-dumpFilePath", tuple.dumpLocation, "-fileLevelCheck", "-filters", ".*randomStuff.*" }));
    assertTrue(out.toString(), out.toString().contains("Completed verification. Source & Target are in Sync."));
    out.reset();

    // Verify at file level, with checksum.
    assertEquals(0, ToolRunner.run(conf, replTool,
        new String[] { "-dumpFilePath", tuple.dumpLocation, "-fileLevelCheck", "-verifyChecksum", "-filters",
            ".*randomStuff.*" }));
    assertTrue(out.toString(), out.toString().contains("Completed verification. Source & Target are in Sync."));
    out.reset();
  }

  private WarehouseInstance.Tuple doAnEmptyDumpAndLoadCycle(List<String> withClause) throws Throwable {
    WarehouseInstance.Tuple tuple;
    tuple = primary.run("use " + primaryDbName).dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause).run("use " + replicatedDbName)
        .verifyReplTargetProperty(replicatedDbName);
    return tuple;
  }

  private void validateChecksumValidationFails(ReplicationMigrationTool replTool, WarehouseInstance.Tuple tuple,
      String message) throws Exception {
    ToolRunner.run(conf, replTool,
        new String[] { "-dumpFilePath", tuple.dumpLocation, "-fileLevelCheck", "-verifyChecksum" });
    fail("Script didn't fail despite having an extra file.");
    // Make sure we get the exception.
    assertTrue(err.toString(), err.toString().contains(message));
    // Confirm that successful message is not printed.
    assertFalse(out.toString(), out.toString().contains("Completed verification. Source & Target are in Sync."));
    assertTrue(out.toString(), out.toString().contains("Completed verification. Source & Target are not in Sync."));

    out.reset();
    err.reset();
  }

  private void verifyFailures(WarehouseInstance.Tuple tuple, ReplicationMigrationTool replTool,
      String dirLevelFailureMsg, String fileLevelFailureMsg, String checksumLevelFailureMsg) throws Exception {
    // Verify failures at directory level.
    ToolRunner.run(conf, replTool, new String[] { "-dumpFilePath", tuple.dumpLocation, "-dirLevelCheck" });
    // Make sure we get the exception.
    assertTrue(err.toString().contains(dirLevelFailureMsg));
    // Confirm that successful message is not printed and failure message is printed.
    assertFalse(out.toString(), out.toString().contains("Completed verification. Source & Target are in Sync."));
    assertTrue(out.toString(), out.toString().contains("Completed verification. Source & Target are not in Sync."));
    out.reset();
    err.reset();

    // Verify failures at file level.
    validateFileLevelValidationFails(replTool, tuple, fileLevelFailureMsg);

    // Verify failures at file level with checksum.
    validateChecksumValidationFails(replTool, tuple, checksumLevelFailureMsg);
  }

  private void validateFileLevelValidationFails(ReplicationMigrationTool replTool, WarehouseInstance.Tuple tuple,
      String fileLevelFailureMsg) throws Exception {

    ToolRunner.run(conf, replTool, new String[] { "-dumpFilePath", tuple.dumpLocation, "-fileLevelCheck", });

    // Make sure we get the exception.
    assertTrue(err.toString(), err.toString().contains(fileLevelFailureMsg));
    // Confirm that successful message is not printed and failure message is printed.
    assertFalse(out.toString(), out.toString().contains("Completed verification. Source & Target are in Sync."));
    assertTrue(out.toString(), out.toString().contains("Completed verification. Source & Target are not in Sync."));

    out.reset();
    err.reset();
  }

  private void verifySuccessfulResult(WarehouseInstance.Tuple tuple) throws Exception {
    // Verify at directory level. use all three way the dumpFilePath can be specified, one full path, second with
    // /hive and third just the path.
    ReplicationMigrationTool replTool = new ReplicationMigrationTool();
    assertEquals(0,
        ToolRunner.run(conf, replTool, new String[] { "-dumpFilePath", tuple.dumpLocation +"/hive/_file_list_external", "-dirLevelCheck" }));
    assertTrue(out.toString().contains("Completed verification"));
    assertFalse(err.toString(), err.toString().isEmpty());
    out.reset();
    err.reset();

    // Verify at file level.
    assertEquals(0, ToolRunner
        .run(conf, replTool, new String[] { "-dumpFilePath", tuple.dumpLocation + "/hive", "-fileLevelCheck" }));
    assertTrue(out.toString().contains("Completed verification."));
    assertFalse(err.toString(), err.toString().isEmpty());
    out.reset();
    err.reset();

    // Verify at file level, with checksum.
    assertEquals(0, ToolRunner.run(conf, replTool,
        new String[] { "-dumpFilePath", tuple.dumpLocation, "-fileLevelCheck", "-verifyChecksum" }));
    assertTrue(out.toString().contains("Completed verification. Source & Target are in Sync."));
    assertFalse(err.toString(), err.toString().isEmpty());
    out.reset();
    err.reset();
  }
}
