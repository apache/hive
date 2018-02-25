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

package org.apache.hadoop.hive.ql.metadata;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.BasicTxnInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.index.HiveIndex;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;

import com.google.common.collect.ImmutableMap;

import junit.framework.TestCase;

/**
 * TestHive.
 *
 */
public class TestHive extends TestCase {
  protected Hive hm;
  protected HiveConf hiveConf;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());
    hiveConf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    // enable trash so it can be tested
    hiveConf.setFloat("fs.trash.checkpoint.interval", 30);  // FS_TRASH_CHECKPOINT_INTERVAL_KEY (hadoop-2)
    hiveConf.setFloat("fs.trash.interval", 30);             // FS_TRASH_INTERVAL_KEY (hadoop-2)
    SessionState.start(hiveConf);
    try {
      hm = Hive.get(hiveConf);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err
          .println("Unable to initialize Hive Metastore using configuration: \n "
          + hiveConf);
      throw e;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      // disable trash
      hiveConf.setFloat("fs.trash.checkpoint.interval", 30);  // FS_TRASH_CHECKPOINT_INTERVAL_KEY (hadoop-2)
      hiveConf.setFloat("fs.trash.interval", 30);             // FS_TRASH_INTERVAL_KEY (hadoop-2)
      Hive.closeCurrent();
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err
          .println("Unable to close Hive Metastore using configruation: \n "
          + hiveConf);
      throw e;
    }
  }

  public void testTable() throws Throwable {
    try {
      // create a simple table and test create, drop, get
      String tableName = "table_for_testtable";
      try {
        hm.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e1) {
        e1.printStackTrace();
        assertTrue("Unable to drop table", false);
      }

      Table tbl = new Table(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      List<FieldSchema> fields = tbl.getCols();

      fields.add(new FieldSchema("col1", serdeConstants.INT_TYPE_NAME, "int -- first column"));
      fields.add(new FieldSchema("col2", serdeConstants.STRING_TYPE_NAME, "string -- second column"));
      fields.add(new FieldSchema("col3", serdeConstants.DOUBLE_TYPE_NAME, "double -- thrift column"));
      tbl.setFields(fields);

      tbl.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
      tbl.setInputFormatClass(SequenceFileInputFormat.class);

      tbl.setProperty("comment", "this is a test table created as part junit tests");

      List<String> bucketCols = tbl.getBucketCols();
      bucketCols.add("col1");
      try {
        tbl.setBucketCols(bucketCols);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to set bucket column for table: " + tableName, false);
      }

      List<FieldSchema> partCols = new ArrayList<FieldSchema>();
      partCols
          .add(new FieldSchema(
          "ds",
          serdeConstants.STRING_TYPE_NAME,
          "partition column, date but in string format as date type is not yet supported in QL"));
      tbl.setPartCols(partCols);

      tbl.setNumBuckets((short) 512);
      tbl.setOwner("pchakka");
      tbl.setRetention(10);

      // set output format parameters (these are not supported by QL but only
      // for demo purposes)
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, "1");
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, "\n");
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, "3");
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, "2");

      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, "1");
      tbl.setSerializationLib(LazySimpleSerDe.class.getName());
      tbl.setStoredAsSubDirectories(false);

      tbl.setRewriteEnabled(false);

      // create table
      setNullCreateTableGrants();
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to create table: " + tableName, false);
      }

      // get table
      validateTable(tbl, tableName);

      try {
        hm.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName, true,
            false);
        Table ft2 = hm.getTable(Warehouse.DEFAULT_DATABASE_NAME,
            tableName, false);
        assertNull("Unable to drop table ", ft2);
      } catch (HiveException e) {
        assertTrue("Unable to drop table: " + tableName, false);
      }
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTable failed");
      throw e;
    }
  }

  private void setNullCreateTableGrants() {
    //having a non null create table grants privileges causes problems in
    // the tests that compares underlying thrift Table object of created
    // table with a table object that was fetched from metastore.
    // This is because the fetch does not populate the privileges field in Table
    SessionState.get().setCreateTableGrants(null);
  }

  /**
   * Tests create and fetch of a thrift based table.
   *
   * @throws Throwable
   */
  public void testThriftTable() throws Throwable {
    String tableName = "table_for_test_thrifttable";
    try {
      try {
        hm.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e1) {
        System.err.println(StringUtils.stringifyException(e1));
        assertTrue("Unable to drop table", false);
      }
      Table tbl = new Table(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
      tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
      tbl.setSerializationLib(ThriftDeserializer.class.getName());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_CLASS, Complex.class.getName());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, TBinaryProtocol.class
          .getName());
      tbl.setStoredAsSubDirectories(false);

      tbl.setRewriteEnabled(false);

      setNullCreateTableGrants();
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create table: " + tableName, false);
      }
      // get table
      validateTable(tbl, tableName);
      hm.dropTable(DEFAULT_DATABASE_NAME, tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testThriftTable() failed");
      throw e;
    }
  }


  /**
   * Test logging of timing for metastore api calls
   *
   * @throws Throwable
   */
  public void testMetaStoreApiTiming() throws Throwable {
    // Get the RootLogger which, if you don't have log4j2-test.properties defined, will only log ERRORs
    Logger logger = LogManager.getLogger("hive.ql.metadata.Hive");
    Level oldLevel = logger.getLevel();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();

    // Create a String Appender to capture log output
    StringAppender appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.DEBUG);
    appender.start();

    try {
      hm.clearMetaCallTiming();
      hm.getAllDatabases();
      hm.dumpAndClearMetaCallTiming("test");
      String logStr = appender.getOutput();
      String expectedString = "getAllDatabases_()=";
      Assert.assertTrue(logStr + " should contain <" + expectedString,
          logStr.contains(expectedString));

      // reset the log buffer, verify new dump without any api call does not contain func
      appender.reset();
      hm.dumpAndClearMetaCallTiming("test");
      logStr = appender.getOutput();
      Assert.assertFalse(logStr + " should not contain <" + expectedString,
          logStr.contains(expectedString));
    } finally {
      loggerConfig.setLevel(oldLevel);
      ctx.updateLoggers();
      appender.removeFromLogger(logger.getName());
    }
  }

  /**
   * Gets a table from the metastore and compares it to the original Table
   *
   * @param tbl
   * @param tableName
   * @throws MetaException
   */
  private void validateTable(Table tbl, String tableName) throws MetaException {
    Warehouse wh = new Warehouse(hiveConf);
    Table ft = null;
    try {
      // hm.getTable result will not have privileges set (it does not retrieve
      // that part from metastore), so unset privileges to null before comparing
      // (create table sets it to empty (non null) structures)
      tbl.getTTable().setPrivilegesIsSet(false);

      ft = hm.getTable(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      assertNotNull("Unable to fetch table", ft);
      ft.checkValidity(hiveConf);
      assertEquals("Table names didn't match for table: " + tableName, tbl
          .getTableName(), ft.getTableName());
      assertEquals("Table owners didn't match for table: " + tableName, tbl
          .getOwner(), ft.getOwner());
      assertEquals("Table retention didn't match for table: " + tableName,
          tbl.getRetention(), ft.getRetention());
      assertEquals("Data location is not set correctly",
          wh.getDefaultTablePath(hm.getDatabase(DEFAULT_DATABASE_NAME), tableName).toString(),
          ft.getDataLocation().toString());
      // now that URI and times are set correctly, set the original table's uri and times
      // and then compare the two tables
      tbl.setDataLocation(ft.getDataLocation());
      tbl.setCreateTime(ft.getTTable().getCreateTime());
      tbl.getParameters().put(hive_metastoreConstants.DDL_TIME,
          ft.getParameters().get(hive_metastoreConstants.DDL_TIME));
      assertTrue("Tables  doesn't match: " + tableName + " (" + ft.getTTable()
          + "; " + tbl.getTTable() + ")", ft.getTTable().equals(tbl.getTTable()));
      assertEquals("SerializationLib is not set correctly", tbl
          .getSerializationLib(), ft.getSerializationLib());
      assertEquals("Serde is not set correctly", tbl.getDeserializer()
          .getClass().getName(), ft.getDeserializer().getClass().getName());
    } catch (HiveException e) {
      System.err.println(StringUtils.stringifyException(e));
      assertTrue("Unable to fetch table correctly: " + tableName, false);
    }
  }

  private static Table createTestTable(String dbName, String tableName) throws HiveException {
    Table tbl = new Table(dbName, tableName);
    tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
    tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
    tbl.setSerializationLib(ThriftDeserializer.class.getName());
    tbl.setSerdeParam(serdeConstants.SERIALIZATION_CLASS, Complex.class.getName());
    tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, TBinaryProtocol.class
        .getName());
    return tbl;
  }

  /**
   * Test basic Hive class interaction, that:
   * - We can have different Hive objects throughout the lifetime of this thread.
   */
  public void testHiveCloseCurrent() throws Throwable {
    Hive hive1 = Hive.get();
    Hive.closeCurrent();
    Hive hive2 = Hive.get();
    Hive.closeCurrent();
    assertTrue(hive1 != hive2);
  }

  public void testGetAndDropTables() throws Throwable {
    try {
      String dbName = "db_for_testgettables";
      String table1Name = "table1";
      hm.dropDatabase(dbName, true, true, true);

      Database db = new Database();
      db.setName(dbName);
      hm.createDatabase(db);

      List<String> ts = new ArrayList<String>(2);
      ts.add(table1Name);
      ts.add("table2");
      Table tbl1 = createTestTable(dbName, ts.get(0));
      hm.createTable(tbl1);

      Table tbl2 = createTestTable(dbName, ts.get(1));
      hm.createTable(tbl2);

      List<String> fts = hm.getTablesForDb(dbName, ".*");
      assertEquals(ts, fts);
      assertEquals(2, fts.size());

      fts = hm.getTablesForDb(dbName, ".*1");
      assertEquals(1, fts.size());
      assertEquals(ts.get(0), fts.get(0));

      // also test getting a table from a specific db
      Table table1 = hm.getTable(dbName, table1Name);
      assertNotNull(table1);
      assertEquals(table1Name, table1.getTableName());

      FileSystem fs = table1.getPath().getFileSystem(hiveConf);
      assertTrue(fs.exists(table1.getPath()));
      // and test dropping this specific table
      hm.dropTable(dbName, table1Name);
      assertFalse(fs.exists(table1.getPath()));

      // Drop all tables
      for (String tableName : hm.getAllTables(dbName)) {
        Table table = hm.getTable(dbName, tableName);
        hm.dropTable(dbName, tableName);
        assertFalse(fs.exists(table.getPath()));
      }
      hm.dropDatabase(dbName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testGetAndDropTables() failed");
      throw e;
    }
  }

  public void testDropTableTrash() throws Throwable {
    if (!ShimLoader.getHadoopShims().supportTrashFeature()) {
      return; // it's hadoop-1
    }
    try {
      String dbName = "db_for_testdroptable";
      hm.dropDatabase(dbName, true, true, true);

      Database db = new Database();
      db.setName(dbName);
      hm.createDatabase(db);

      List<String> ts = new ArrayList<String>(2);
      String tableBaseName = "droptable";
      ts.add(tableBaseName + "1");
      ts.add(tableBaseName + "2");
      Table tbl1 = createTestTable(dbName, ts.get(0));
      hm.createTable(tbl1);
      Table tbl2 = createTestTable(dbName, ts.get(1));
      hm.createTable(tbl2);
      // test dropping tables and trash behavior
      Table table1 = hm.getTable(dbName, ts.get(0));
      assertNotNull(table1);
      assertEquals(ts.get(0), table1.getTableName());
      Path path1 = table1.getPath();
      FileSystem fs = path1.getFileSystem(hiveConf);
      assertTrue(fs.exists(path1));
      // drop table and check that trash works
      Path trashDir = ShimLoader.getHadoopShims().getCurrentTrashPath(hiveConf, fs);
      assertNotNull("trash directory should not be null", trashDir);
      Path trash1 = mergePaths(trashDir, path1);
      Path pathglob = trash1.suffix("*");;
      FileStatus before[] = fs.globStatus(pathglob);
      hm.dropTable(dbName, ts.get(0));
      assertFalse(fs.exists(path1));
      FileStatus after[] = fs.globStatus(pathglob);
      assertTrue("trash dir before and after DROP TABLE noPURGE are not different",
                 before.length != after.length);

      // drop a table without saving to trash by setting the purge option
      Table table2 = hm.getTable(dbName, ts.get(1));
      assertNotNull(table2);
      assertEquals(ts.get(1), table2.getTableName());
      Path path2 = table2.getPath();
      assertTrue(fs.exists(path2));
      Path trash2 = mergePaths(trashDir, path2);
      System.out.println("trashDir2 is " + trash2);
      pathglob = trash2.suffix("*");
      before = fs.globStatus(pathglob);
      hm.dropTable(dbName, ts.get(1), true, true, true); // deleteData, ignoreUnknownTable, ifPurge
      assertFalse(fs.exists(path2));
      after = fs.globStatus(pathglob);
      Arrays.sort(before);
      Arrays.sort(after);
      assertEquals("trash dir before and after DROP TABLE PURGE are different",
                   before.length, after.length);
      assertTrue("trash dir before and after DROP TABLE PURGE are different",
                 Arrays.equals(before, after));

      // Drop all tables
      for (String tableName : hm.getAllTables(dbName)) {
        Table table = hm.getTable(dbName, tableName);
        hm.dropTable(dbName, tableName);
        assertFalse(fs.exists(table.getPath()));
      }
      hm.dropDatabase(dbName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDropTableTrash() failed");
      throw e;
    }
  }

  private FileStatus[] getTrashContents() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    Path trashDir = ShimLoader.getHadoopShims().getCurrentTrashPath(hiveConf, fs);
    return fs.globStatus(trashDir.suffix("/*"));
  }

  private Table createPartitionedTable(String dbName, String tableName) throws Exception {
    try {

      hm.dropTable(dbName, tableName);
      hm.createTable(tableName,
                     Arrays.asList("key", "value"),   // Data columns.
                     Arrays.asList("ds", "hr"),       // Partition columns.
                     TextInputFormat.class,
                     HiveIgnoreKeyTextOutputFormat.class);
      return hm.getTable(dbName, tableName);
    }
    catch (Exception exception) {
      fail("Unable to drop and create table " + StatsUtils.getFullyQualifiedTableName(dbName, tableName)
          + " because " + StringUtils.stringifyException(exception));
      throw exception;
    }
  }

  private void cleanUpTableQuietly(String dbName, String tableName) {
    try {
      hm.dropTable(dbName, tableName, true, true, true);
    }
    catch(Exception exception) {
      fail("Unexpected exception: " + StringUtils.stringifyException(exception));
    }
  }

  /**
   * Test for PURGE support for dropping partitions.
   * 1. Drop partitions without PURGE, and check that the data isn't moved to Trash.
   * 2. Drop partitions with PURGE, and check that the data is moved to Trash.
   * @throws Exception on failure.
   */
  public void testDropPartitionsWithPurge() throws Exception {
    String dbName = Warehouse.DEFAULT_DATABASE_NAME;
    String tableName = "table_for_testDropPartitionsWithPurge";

    try {

      Map<String, String> partitionSpec =  new ImmutableMap.Builder<String, String>()
                                                 .put("ds", "20141216")
                                                 .put("hr", "12")
                                                 .build();

      int trashSizeBeforeDrop = getTrashContents().length;

      Table table = createPartitionedTable(dbName, tableName);
      hm.createPartition(table, partitionSpec);

      Partition partition = hm.getPartition(table, partitionSpec, false);
      assertNotNull("Newly created partition shouldn't be null!", partition);

      hm.dropPartition(dbName, tableName,
                       partition.getValues(),
                       PartitionDropOptions.instance()
                                           .deleteData(true)
                                           .purgeData(true)
                      );

      int trashSizeAfterDropPurge = getTrashContents().length;

      assertEquals("After dropPartitions(purge), trash should've remained unchanged!",
                 trashSizeBeforeDrop, trashSizeAfterDropPurge);

      // Repeat, and drop partition without purge.
      hm.createPartition(table, partitionSpec);

      partition = hm.getPartition(table, partitionSpec, false);
      assertNotNull("Newly created partition shouldn't be null!", partition);

      hm.dropPartition(dbName, tableName,
                       partition.getValues(),
                       PartitionDropOptions.instance()
                                           .deleteData(true)
                                           .purgeData(false)
                      );

      int trashSizeWithoutPurge = getTrashContents().length;

      assertEquals("After dropPartitions(noPurge), data should've gone to trash!",
                  trashSizeBeforeDrop, trashSizeWithoutPurge);

    }
    catch (Exception e) {
      fail("Unexpected exception: " + StringUtils.stringifyException(e));
    }
    finally {
      cleanUpTableQuietly(dbName, tableName);
    }
  }

  /**
   * Test that tables set up with auto-purge skip trash-directory when tables/partitions are dropped.
   * @throws Throwable
   */
  public void testAutoPurgeTablesAndPartitions() throws Throwable {

    String dbName = Warehouse.DEFAULT_DATABASE_NAME;
    String tableName = "table_for_testAutoPurgeTablesAndPartitions";
    try {

      Table table = createPartitionedTable(dbName, tableName);
      table.getParameters().put("auto.purge", "true");
      hm.alterTable(tableName, table, null);

      Map<String, String> partitionSpec =  new ImmutableMap.Builder<String, String>()
          .put("ds", "20141216")
          .put("hr", "12")
          .build();

      int trashSizeBeforeDrop = getTrashContents().length;

      hm.createPartition(table, partitionSpec);

      Partition partition = hm.getPartition(table, partitionSpec, false);
      assertNotNull("Newly created partition shouldn't be null!", partition);

      hm.dropPartition(dbName, tableName,
          partition.getValues(),
          PartitionDropOptions.instance()
                              .deleteData(true)
                              .purgeData(false)
      );

      int trashSizeAfterDrop = getTrashContents().length;

      assertEquals("After dropPartition(noPurge), data should still have skipped trash.",
                 trashSizeBeforeDrop, trashSizeAfterDrop);

      // Repeat the same check for dropTable.

      trashSizeBeforeDrop = trashSizeAfterDrop;
      hm.dropTable(dbName, tableName);
      trashSizeAfterDrop = getTrashContents().length;

      assertEquals("After dropTable(noPurge), data should still have skipped trash.",
                 trashSizeBeforeDrop, trashSizeAfterDrop);

    }
    catch(Exception e) {
      fail("Unexpected failure: " + StringUtils.stringifyException(e));
    }
    finally {
      cleanUpTableQuietly(dbName, tableName);
    }
  }

  public void testPartition() throws Throwable {
    try {
      String tableName = "table_for_testpartition";
      try {
        hm.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to drop table: " + tableName, false);
      }
      LinkedList<String> cols = new LinkedList<String>();
      cols.add("key");
      cols.add("value");

      LinkedList<String> part_cols = new LinkedList<String>();
      part_cols.add("ds");
      part_cols.add("hr");
      try {
        hm.createTable(tableName, cols, part_cols, TextInputFormat.class,
            HiveIgnoreKeyTextOutputFormat.class);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create table: " + tableName, false);
      }
      Table tbl = null;
      try {
        tbl = hm.getTable(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to fetch table: " + tableName, false);
      }
      HashMap<String, String> part_spec = new HashMap<String, String>();
      part_spec.clear();
      part_spec.put("ds", "2008-04-08");
      part_spec.put("hr", "12");
      try {
        hm.createPartition(tbl, part_spec);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create parition for table: " + tableName, false);
      }
      hm.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed");
      throw e;
    }
  }

  /**
   * Tests creating a simple index on a simple table.
   *
   * @throws Throwable
   */
  public void testIndex() throws Throwable {
    try{
      // create a simple table
      String tableName = "table_for_testindex";
      String qTableName = Warehouse.DEFAULT_DATABASE_NAME + "." + tableName;
      try {
        hm.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to drop table", false);
      }

      Table tbl = new Table(Warehouse.DEFAULT_DATABASE_NAME, tableName);
      List<FieldSchema> fields = tbl.getCols();

      fields.add(new FieldSchema("col1", serdeConstants.INT_TYPE_NAME, "int -- first column"));
      fields.add(new FieldSchema("col2", serdeConstants.STRING_TYPE_NAME,
          "string -- second column"));
      fields.add(new FieldSchema("col3", serdeConstants.DOUBLE_TYPE_NAME,
          "double -- thrift column"));
      tbl.setFields(fields);

      tbl.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
      tbl.setInputFormatClass(SequenceFileInputFormat.class);

      // create table
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to create table: " + tableName, false);
      }

      // Create a simple index
      String indexName = "index_on_table_for_testindex";
      String indexHandlerClass = HiveIndex.IndexType.COMPACT_SUMMARY_TABLE.getHandlerClsName();
      List<String> indexedCols = new ArrayList<String>();
      indexedCols.add("col1");
      String indexTableName = "index_on_table_for_testindex_table";
      String qIndexTableName = Warehouse.DEFAULT_DATABASE_NAME + "." + indexTableName;
      boolean deferredRebuild = true;
      String inputFormat = SequenceFileInputFormat.class.getName();
      String outputFormat = SequenceFileOutputFormat.class.getName();
      String serde = null;
      String storageHandler = null;
      String location = null;
      String collItemDelim = null;
      String fieldDelim = null;
      String fieldEscape = null;
      String lineDelim = null;
      String mapKeyDelim = null;
      String indexComment = null;
      Map<String, String> indexProps = null;
      Map<String, String> tableProps = null;
      Map<String, String> serdeProps = new HashMap<String, String>();
      hm.createIndex(qTableName, indexName, indexHandlerClass, indexedCols, qIndexTableName,
          deferredRebuild, inputFormat, outputFormat, serde, storageHandler, location,
          indexProps, tableProps, serdeProps, collItemDelim, fieldDelim, fieldEscape, lineDelim,
          mapKeyDelim, indexComment);

      // Retrieve and validate the index
      Index index = null;
      try {
        index = hm.getIndex(tableName, indexName);
        assertNotNull("Unable to fetch index", index);
        index.validate();
        assertEquals("Index names don't match for index: " + indexName, indexName,
            index.getIndexName());
        assertEquals("Table names don't match for index: " + indexName, tableName,
            index.getOrigTableName());
        assertEquals("Index table names didn't match for index: " + indexName, indexTableName,
            index.getIndexTableName());
        assertEquals("Index handler classes didn't match for index: " + indexName,
            indexHandlerClass, index.getIndexHandlerClass());
        assertEquals("Deferred rebuild didn't match for index: " + indexName, deferredRebuild,
            index.isDeferredRebuild());

      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to fetch index correctly: " + indexName, false);
      }

      // Drop index
      try {
        hm.dropIndex(Warehouse.DEFAULT_DATABASE_NAME, tableName, indexName, false, true);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to drop index: " + indexName, false);
      }

      boolean dropIndexException = false;
      try {
        hm.getIndex(tableName, indexName);
      } catch (HiveException e) {
        // Expected since it was just dropped
        dropIndexException = true;
      }

      assertTrue("Unable to drop index: " + indexName, dropIndexException);

      // Drop table
      try {
        hm.dropTable(tableName);
        Table droppedTable = hm.getTable(tableName, false);
        assertNull("Unable to drop table " + tableName, droppedTable);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to drop table: " + tableName, false);
      }
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testIndex failed");
      throw e;
    }
  }

  public void testHiveRefreshOnConfChange() throws Throwable{
    Hive prevHiveObj = Hive.get();
    prevHiveObj.getDatabaseCurrent();
    Hive newHiveObj;

    //if HiveConf has not changed, same object should be returned
    HiveConf newHconf = new HiveConf(hiveConf);
    newHiveObj = Hive.get(newHconf);
    assertTrue(prevHiveObj == newHiveObj);

    //if needs refresh param is passed, it should return new object
    newHiveObj = Hive.get(newHconf, true);
    assertTrue(prevHiveObj != newHiveObj);

    //if HiveConf has changed, new object should be returned
    prevHiveObj = Hive.get();
    prevHiveObj.getDatabaseCurrent();
    //change value of a metavar config param in new hive conf
    newHconf = new HiveConf(hiveConf);
    newHconf.setIntVar(ConfVars.METASTORETHRIFTCONNECTIONRETRIES,
        newHconf.getIntVar(ConfVars.METASTORETHRIFTCONNECTIONRETRIES) + 1);
    newHiveObj = Hive.get(newHconf);
    assertTrue(prevHiveObj != newHiveObj);
  }

  // shamelessly copied from Path in hadoop-2
  private static final String SEPARATOR = "/";
  private static final char SEPARATOR_CHAR = '/';

  private static final String CUR_DIR = ".";

  private static final boolean WINDOWS
      = System.getProperty("os.name").startsWith("Windows");

  private static final Pattern hasDriveLetterSpecifier =
      Pattern.compile("^/?[a-zA-Z]:");

  private static Path mergePaths(Path path1, Path path2) {
    String path2Str = path2.toUri().getPath();
    path2Str = path2Str.substring(startPositionWithoutWindowsDrive(path2Str));
    // Add path components explicitly, because simply concatenating two path
    // string is not safe, for example:
    // "/" + "/foo" yields "//foo", which will be parsed as authority in Path
    return new Path(path1.toUri().getScheme(),
        path1.toUri().getAuthority(),
        path1.toUri().getPath() + path2Str);
  }

  private static int startPositionWithoutWindowsDrive(String path) {
    if (hasWindowsDrive(path)) {
      return path.charAt(0) ==  SEPARATOR_CHAR ? 3 : 2;
    } else {
      return 0;
    }
  }

  private static boolean hasWindowsDrive(String path) {
    return (WINDOWS && hasDriveLetterSpecifier.matcher(path).find());
  }
}
