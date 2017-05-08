/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * Tests DDL with remote metastore service and second namenode (HIVE-6374)
 *
 */
public class TestDDLWithRemoteMetastoreSecondNamenode extends TestCase {
  static HiveConf conf;

  private static final String Database1Name = "db1_nondefault_nn";
  private static final String Database2Name = "db2_nondefault_nn";
  private static final String Table1Name = "table1_nondefault_nn";
  private static final String Table2Name = "table2_nondefault_nn";
  private static final String Table3Name = "table3_nondefault_nn";
  private static final String Table4Name = "table4_nondefault_nn";
  private static final String Table5Name = "table5_nondefault_nn";
  private static final String Table6Name = "table6_nondefault_nn";
  private static final String Index1Name = "index1_table1_nondefault_nn";
  private static final String Index2Name = "index2_table1_nondefault_nn";
  private static final String tmpdir = System.getProperty("test.tmp.dir");
  private static final String tmpdirFs2 = "/" + TestDDLWithRemoteMetastoreSecondNamenode.class.getName();
  private static final Path tmppath = new Path(tmpdir);
  private static final Path tmppathFs2 = new Path(tmpdirFs2);
  private static String fs2Uri;
  private static MiniDFSCluster miniDfs = null;
  private static Hive db;
  private static FileSystem fs, fs2;
  private static HiveConf jobConf;
  private static Driver driver;
  private static int tests = 0;
  private static Boolean isInitialized = false;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    if (tests > 0) {
      return;
    }
    tests = new JUnit4TestAdapter(this.getClass()).countTestCases();
    try {
      conf = new HiveConf(ExecDriver.class);
      SessionState.start(conf);

      // Test with remote metastore service
      int port = MetaStoreUtils.findFreePort();
      MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
      conf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
      conf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
      conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, new URI(tmppath + "/warehouse").getPath());

      // Initialize second mocked filesystem (implement only necessary stuff)
      // Physical files are resides in local file system in the similar location
      jobConf = new HiveConf(conf);
      miniDfs = new MiniDFSCluster(new Configuration(), 1, true, null);
      fs2 = miniDfs.getFileSystem();
      try {
        fs2.delete(tmppathFs2, true);
      }
      catch (IOException e) {
      }
      fs2.mkdirs(tmppathFs2);
      fs2Uri = fs2.getUri().toString();
      jobConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fs2Uri);

      driver = new Driver(jobConf);

      fs = FileSystem.get(conf);
      if (fs.exists(tmppath) && !fs.getFileStatus(tmppath).isDir()) {
        throw new RuntimeException(tmpdir + " exists but is not a directory");
      }

      if (!fs.exists(tmppath)) {
        if (!fs.mkdirs(tmppath)) {
          throw new RuntimeException("Could not make scratch directory "
              + tmpdir);
        }
      }

      db = Hive.get(conf);
      cleanup();
      isInitialized = true;
    } catch (Exception e) {
      throw new RuntimeException("Encountered exception " + e.getMessage()
              + (e.getCause() == null ? "" : ", caused by: " + e.getCause().getMessage()), e);
    }
    finally {
      if (!isInitialized) {
        shutdownMiniDfs();
      }
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if (--tests == 0) {
      cleanup();
      shutdownMiniDfs();
    }
  }

  private void shutdownMiniDfs() {
      if(miniDfs != null) {
        miniDfs.shutdown();
      }
  }

  private void cleanup() throws CommandNeedRetryException {
      String[] srcidx = {Index1Name, Index2Name};
      for (String src : srcidx) {
        driver.run("DROP INDEX IF EXISTS " + src + " ON " + Table1Name);
      }
      String[] srctables = {Table1Name, Table2Name, Database1Name + "." + Table3Name,
        Database1Name + "." + Table4Name, Table5Name, Table6Name};
      for (String src : srctables) {
        driver.run("DROP TABLE IF EXISTS " + src);
      }
      String[] srcdatabases = {Database1Name, Database2Name};
      for (String src : srcdatabases) {
        driver.run("DROP DATABASE IF EXISTS " + src + " CASCADE");
      }
  }

  private void executeQuery(String query) throws CommandNeedRetryException {
    CommandProcessorResponse result =  driver.run(query);
    assertNotNull("driver.run() was expected to return result for query: " + query, result);
    assertEquals("Execution of (" + query + ") failed with exit status: "
          + result.getResponseCode() + ", " + result.getErrorMessage()
          + ", query: " + query,
          result.getResponseCode(), 0);
  }

  private String buildLocationClause(String location) {
    return location == null ? "" : (" LOCATION '" + location + "'");
  }

  private void addPartitionAndCheck(Table table, String column,
          String value, String location) throws CommandNeedRetryException, HiveException {
    executeQuery("ALTER TABLE " + table.getTableName() +
            " ADD PARTITION (" + column + "='" + value + "')" +
            buildLocationClause(location));
    HashMap<String, String> partitionDef1 = new HashMap<String, String>();
    partitionDef1.put(column, value);
    Partition partition = db.getPartition(table, partitionDef1, false);
    assertNotNull("Partition object is expected for " + Table1Name , partition);
    String locationActual = partition.getLocation();
    if (location == null) {
      assertEquals("Partition should be located in the second filesystem",
              fs2.makeQualified(new Path(table.getTTable().getSd().getLocation())).toString()
                    + "/p=p1", locationActual);
    }
    else if (new Path(location).toUri().getScheme()!= null) {
      assertEquals("Partition should be located in the first filesystem",
              fs.makeQualified(new Path(location)).toString(), locationActual);
    }
    else {
      assertEquals("Partition should be located in the second filesystem",
              fs2.makeQualified(new Path(location)).toString(), locationActual);
    }
  }

  private Table createTableAndCheck(String tableName, String tableLocation)
          throws CommandNeedRetryException, HiveException, URISyntaxException {
    return createTableAndCheck(null, tableName, tableLocation);
  }

  private Table createTableAndCheck(Table baseTable, String tableName, String tableLocation)
          throws CommandNeedRetryException, HiveException, URISyntaxException {
    executeQuery("CREATE TABLE " + tableName + (baseTable == null ?
            " (col1 string, col2 string) PARTITIONED BY (p string) " :
            " LIKE " + baseTable.getTableName())
            + buildLocationClause(tableLocation));
    Table table = db.getTable(tableName);
    assertNotNull("Table object is expected for " + tableName , table);
    String location = table.getTTable().getSd().getLocation();
    if (tableLocation != null) {
      assertEquals("Table should be located in the second filesystem",
              fs2.makeQualified(new Path(tableLocation)).toString(), location);
    }
    else {
      // Since warehouse path is non-qualified the table should be located on second filesystem
      assertEquals("Table should be located in the second filesystem",
              fs2.getUri().getScheme(), new URI(location).getScheme());
    }
    return table;
  }

  private void createIndexAndCheck(Table table, String indexName, String indexLocation)
          throws CommandNeedRetryException, HiveException, URISyntaxException {
    executeQuery("CREATE INDEX " + indexName + " ON TABLE " + table.getTableName()
            + " (col1) AS 'COMPACT' WITH DEFERRED REBUILD "
            + buildLocationClause(indexLocation));
    Index index = db.getIndex(table.getTableName(), indexName);
    assertNotNull("Index object is expected for " + indexName , index);
    String location = index.getSd().getLocation();
    if (indexLocation != null) {
      assertEquals("Index should be located in the second filesystem",
              fs2.makeQualified(new Path(indexLocation)).toString(), location);
    }
    else {
      // Since warehouse path is non-qualified the index should be located on second filesystem
      assertEquals("Index should be located in the second filesystem",
              fs2.getUri().getScheme(), new URI(location).getScheme());
    }
  }

  private void createDatabaseAndCheck(String databaseName, String databaseLocation)
          throws CommandNeedRetryException, HiveException, URISyntaxException {
    executeQuery("CREATE DATABASE " + databaseName + buildLocationClause(databaseLocation));
    Database database = db.getDatabase(databaseName);
    assertNotNull("Database object is expected for " + databaseName , database);
    String location = database.getLocationUri().toString();
    if (databaseLocation != null) {
      assertEquals("Database should be located in the second filesystem",
              fs2.makeQualified(new Path(databaseLocation)).toString(), location);
    }
    else {
      // Since warehouse path is non-qualified the database should be located on second filesystem
      assertEquals("Database should be located in the second filesystem",
              fs2.getUri().getScheme(), new URI(location).getScheme());
    }
  }

  public void testCreateTableWithIndexAndPartitionsNonDefaultNameNode() throws Exception {
    assertTrue("Test suite should be initialied", isInitialized );
    final String tableLocation = tmppathFs2 + "/" + Table1Name;
    final String table5Location = tmppathFs2 + "/" + Table5Name;
    final String indexLocation = tmppathFs2 + "/" + Index1Name;
    final String partition3Location = fs.makeQualified(new Path(tmppath + "/p3")).toString();

    // Create table with absolute non-qualified path
    Table table1 = createTableAndCheck(Table1Name, tableLocation);

    // Create table without location
    createTableAndCheck(Table2Name, null);

    // Add partition without location
    addPartitionAndCheck(table1, "p", "p1", null);

    // Add partition with absolute location
    addPartitionAndCheck(table1, "p", "p2", tableLocation + "/p2");

    // Add partition with qualified location in default fs
    addPartitionAndCheck(table1, "p", "p3", partition3Location);

    // Create index with absolute non-qualified path
    createIndexAndCheck(table1, Index1Name, indexLocation);

    // Create index with absolute non-qualified path
    createIndexAndCheck(table1, Index2Name, null);

    // Create table like Table1Name absolute non-qualified path
    createTableAndCheck(table1, Table5Name, table5Location);

    // Create table without location
    createTableAndCheck(table1, Table6Name, null);
  }

  public void testCreateDatabaseWithTableNonDefaultNameNode() throws Exception {
    assertTrue("Test suite should be initialied", isInitialized );
    final String tableLocation = tmppathFs2 + "/" + Table3Name;
    final String databaseLocation = tmppathFs2 + "/" + Database1Name;

    // Create database in specific location (absolute non-qualified path)
    createDatabaseAndCheck(Database1Name, databaseLocation);

    // Create database without location clause
    createDatabaseAndCheck(Database2Name, null);

    // Create table in database in specific location
    createTableAndCheck(Database1Name + "." + Table3Name, tableLocation);

    // Create table in database without location clause
    createTableAndCheck(Database1Name + "." + Table4Name, null);
  }
}
