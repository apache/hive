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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.HashMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestExportImport {

  protected static final Logger LOG = LoggerFactory.getLogger(TestExportImport.class);
  private static WarehouseInstance srcHiveWarehouse;
  private static WarehouseInstance destHiveWarehouse;
  private static WarehouseInstance dumpExternalWarehouse;

  @Rule
  public final TestName testName = new TestName();
  private String dbName;
  private String replDbName;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    HashMap<String, String> overridesForHiveConf = new HashMap<String, String>() {{
      put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
    }};
    HashMap<String, String> overridesForHiveConfDump = new HashMap<String, String>() {{
        put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
        put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    }};
    srcHiveWarehouse =
        new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
    destHiveWarehouse = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
    dumpExternalWarehouse =
            new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConfDump);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    srcHiveWarehouse.close();
    destHiveWarehouse.close();
  }

  @Before
  public void setup() throws Throwable {
    dbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replDbName = dbName + "_dupe";
    srcHiveWarehouse.run("create database " + dbName);
    destHiveWarehouse.run("create database " + replDbName);
  }

  @Test
  public void shouldExportImportATemporaryTable() throws Throwable {
    String path = "hdfs:///tmp/" + dbName + "/";
    String exportPath = "'" + path + "'";
    String importDataPath = path + "/data";
    srcHiveWarehouse
        .run("use " + dbName)
        .run("create temporary table t1 (i int)")
        .run("insert into table t1 values (1),(2)")
        .run("export table t1 to " + exportPath)
        .run("create temporary table t2 like t1")
        .run("load data inpath '" + importDataPath + "' overwrite into table t2")
        .run("select * from t2")
        .verifyResults(new String[] { "1", "2" });
  }

  @Test
  public void dataImportAfterMetadataOnlyImport() throws Throwable {
    String path = "hdfs:///tmp/" + dbName + "/";
    String exportMDPath = "'" + path + "1/'";
    String exportDataPath = "'" + path + "2/'";
    srcHiveWarehouse.run("create table " + dbName + ".t1 (i int)")
        .run("insert into table " + dbName + ".t1 values (1),(2)")
        .run("export table " + dbName + ".t1 to " + exportMDPath + " for metadata replication('1')")
        .run("export table " + dbName + ".t1 to " + exportDataPath + " for replication('2')");

    destHiveWarehouse.run("import table " + replDbName + ".t1 from " + exportMDPath)
        .run("import table " + replDbName + ".t1 from " + exportDataPath)
        .run("select * from " + replDbName + ".t1")
        .verifyResults(new String[] { "1", "2" });
  }

  @Test
  public void testExportExternalTableSetFalse() throws Throwable {
    String path = "hdfs:///tmp/" + dbName + "/";
    String exportMDPath = "'" + path + "1/'";
    String exportDataPath = "'" + path + "2/'";
    String exportDataPathRepl = "'" + path + "3/'";
    srcHiveWarehouse.run("create external table " + dbName + ".t1 (i int)")
            .run("insert into table " + dbName + ".t1 values (1),(2)")
            .run("export table " + dbName + ".t1 to " + exportMDPath + " for metadata replication('1')")
            .run("export table " + dbName + ".t1 to " + exportDataPath)
            .runFailure("export table " + dbName + ".t1 to " + exportDataPathRepl + " for replication('2')");

    destHiveWarehouse.run("use " + replDbName)
            .run("import table " + replDbName + ".t1 from " + exportMDPath)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("import table " + replDbName + ".t2 from " + exportDataPath)
            .run("select * from " + replDbName + ".t2")
            .verifyResults(new String[] {"1", "2" })
            .runFailure("import table " + replDbName + ".t3 from " + exportDataPathRepl)
            .run("show tables like 't3'")
            .verifyFailure(new String[] {"t3"});
  }

  @Test
  public void testExportExternalTableSetTrue() throws Throwable {
    String path = "hdfs:///tmp/" + dbName + "/";
    String exportMDPath = "'" + path + "1/'";
    String exportDataPath = "'" + path + "2/'";
    String exportDataPathRepl = "'" + path + "3/'";
    dumpExternalWarehouse.run("create external table " + dbName + ".t1 (i int)")
            .run("insert into table " + dbName + ".t1 values (1),(2)")
            .run("export table " + dbName + ".t1 to " + exportDataPathRepl + " for replication('2')")
            .run("export table " + dbName + ".t1 to " + exportMDPath + " for metadata replication('1')")
            .run("export table " + dbName + ".t1 to " + exportDataPath);

    destHiveWarehouse.run("use " + replDbName)
            .run("import table " + replDbName + ".t1 from " + exportMDPath)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("import table " + replDbName + ".t2 from " + exportDataPath)
            .run("select * from " + replDbName + ".t2")
            .verifyResults(new String[] {"1", "2" })
            .run("import table " + replDbName + ".t3 from " + exportDataPathRepl)
            .run("select * from " + replDbName + ".t3")
            .verifyResults(new String[] {"1", "2" });
  }

  @Test
  public void databaseTheTableIsImportedIntoShouldBeParsedFromCommandLine() throws Throwable {
    String path = "hdfs:///tmp/" + dbName + "/";
    String exportPath = "'" + path + "1/'";

    srcHiveWarehouse.run("create table " + dbName + ".t1 (i int)")
        .run("insert into table " + dbName + ".t1 values (1),(2)")
        .run("export table " + dbName + ".t1 to " + exportPath);

    destHiveWarehouse.run("create database test1")
        .run("use default")
        .run("import table test1.t1 from " + exportPath)
        .run("select * from test1.t1")
        .verifyResults(new String[] { "1", "2" });

  }

  @Test
  public void testExportNonNativeTable() throws Throwable {
    String path = "hdfs:///tmp/" + dbName + "/";
    String exportPath = path + "1/";
    String exportMetaPath = exportPath + "/Meta";
    String tableName =  testName.getMethodName();
    String createTableQuery =
            "CREATE TABLE " + tableName + " ( serde_id bigint COMMENT 'from deserializer', name string "
                    + "COMMENT 'from deserializer', slib string COMMENT 'from deserializer') "
                    + "ROW FORMAT SERDE 'org.apache.hive.storage.jdbc.JdbcSerDe' "
                    + "STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler' "
                    + "WITH SERDEPROPERTIES ('serialization.format'='1') "
                    + "TBLPROPERTIES ( "
                    + "'hive.sql.database.type'='METASTORE', "
                    + "'hive.sql.query'='SELECT \"SERDE_ID\", \"NAME\", \"SLIB\" FROM \"SERDES\"')";

    srcHiveWarehouse.run("use " + dbName)
            .run(createTableQuery)
            .runFailure("export table " + tableName + " to '" + exportPath + "'")
            .run("export table " + tableName + " to '" + exportMetaPath + "'" + " for metadata replication('1')");

    destHiveWarehouse.run("use " + replDbName)
            .runFailure("import table " +  tableName + " from '" + exportPath + "'")
            .run("show tables")
            .verifyFailure(new String[] {tableName})
            .run("import table " + tableName + " from '" + exportMetaPath + "'")
            .run("show tables")
            .verifyResult(tableName);

    // check physical path
    Path checkPath = new Path(exportPath);
    checkPath = new Path(checkPath, EximUtil.DATA_PATH_NAME);
    FileSystem fs = checkPath.getFileSystem(srcHiveWarehouse.hiveConf);
    assertFalse(fs.exists(checkPath));
    checkPath = new Path(exportMetaPath);
    checkPath = new Path(checkPath, EximUtil.METADATA_NAME);
    assertTrue(fs.exists(checkPath));
  }
}
