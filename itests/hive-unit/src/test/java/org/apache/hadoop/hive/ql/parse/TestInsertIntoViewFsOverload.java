package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.ViewDistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.*;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class TestInsertIntoViewFsOverload {
  private static final Logger LOG = LoggerFactory.getLogger(TestInsertIntoViewFsOverload.class);
  private static MiniDFSCluster cluster;
  private static WarehouseInstance hiveWarehouse;
  @Rule
  public final TestName testName = new TestName();
  private static String dbName;
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    String FS_DEFAULT_NAME = "hdfs://localhost:55149/";
    URI defaultURI = URI.create(FS_DEFAULT_NAME);
    conf.set("fs.viewfs.mounttable." + defaultURI.getHost() + ".linkFallback", defaultURI.toString());
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).nameNodePort(55149).format(true).build();
    hiveWarehouse = new WarehouseInstance(LOG, cluster, new HashMap<String, String>() {{
      put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
    }}, cluster.getFileSystem());
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    if (cluster != null) {
      FileSystem.closeAll();
      cluster.shutdown();
    }
    hiveWarehouse.close();
  }

  @Before
  public void setUpDB() throws Throwable {
    dbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    hiveWarehouse.run("create database " + dbName);
  }

  // Tests the functionality of hive insert into HDFS with ViewFs Overload scheme enabled
  @Test
  public void testInsertIntoHDFSViewFsOverload() throws Throwable {
    hiveWarehouse.run("use " + dbName)
        .run("create table t1 (a int)")
        .run("insert into table t1 values (1),(2)")
        .run("select * from t1")
        .verifyResults(new String[] { "1", "2" });
  }
}

