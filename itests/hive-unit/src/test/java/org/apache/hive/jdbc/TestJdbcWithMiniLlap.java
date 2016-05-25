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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapRowRecordReader;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapRowInputFormat;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.AbstractNucleusContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJdbcWithMiniLlap {
  private static MiniHS2 miniHS2 = null;
  private static String dataFileDir;
  private static Path kvDataFilePath;
  private static final String tmpDir = System.getProperty("test.tmp.dir");

  private static HiveConf conf = null;
  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    String confDir = "../../data/conf/llap/";
    if (confDir != null && !confDir.isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
      System.out.println("Setting hive-site: "+HiveConf.getHiveSiteLocation());
    }

    conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
        + "/tez-site.xml"));

    miniHS2 = new MiniHS2(conf, MiniClusterType.LLAP);

    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  private Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    conn.createStatement().execute("set hive.support.concurrency = false");
    return conn;
  }

  @After
  public void tearDown() throws Exception {
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  private void createTestTable(String tableName) throws Exception {
    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

    // load data
    stmt.execute("load data local inpath '"
        + kvDataFilePath.toString() + "' into table " + tableName);

    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

  private int getLlapIFRowCount(String query, int numSplits) throws Exception {

    // Setup LlapInputFormat
    String url = miniHS2.getJdbcURL();
    String user = System.getProperty("user.name");
    String pwd = user;

    LlapRowInputFormat inputFormat = new LlapRowInputFormat();

    // Get splits
    JobConf job = new JobConf(conf);
    job.set(LlapBaseInputFormat.URL_KEY, url);
    job.set(LlapBaseInputFormat.USER_KEY, user);
    job.set(LlapBaseInputFormat.PWD_KEY, pwd);
    job.set(LlapBaseInputFormat.QUERY_KEY, query);

    InputSplit[] splits = inputFormat.getSplits(job, numSplits);
    assertTrue(splits.length > 0);

    // Fetch rows from splits
    boolean first = true;
    int rowCount = 0;
    for (InputSplit split : splits) {
      System.out.println("Processing split " + split.getLocations());

      int numColumns = 2;
      RecordReader<NullWritable, Row> reader = inputFormat.getRecordReader(split, job, null);
      if (reader instanceof LlapRowRecordReader && first) {
        Schema schema = ((LlapRowRecordReader) reader).getSchema();
        System.out.println(""+schema);
        assertEquals(numColumns, schema.getColumns().size());
      }

      if (first) {
        System.out.println("Results: ");
        System.out.println("");
        first = false;
      }

      Row row = reader.createValue();
      while (reader.next(NullWritable.get(), row)) {
        for (int idx = 0; idx < numColumns; idx++) {
          if (idx > 0) {
            System.out.print(", ");
          }
          System.out.print(row.getValue(idx));
        }
        System.out.println("");
        ++rowCount;
      }
    }

    return rowCount;
  }

  @Test(timeout = 60000)
  public void testLlapInputFormatEndToEnd() throws Exception {
    createTestTable("testtab1");

    int rowCount;

    String query = "select * from testtab1 where under_col = 0";
    rowCount = getLlapIFRowCount(query, 1);
    assertEquals(3, rowCount);

    // Try empty rows query
    query = "select * from testtab1 where true = false";
    rowCount = getLlapIFRowCount(query, 1);
    assertEquals(0, rowCount);
  }
}