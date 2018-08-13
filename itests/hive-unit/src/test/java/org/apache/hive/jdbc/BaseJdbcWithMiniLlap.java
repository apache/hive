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

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
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
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.LlapRowRecordReader;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.AbstractNucleusContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Specialize this base class for different serde's/formats
 * {@link #beforeTest(boolean) beforeTest} should be called
 * by sub-classes in a {@link org.junit.BeforeClass} initializer
 */
public abstract class BaseJdbcWithMiniLlap {
  private static MiniHS2 miniHS2 = null;
  private static String dataFileDir;
  private static Path kvDataFilePath;
  private static Path dataTypesFilePath;

  private static HiveConf conf = null;
  private static Connection hs2Conn = null;

  // This method should be called by sub-classes in a @BeforeClass initializer
  public static MiniHS2 beforeTest(boolean useArrow) throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    String confDir = "../../data/conf/llap/";
    if (confDir != null && !confDir.isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
      System.out.println("Setting hive-site: "+HiveConf.getHiveSiteLocation());
    }

    conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    if(useArrow) {
      conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    } else {
      conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, false);
    }

    conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
        + "/tez-site.xml"));

    miniHS2 = new MiniHS2(conf, MiniClusterType.LLAP);

    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
    dataTypesFilePath = new Path(dataFileDir, "datatypes.txt");
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));
    return miniHS2;
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  public static Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    conn.createStatement().execute("set hive.support.concurrency = false");
    return conn;
  }

  @After
  public void tearDown() throws Exception {
    LlapBaseInputFormat.closeAll();
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  private void createTestTable(String tableName) throws Exception {
    createTestTable(hs2Conn, null, tableName, kvDataFilePath.toString());
  }

  public static void createTestTable(Connection connection, String database, String tableName, String srcFile) throws
    Exception {
    Statement stmt = connection.createStatement();

    if (database != null) {
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
      stmt.execute("USE " + database);
    }

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

    // load data
    stmt.execute("load data local inpath '" + srcFile + "' into table " + tableName);

    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

  protected void createDataTypesTable(String tableName) throws Exception {
    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    // tables with various types
    stmt.execute("create table " + tableName
        + " (c1 int, c2 boolean, c3 double, c4 string,"
        + " c5 array<int>, c6 map<int,string>, c7 map<string,string>,"
        + " c8 struct<r:string,s:int,t:double>,"
        + " c9 tinyint, c10 smallint, c11 float, c12 bigint,"
        + " c13 array<array<string>>,"
        + " c14 map<int, map<int,int>>,"
        + " c15 struct<r:int,s:struct<a:int,b:string>>,"
        + " c16 array<struct<m:map<string,string>,n:int>>,"
        + " c17 timestamp, "
        + " c18 decimal(16,7), "
        + " c19 binary, "
        + " c20 date,"
        + " c21 varchar(20),"
        + " c22 char(15),"
        + " c23 binary"
        + ")");
    stmt.execute("load data local inpath '"
        + dataTypesFilePath.toString() + "' into table " + tableName);
    stmt.close();
  }

  @Test(timeout = 60000)
  public void testLlapInputFormatEndToEnd() throws Exception {
    createTestTable("testtab1");

    int rowCount;

    RowCollector rowCollector = new RowCollector();
    String query = "select * from testtab1 where under_col = 0";
    rowCount = processQuery(query, 1, rowCollector);
    assertEquals(3, rowCount);
    assertArrayEquals(new String[] {"0", "val_0"}, rowCollector.rows.get(0));
    assertArrayEquals(new String[] {"0", "val_0"}, rowCollector.rows.get(1));
    assertArrayEquals(new String[] {"0", "val_0"}, rowCollector.rows.get(2));

    // Try empty rows query
    rowCollector.rows.clear();
    query = "select * from testtab1 where true = false";
    rowCount = processQuery(query, 1, rowCollector);
    assertEquals(0, rowCount);
  }

  @Test(timeout = 60000)
  public void testNonAsciiStrings() throws Exception {
    createTestTable("testtab_nonascii");

    RowCollector rowCollector = new RowCollector();
    String nonAscii = "À côté du garçon";
    String query = "select value, '" + nonAscii + "' from testtab_nonascii where under_col=0";
    int rowCount = processQuery(query, 1, rowCollector);
    assertEquals(3, rowCount);

    assertArrayEquals(new String[] {"val_0", nonAscii}, rowCollector.rows.get(0));
    assertArrayEquals(new String[] {"val_0", nonAscii}, rowCollector.rows.get(1));
    assertArrayEquals(new String[] {"val_0", nonAscii}, rowCollector.rows.get(2));
  }

  @Test(timeout = 60000)
  public void testEscapedStrings() throws Exception {
    createTestTable("testtab1");

    RowCollector rowCollector = new RowCollector();
    String expectedVal1 = "'a',\"b\",\\c\\";
    String expectedVal2 = "multi\nline";
    String query = "select value, '\\'a\\',\"b\",\\\\c\\\\', 'multi\\nline' from testtab1 where under_col=0";
    int rowCount = processQuery(query, 1, rowCollector);
    assertEquals(3, rowCount);

    assertArrayEquals(new String[] {"val_0", expectedVal1, expectedVal2}, rowCollector.rows.get(0));
    assertArrayEquals(new String[] {"val_0", expectedVal1, expectedVal2}, rowCollector.rows.get(1));
    assertArrayEquals(new String[] {"val_0", expectedVal1, expectedVal2}, rowCollector.rows.get(2));
  }

  @Test(timeout = 60000)
  public void testDataTypes() throws Exception {
    createDataTypesTable("datatypes");
    RowCollector2 rowCollector = new RowCollector2();
    String query = "select * from datatypes";
    int rowCount = processQuery(query, 1, rowCollector);
    assertEquals(3, rowCount);

    // Verify schema
    String[][] colNameTypes = new String[][] {
        {"datatypes.c1", "int"},
        {"datatypes.c2", "boolean"},
        {"datatypes.c3", "double"},
        {"datatypes.c4", "string"},
        {"datatypes.c5", "array<int>"},
        {"datatypes.c6", "map<int,string>"},
        {"datatypes.c7", "map<string,string>"},
        {"datatypes.c8", "struct<r:string,s:int,t:double>"},
        {"datatypes.c9", "tinyint"},
        {"datatypes.c10", "smallint"},
        {"datatypes.c11", "float"},
        {"datatypes.c12", "bigint"},
        {"datatypes.c13", "array<array<string>>"},
        {"datatypes.c14", "map<int,map<int,int>>"},
        {"datatypes.c15", "struct<r:int,s:struct<a:int,b:string>>"},
        {"datatypes.c16", "array<struct<m:map<string,string>,n:int>>"},
        {"datatypes.c17", "timestamp"},
        {"datatypes.c18", "decimal(16,7)"},
        {"datatypes.c19", "binary"},
        {"datatypes.c20", "date"},
        {"datatypes.c21", "varchar(20)"},
        {"datatypes.c22", "char(15)"},
        {"datatypes.c23", "binary"},
    };
    FieldDesc fieldDesc;
    assertEquals(23, rowCollector.numColumns);
    for (int idx = 0; idx < rowCollector.numColumns; ++idx) {
      fieldDesc = rowCollector.schema.getColumns().get(idx);
      assertEquals("ColName idx=" + idx, colNameTypes[idx][0], fieldDesc.getName());
      assertEquals("ColType idx=" + idx, colNameTypes[idx][1], fieldDesc.getTypeInfo().getTypeName());
    }

    // First row is all nulls
    Object[] rowValues = rowCollector.rows.get(0);
    for (int idx = 0; idx < rowCollector.numColumns; ++idx) {
      assertEquals("idx=" + idx, null, rowValues[idx]);
    }

    // Second Row
    rowValues = rowCollector.rows.get(1);
    assertEquals(Integer.valueOf(-1), rowValues[0]);
    assertEquals(Boolean.FALSE, rowValues[1]);
    assertEquals(Double.valueOf(-1.1d), rowValues[2]);
    assertEquals("", rowValues[3]);

    List<?> c5Value = (List<?>) rowValues[4];
    assertEquals(0, c5Value.size());

    Map<?,?> c6Value = (Map<?,?>) rowValues[5];
    assertEquals(0, c6Value.size());

    Map<?,?> c7Value = (Map<?,?>) rowValues[6];
    assertEquals(0, c7Value.size());

    List<?> c8Value = (List<?>) rowValues[7];
    assertEquals(null, c8Value.get(0));
    assertEquals(null, c8Value.get(1));
    assertEquals(null, c8Value.get(2));

    assertEquals(Byte.valueOf((byte) -1), rowValues[8]);
    assertEquals(Short.valueOf((short) -1), rowValues[9]);
    assertEquals(Float.valueOf(-1.0f), rowValues[10]);
    assertEquals(Long.valueOf(-1l), rowValues[11]);

    List<?> c13Value = (List<?>) rowValues[12];
    assertEquals(0, c13Value.size());

    Map<?,?> c14Value = (Map<?,?>) rowValues[13];
    assertEquals(0, c14Value.size());

    List<?> c15Value = (List<?>) rowValues[14];
    assertEquals(null, c15Value.get(0));
    assertEquals(null, c15Value.get(1));

    List<?> c16Value = (List<?>) rowValues[15];
    assertEquals(0, c16Value.size());

    assertEquals(null, rowValues[16]);
    assertEquals(null, rowValues[17]);
    assertEquals(null, rowValues[18]);
    assertEquals(null, rowValues[19]);
    assertEquals(null, rowValues[20]);
    assertEquals(null, rowValues[21]);
    assertEquals(null, rowValues[22]);

    // Third row
    rowValues = rowCollector.rows.get(2);
    assertEquals(Integer.valueOf(1), rowValues[0]);
    assertEquals(Boolean.TRUE, rowValues[1]);
    assertEquals(Double.valueOf(1.1d), rowValues[2]);
    assertEquals("1", rowValues[3]);

    c5Value = (List<?>) rowValues[4];
    assertEquals(2, c5Value.size());
    assertEquals(Integer.valueOf(1), c5Value.get(0));
    assertEquals(Integer.valueOf(2), c5Value.get(1));

    c6Value = (Map<?,?>) rowValues[5];
    assertEquals(2, c6Value.size());
    assertEquals("x", c6Value.get(Integer.valueOf(1)));
    assertEquals("y", c6Value.get(Integer.valueOf(2)));

    c7Value = (Map<?,?>) rowValues[6];
    assertEquals(1, c7Value.size());
    assertEquals("v", c7Value.get("k"));

    c8Value = (List<?>) rowValues[7];
    assertEquals("a", c8Value.get(0));
    assertEquals(Integer.valueOf(9), c8Value.get(1));
    assertEquals(Double.valueOf(2.2d), c8Value.get(2));

    assertEquals(Byte.valueOf((byte) 1), rowValues[8]);
    assertEquals(Short.valueOf((short) 1), rowValues[9]);
    assertEquals(Float.valueOf(1.0f), rowValues[10]);
    assertEquals(Long.valueOf(1l), rowValues[11]);

    c13Value = (List<?>) rowValues[12];
    assertEquals(2, c13Value.size());
    List<?> listVal = (List<?>) c13Value.get(0);
    assertEquals("a", listVal.get(0));
    assertEquals("b", listVal.get(1));
    listVal = (List<?>) c13Value.get(1);
    assertEquals("c", listVal.get(0));
    assertEquals("d", listVal.get(1));

    c14Value = (Map<?,?>) rowValues[13];
    assertEquals(2, c14Value.size());
    Map<?,?> mapVal = (Map<?,?>) c14Value.get(Integer.valueOf(1));
    assertEquals(2, mapVal.size());
    assertEquals(Integer.valueOf(12), mapVal.get(Integer.valueOf(11)));
    assertEquals(Integer.valueOf(14), mapVal.get(Integer.valueOf(13)));
    mapVal = (Map<?,?>) c14Value.get(Integer.valueOf(2));
    assertEquals(1, mapVal.size());
    assertEquals(Integer.valueOf(22), mapVal.get(Integer.valueOf(21)));

    c15Value = (List<?>) rowValues[14];
    assertEquals(Integer.valueOf(1), c15Value.get(0));
    listVal = (List<?>) c15Value.get(1);
    assertEquals(2, listVal.size());
    assertEquals(Integer.valueOf(2), listVal.get(0));
    assertEquals("x", listVal.get(1));

    c16Value = (List<?>) rowValues[15];
    assertEquals(2, c16Value.size());
    listVal = (List<?>) c16Value.get(0);
    assertEquals(2, listVal.size());
    mapVal = (Map<?,?>) listVal.get(0);
    assertEquals(0, mapVal.size());
    assertEquals(Integer.valueOf(1), listVal.get(1));
    listVal = (List<?>) c16Value.get(1);
    mapVal = (Map<?,?>) listVal.get(0);
    assertEquals(2, mapVal.size());
    assertEquals("b", mapVal.get("a"));
    assertEquals("d", mapVal.get("c"));
    assertEquals(Integer.valueOf(2), listVal.get(1));

    assertEquals(Timestamp.valueOf("2012-04-22 09:00:00.123456789"), rowValues[16]);
    assertEquals(new BigDecimal("123456789.123456"), rowValues[17]);
    assertArrayEquals("abcd".getBytes("UTF-8"), (byte[]) rowValues[18]);
    assertEquals(Date.valueOf("2013-01-01"), rowValues[19]);
    assertEquals("abc123", rowValues[20]);
    assertEquals("abc123         ", rowValues[21]);
    assertArrayEquals("X'01FF'".getBytes("UTF-8"), (byte[]) rowValues[22]);
  }


  @Test(timeout = 60000)
  public void testComplexQuery() throws Exception {
    createTestTable("testtab1");

    RowCollector rowCollector = new RowCollector();
    String query = "select value, count(*) from testtab1 where under_col=0 group by value";
    int rowCount = processQuery(query, 1, rowCollector);
    assertEquals(1, rowCount);

    assertArrayEquals(new String[] {"val_0", "3"}, rowCollector.rows.get(0));
  }

  private interface RowProcessor {
    void process(Row row);
  }

  protected static class RowCollector implements RowProcessor {
    ArrayList<String[]> rows = new ArrayList<String[]>();
    Schema schema = null;
    int numColumns = 0;

    public void process(Row row) {
      if (schema == null) {
        schema = row.getSchema();
        numColumns = schema.getColumns().size();
      }

      String[] arr = new String[numColumns];
      for (int idx = 0; idx < numColumns; ++idx) {
        Object val = row.getValue(idx);
        arr[idx] = (val == null ? null : val.toString());
      }
      rows.add(arr);
    }
  }

  // Save the actual values from each row as opposed to the String representation.
  protected static class RowCollector2 implements RowProcessor {
    ArrayList<Object[]> rows = new ArrayList<Object[]>();
    Schema schema = null;
    int numColumns = 0;

    public void process(Row row) {
      if (schema == null) {
        schema = row.getSchema();
        numColumns = schema.getColumns().size();
      }

      Object[] arr = new Object[numColumns];
      for (int idx = 0; idx < numColumns; ++idx) {
        arr[idx] = row.getValue(idx);
      }
      rows.add(arr);
    }
  }

  protected int processQuery(String query, int numSplits, RowProcessor rowProcessor) throws Exception {
    return processQuery(null, query, numSplits, rowProcessor);
  }

  protected abstract InputFormat<NullWritable, Row> getInputFormat();

  private int processQuery(String currentDatabase, String query, int numSplits, RowProcessor rowProcessor) throws Exception {
    String url = miniHS2.getJdbcURL();
    String user = System.getProperty("user.name");
    String pwd = user;
    String handleId = UUID.randomUUID().toString();

    InputFormat<NullWritable, Row> inputFormat = getInputFormat();

    // Get splits
    JobConf job = new JobConf(conf);
    job.set(LlapBaseInputFormat.URL_KEY, url);
    job.set(LlapBaseInputFormat.USER_KEY, user);
    job.set(LlapBaseInputFormat.PWD_KEY, pwd);
    job.set(LlapBaseInputFormat.QUERY_KEY, query);
    job.set(LlapBaseInputFormat.HANDLE_ID, handleId);
    if (currentDatabase != null) {
      job.set(LlapBaseInputFormat.DB_KEY, currentDatabase);
    }

    InputSplit[] splits = inputFormat.getSplits(job, numSplits);
    assertTrue(splits.length > 0);

    // Fetch rows from splits
    boolean first = true;
    int rowCount = 0;
    for (InputSplit split : splits) {
      System.out.println("Processing split " + split.getLocations());

      int numColumns = 2;
      RecordReader<NullWritable, Row> reader = inputFormat.getRecordReader(split, job, null);
      Row row = reader.createValue();
      while (reader.next(NullWritable.get(), row)) {
        rowProcessor.process(row);
        ++rowCount;
      }
      reader.close();
    }
    LlapBaseInputFormat.close(handleId);

    return rowCount;
  }

  /**
   * Test CLI kill command of a query that is running.
   * We spawn 2 threads - one running the query and
   * the other attempting to cancel.
   * We're using a dummy udf to simulate a query,
   * that runs for a sufficiently long time.
   * @throws Exception
   */
  @Test
  public void testKillQuery() throws Exception {
    String tableName = "testtab1";
    createTestTable(tableName);
    Connection con = hs2Conn;
    Connection con2 = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");

    String udfName = TestJdbcWithMiniHS2.SleepMsUDF.class.getName();
    Statement stmt1 = con.createStatement();
    Statement stmt2 = con2.createStatement();
    stmt1.execute("create temporary function sleepMsUDF as '" + udfName + "'");
    stmt1.close();
    final Statement stmt = con.createStatement();

    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();

    // Thread executing the query
    Thread tExecute = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("Executing query: ");
          // The test table has 500 rows, so total query time should be ~ 500*500ms
          stmt.executeQuery("select sleepMsUDF(t1.under_col, 100), t1.under_col, t2.under_col " +
              "from " + tableName + " t1 join " + tableName + " t2 on t1.under_col = t2.under_col");
          fail("Expecting SQLException");
        } catch (SQLException e) {
          tExecuteHolder.throwable = e;
        }
      }
    });
    // Thread killing the query
    Thread tKill = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
          String queryId = ((HiveStatement) stmt).getQueryId();
          System.out.println("Killing query: " + queryId);

          stmt2.execute("kill query '" + queryId + "'");
          stmt2.close();
        } catch (Exception e) {
          tKillHolder.throwable = e;
        }
      }
    });

    tExecute.start();
    tKill.start();
    tExecute.join();
    tKill.join();
    stmt.close();
    con2.close();

    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertNull("tCancel", tKillHolder.throwable);
  }

  private static class ExceptionHolder {
    Throwable throwable;
  }
}

