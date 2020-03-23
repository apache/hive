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

import java.math.BigDecimal;

import com.google.common.collect.Iterables;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import org.junit.AfterClass;
import org.junit.Test;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.hive.llap.LlapArrowRowInputFormat;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import static org.junit.Assert.*;

/**
 * TestJdbcWithMiniLlap for Arrow format
 */
public class TestJdbcWithMiniLlapArrow extends BaseJdbcWithMiniLlap {

  private static MiniHS2 miniHS2 = null;
  private static final String tableName = "testJdbcMinihs2Tbl";
  private static String dataFileDir;
  private static final String testDbName = "testJdbcMinihs2";

  private static class ExceptionHolder {
    Throwable throwable;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    MiniHS2.cleanupLocalDir();
    miniHS2 = BaseJdbcWithMiniLlap.beforeTest(conf);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    Connection conDefault = BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(),
            System.getProperty("user.name"), "bar");
    Statement stmt = conDefault.createStatement();
    stmt.execute("drop database if exists " + testDbName + " cascade");
    stmt.execute("create database " + testDbName);
    stmt.close();
    conDefault.close();
  }

  @AfterClass
  public static void afterTest() {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Override
  protected InputFormat<NullWritable, Row> getInputFormat() {
    //For unit testing, no harm in hard-coding allocator ceiling to LONG.MAX_VALUE
    return new LlapArrowRowInputFormat(Long.MAX_VALUE);
  }

  // Currently MAP type is not supported. Add it back when Arrow 1.0 is released.
  // See: SPARK-21187
  @Override
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

    //Map<?,?> c6Value = (Map<?,?>) rowValues[5];
    //assertEquals(0, c6Value.size());

    //Map<?,?> c7Value = (Map<?,?>) rowValues[6];
    //assertEquals(0, c7Value.size());

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

    //Map<?,?> c14Value = (Map<?,?>) rowValues[13];
    //assertEquals(0, c14Value.size());

    List<?> c15Value = (List<?>) rowValues[14];
    assertEquals(null, c15Value.get(0));
    assertEquals(null, c15Value.get(1));

    //List<?> c16Value = (List<?>) rowValues[15];
    //assertEquals(0, c16Value.size());

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

    //c6Value = (Map<?,?>) rowValues[5];
    //assertEquals(2, c6Value.size());
    //assertEquals("x", c6Value.get(Integer.valueOf(1)));
    //assertEquals("y", c6Value.get(Integer.valueOf(2)));

    //c7Value = (Map<?,?>) rowValues[6];
    //assertEquals(1, c7Value.size());
    //assertEquals("v", c7Value.get("k"));

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

    //c14Value = (Map<?,?>) rowValues[13];
    //assertEquals(2, c14Value.size());
    //Map<?,?> mapVal = (Map<?,?>) c14Value.get(Integer.valueOf(1));
    //assertEquals(2, mapVal.size());
    //assertEquals(Integer.valueOf(12), mapVal.get(Integer.valueOf(11)));
    //assertEquals(Integer.valueOf(14), mapVal.get(Integer.valueOf(13)));
    //mapVal = (Map<?,?>) c14Value.get(Integer.valueOf(2));
    //assertEquals(1, mapVal.size());
    //assertEquals(Integer.valueOf(22), mapVal.get(Integer.valueOf(21)));

    c15Value = (List<?>) rowValues[14];
    assertEquals(Integer.valueOf(1), c15Value.get(0));
    listVal = (List<?>) c15Value.get(1);
    assertEquals(2, listVal.size());
    assertEquals(Integer.valueOf(2), listVal.get(0));
    assertEquals("x", listVal.get(1));

    //c16Value = (List<?>) rowValues[15];
    //assertEquals(2, c16Value.size());
    //listVal = (List<?>) c16Value.get(0);
    //assertEquals(2, listVal.size());
    //mapVal = (Map<?,?>) listVal.get(0);
    //assertEquals(0, mapVal.size());
    //assertEquals(Integer.valueOf(1), listVal.get(1));
    //listVal = (List<?>) c16Value.get(1);
    //mapVal = (Map<?,?>) listVal.get(0);
    //assertEquals(2, mapVal.size());
    //assertEquals("b", mapVal.get("a"));
    //assertEquals("d", mapVal.get("c"));
    //assertEquals(Integer.valueOf(2), listVal.get(1));

    assertEquals(Timestamp.valueOf("2012-04-22 09:00:00.123456"), rowValues[16]);
    assertEquals(new BigDecimal("123456789.123456"), rowValues[17]);
    assertArrayEquals("abcd".getBytes("UTF-8"), (byte[]) rowValues[18]);
    assertEquals(Date.valueOf("2013-01-01"), rowValues[19]);
    assertEquals("abc123", rowValues[20]);
    assertEquals("abc123         ", rowValues[21]);
    assertArrayEquals("X'01FF'".getBytes("UTF-8"), (byte[]) rowValues[22]);
  }

  /**
   * SleepMsUDF
   */
  public static class SleepMsUDF extends UDF {
    public Integer evaluate(int value, int ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        // No-op
      }
      return value;
    }
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
    Connection con = BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(testDbName),
            System.getProperty("user.name"), "bar");
    Connection con2 = BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(testDbName),
            System.getProperty("user.name"), "bar");

    String udfName = SleepMsUDF.class.getName();
    Statement stmt1 = con.createStatement();
    final Statement stmt2 = con2.createStatement();
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");

    String tblName = testDbName + "." + tableName;

    stmt1.execute("create temporary function sleepMsUDF as '" + udfName + "'");
    stmt1.execute("create table " + tblName + " (int_col int, value string) ");
    stmt1.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tblName);


    stmt1.close();
    final Statement stmt = con.createStatement();
    final ExceptionHolder tExecuteHolder = new ExceptionHolder();
    final ExceptionHolder tKillHolder = new ExceptionHolder();

    // Thread executing the query
    Thread tExecute = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("Executing query: ");
          stmt.execute("set hive.llap.execution.mode = none");

          // The test table has 500 rows, so total query time should be ~ 500*500ms
          stmt.executeQuery("select sleepMsUDF(t1.int_col, 100), t1.int_col, t2.int_col " +
                  "from " + tableName + " t1 join " + tableName + " t2 on t1.int_col = t2.int_col");
        } catch (SQLException e) {
          tExecuteHolder.throwable = e;
        }
      }
    });

    tExecute.start();

    // wait for other thread to create the stmt handle
    int count = 0;
    boolean queryIdFound = false;
    while (count++ < 10) {
      try {
        Thread.sleep(2000);
        String queryId = ((HiveStatement) stmt).getQueryId();
        if (queryId != null) {
          queryIdFound = true;
          System.out.println("Killing query: " + queryId);
          stmt2.execute("kill query '" + queryId + "'");
          stmt2.close();
          break;
        }
      } catch (SQLException e) {
        System.err.println("Error while killing query: " + e);
      }
    }
    assertTrue(queryIdFound);
    
    tExecute.join();
    stmt.close();
    con2.close();
    con.close();

    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertNull("tCancel", tKillHolder.throwable);
  }

  @Test
  public void testConcurrentAddAndCloseAndCloseAllConnections() throws Exception {
    createTestTable("testtab1");

    String url = miniHS2.getJdbcURL();
    String user = System.getProperty("user.name");
    String pwd = user;

    InputFormat<NullWritable, Row> inputFormat = getInputFormat();

    // Get splits
    JobConf job = new JobConf(conf);
    job.set(LlapBaseInputFormat.URL_KEY, url);
    job.set(LlapBaseInputFormat.USER_KEY, user);
    job.set(LlapBaseInputFormat.PWD_KEY, pwd);
    job.set(LlapBaseInputFormat.QUERY_KEY, "select * from testtab1");

    final String[] handleIds = IntStream.range(0, 20).boxed().map(i -> "handleId-" + i).toArray(String[]::new);

    final ExceptionHolder exceptionHolder = new ExceptionHolder();

    // addConnThread thread will keep adding connections
    // closeConnThread thread tries close connection(s) associated to handleIds, one at a time
    // closeAllConnThread thread tries to close All at once.

    final int numIterations = 100;
    final Iterator<String> addConnIterator = Iterables.cycle(handleIds).iterator();
    Thread addConnThread = new Thread(() -> executeNTimes(() -> {
      String handleId = addConnIterator.next();
      job.set(LlapBaseInputFormat.HANDLE_ID, handleId);
      InputSplit[] splits = inputFormat.getSplits(job, 1);
      assertTrue(splits.length > 0);
      return null;
    }, numIterations, 1, exceptionHolder));

    final Iterator<String> removeConnIterator = Iterables.cycle(handleIds).iterator();
    Thread closeConnThread = new Thread(() -> executeNTimes(() -> {
      String handleId = removeConnIterator.next();
      LlapBaseInputFormat.close(handleId);
      return null;
    }, numIterations, 2, exceptionHolder));

    Thread closeAllConnThread = new Thread(() -> executeNTimes(() -> {
      LlapBaseInputFormat.closeAll();
      return null;
    }, numIterations, 5, exceptionHolder));

    addConnThread.start();
    closeConnThread.start();
    closeAllConnThread.start();

    closeAllConnThread.join();
    closeConnThread.join();
    addConnThread.join();

    Throwable throwable = exceptionHolder.throwable;
    assertNull("Something went wrong while testAddCloseCloseAllConnections" + throwable, throwable);

  }

  private void executeNTimes(Callable action, int noOfTimes, long intervalMillis, ExceptionHolder exceptionHolder) {
    for (int i = 0; i < noOfTimes; i++) {
      try {
        action.call();
        Thread.sleep(intervalMillis);
      } catch (Exception e) {
        // populate first exception only
        if (exceptionHolder.throwable == null) {
          synchronized (this) {
            if (exceptionHolder.throwable == null) {
              exceptionHolder.throwable = e;
            }
          }
        }
      }
    }
  }

  @Ignore("CDPD-9330 Ignore TestJdbcWithMiniLlapArrow.testComplexQuery|testLlapInputFormatEndToEnd as they are flaky")
  @Test
  public void testComplexQuery() throws Exception {
    super.testComplexQuery();
  }

  @Ignore("CDPD-9330 Ignore TestJdbcWithMiniLlapArrow.testComplexQuery|testLlapInputFormatEndToEnd as they are flaky")
  @Test
  public void testLlapInputFormatEndToEnd() throws Exception {
    super.testLlapInputFormatEndToEnd();
  }
}

