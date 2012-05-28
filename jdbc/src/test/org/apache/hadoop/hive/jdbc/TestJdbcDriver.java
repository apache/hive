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

package org.apache.hadoop.hive.jdbc;

import static org.apache.hadoop.hive.ql.exec.ExplainTask.EXPL_COLUMN_NAME;
import static org.apache.hadoop.hive.ql.processors.SetProcessor.SET_COLUMN_NAME;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * TestJdbcDriver.
 *
 */
public class TestJdbcDriver extends TestCase {
  private static final String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static final String tableName = "testHiveJdbcDriver_Table";
  private static final String tableComment = "Simple table";
  private static final String viewName = "testHiveJdbcDriverView";
  private static final String viewComment = "Simple view";
  private static final String partitionedTableName = "testHiveJdbcDriverPartitionedTable";
  private static final String partitionedColumnName = "partcolabc";
  private static final String partitionedColumnValue = "20090619";
  private static final String partitionedTableComment = "Partitioned table";
  private static final String dataTypeTableName = "testDataTypeTable";
  private static final String dataTypeTableComment = "Table with many column data types";
  private final HiveConf conf;
  private final Path dataFilePath;
  private final Path dataTypeDataFilePath;
  private Connection con;
  private boolean standAloneServer = false;

  public TestJdbcDriver(String name) {
    super(name);
    conf = new HiveConf(TestJdbcDriver.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    dataTypeDataFilePath = new Path(dataFileDir, "datatypes.txt");
    standAloneServer = "true".equals(System
        .getProperty("test.service.standalone.server"));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Class.forName(driverName);
    if (standAloneServer) {
      // get connection
      con = DriverManager.getConnection("jdbc:hive://localhost:10000/default",
          "", "");
    } else {
      con = DriverManager.getConnection("jdbc:hive://", "", "");
    }
    assertNotNull("Connection is null", con);
    assertFalse("Connection should not be closed", con.isClosed());
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    stmt.executeQuery("set hive.support.concurrency = false");

    // drop table. ignore error.
    try {
      stmt.executeQuery("drop table " + tableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    // create table
    ResultSet res = stmt.executeQuery("create table " + tableName
        + " (under_col int comment 'the under column', value string) comment '"
        + tableComment + "'");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);
    assertFalse(res.next());

    // also initialize a paritioned table to test against.

    // drop table. ignore error.
    try {
      stmt.executeQuery("drop table " + partitionedTableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    res = stmt.executeQuery("create table " + partitionedTableName
        + " (under_col int, value string) comment '"+partitionedTableComment
            +"' partitioned by (" + partitionedColumnName + " STRING)");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '"
        + dataFilePath.toString() + "' into table " + partitionedTableName
        + " PARTITION (" + partitionedColumnName + "="
        + partitionedColumnValue + ")");
    assertFalse(res.next());

    // drop table. ignore error.
    try {
      stmt.executeQuery("drop table " + dataTypeTableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    res = stmt.executeQuery("create table " + dataTypeTableName
        + " (c1 int, c2 boolean, c3 double, c4 string,"
        + " c5 array<int>, c6 map<int,string>, c7 map<string,string>,"
        + " c8 struct<r:string,s:int,t:double>,"
        + " c9 tinyint, c10 smallint, c11 float, c12 bigint,"
        + " c13 array<array<string>>,"
        + " c14 map<int, map<int,int>>,"
        + " c15 struct<r:int,s:struct<a:int,b:string>>,"
        + " c16 array<struct<m:map<string,string>,n:int>>) comment '"+dataTypeTableComment
            +"' partitioned by (dt STRING)");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '"
        + dataTypeDataFilePath.toString() + "' into table " + dataTypeTableName
        + " PARTITION (dt='20090619')");
    assertFalse(res.next());

    // drop view. ignore error.
    try {
      stmt.executeQuery("drop view " + viewName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    // create view
    res = stmt.executeQuery("create view " + viewName + " comment '"+viewComment
            +"' as select * from "+ tableName);
    assertFalse(res.next());
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

    // drop table
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);
    ResultSet res = stmt.executeQuery("drop table " + tableName);
    assertFalse(res.next());
    res = stmt.executeQuery("drop table " + partitionedTableName);
    assertFalse(res.next());
    res = stmt.executeQuery("drop table " + dataTypeTableName);
    assertFalse(res.next());

    con.close();
    assertTrue("Connection should be closed", con.isClosed());

    Exception expectedException = null;
    try {
      con.createStatement();
    } catch (Exception e) {
      expectedException = e;
    }

    assertNotNull(
        "createStatement() on closed connection should throw exception",
        expectedException);
  }

  /**
   * verify 'explain ...' resultset
   * @throws SQLException
   */
  public void testExplainStmt() throws SQLException {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "explain select c1, c2, c3, c4, c5 as a, c6, c7, c8, c9, c10, c11, c12, " +
        "c1*2, sentences(null, null, null) as b from " + dataTypeTableName + " limit 1");

    ResultSetMetaData md = res.getMetaData();
    assertEquals(md.getColumnCount(), 1); // only one result column
    assertEquals(md.getColumnLabel(1), EXPL_COLUMN_NAME); // verify the column name
    //verify that there is data in the resultset
    assertTrue("Nothing returned explain", res.next());
  }

  public void testPrepareStatement() {

    String sql = "from (select count(1) from "
        + tableName
        + " where   'not?param?not?param' <> 'not_param??not_param' and ?=? "
        + " and 1=? and 2=? and 3.0=? and 4.0=? and 'test\\'string\"'=? and 5=? and ?=? "
        + " ) t  select '2011-03-25' ddate,'China',true bv, 10 num limit 10";

     ///////////////////////////////////////////////
    //////////////////// correct testcase
    //////////////////////////////////////////////
    try {
      PreparedStatement ps = con.prepareStatement(sql);

      ps.setBoolean(1, true);
      ps.setBoolean(2, true);

      ps.setShort(3, Short.valueOf("1"));
      ps.setInt(4, 2);
      ps.setFloat(5, 3f);
      ps.setDouble(6, Double.valueOf(4));
      ps.setString(7, "test'string\"");
      ps.setLong(8, 5L);
      ps.setByte(9, (byte) 1);
      ps.setByte(10, (byte) 1);

      ps.setMaxRows(2);

      assertTrue(true);

      ResultSet res = ps.executeQuery();
      assertNotNull(res);

      while (res.next()) {
        assertEquals("2011-03-25", res.getString("ddate"));
        assertEquals("10", res.getString("num"));
        assertEquals((byte) 10, res.getByte("num"));
        assertEquals("2011-03-25", res.getDate("ddate").toString());
        assertEquals(Double.valueOf(10).doubleValue(), res.getDouble("num"), 0.1);
        assertEquals(10, res.getInt("num"));
        assertEquals(Short.valueOf("10").shortValue(), res.getShort("num"));
        assertEquals(10L, res.getLong("num"));
        assertEquals(true, res.getBoolean("bv"));
        Object o = res.getObject("ddate");
        assertNotNull(o);
        o = res.getObject("num");
        assertNotNull(o);
      }
      res.close();
      assertTrue(true);

      ps.close();
      assertTrue(true);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }

     ///////////////////////////////////////////////
    //////////////////// other failure testcases
    //////////////////////////////////////////////
    // set nothing for prepared sql
    Exception expectedException = null;
    try {
      PreparedStatement ps = con.prepareStatement(sql);
      ps.executeQuery();
    } catch (Exception e) {
      expectedException = e;
    }
    assertNotNull(
        "Execute the un-setted sql statement should throw exception",
        expectedException);

    // set some of parameters for prepared sql, not all of them.
    expectedException = null;
    try {
      PreparedStatement ps = con.prepareStatement(sql);
      ps.setBoolean(1, true);
      ps.setBoolean(2, true);
      ps.executeQuery();
    } catch (Exception e) {
      expectedException = e;
    }
    assertNotNull(
        "Execute the invalid setted sql statement should throw exception",
        expectedException);

    // set the wrong type parameters for prepared sql.
    expectedException = null;
    try {
      PreparedStatement ps = con.prepareStatement(sql);

      // wrong type here
      ps.setString(1, "wrong");

      assertTrue(true);
      ResultSet res = ps.executeQuery();
      if (!res.next()) {
        throw new Exception("there must be a empty result set");
      }
    } catch (Exception e) {
      expectedException = e;
    }
    assertNotNull(
        "Execute the invalid setted sql statement should throw exception",
        expectedException);
  }

  public final void testSelectAll() throws Exception {
    doTestSelectAll(tableName, -1, -1); // tests not setting maxRows (return all)
    doTestSelectAll(tableName, 0, -1); // tests setting maxRows to 0 (return all)
  }

  public final void testSelectAllPartioned() throws Exception {
    doTestSelectAll(partitionedTableName, -1, -1); // tests not setting maxRows
    // (return all)
    doTestSelectAll(partitionedTableName, 0, -1); // tests setting maxRows to 0
    // (return all)
  }

  public final void testSelectAllMaxRows() throws Exception {
    doTestSelectAll(tableName, 100, -1);
  }

  public final void testSelectAllFetchSize() throws Exception {
    doTestSelectAll(tableName, 100, 20);
  }

  public void testDataTypes() throws Exception {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "select * from " + dataTypeTableName + " order by c1");
    ResultSetMetaData meta = res.getMetaData();

    // row 1
    assertTrue(res.next());
    // skip the last (partitioning) column since it is always non-null
    for (int i = 1; i < meta.getColumnCount(); i++) {
      assertNull(res.getObject(i));
    }

    // row 2
    assertTrue(res.next());
    assertEquals(-1, res.getInt(1));
    assertEquals(false, res.getBoolean(2));
    assertEquals(-1.1d, res.getDouble(3));
    assertEquals("", res.getString(4));
    assertEquals("[]", res.getString(5));
    assertEquals("{}", res.getString(6));
    assertEquals("{}", res.getString(7));
    assertEquals("[null, null, null]", res.getString(8));
    assertEquals(-1, res.getByte(9));
    assertEquals(-1, res.getShort(10));
    assertEquals(-1.0f, res.getFloat(11));
    assertEquals(-1, res.getLong(12));
    assertEquals("[]", res.getString(13));
    assertEquals("{}", res.getString(14));
    assertEquals("[null, null]", res.getString(15));
    assertEquals("[]", res.getString(16));

    // row 3
    assertTrue(res.next());
    assertEquals(1, res.getInt(1));
    assertEquals(true, res.getBoolean(2));
    assertEquals(1.1d, res.getDouble(3));
    assertEquals("1", res.getString(4));
    assertEquals("[1, 2]", res.getString(5));
    assertEquals("{1=x, 2=y}", res.getString(6));
    assertEquals("{k=v}", res.getString(7));
    assertEquals("[a, 9, 2.2]", res.getString(8));
    assertEquals(1, res.getByte(9));
    assertEquals(1, res.getShort(10));
    assertEquals(1.0f, res.getFloat(11));
    assertEquals(1, res.getLong(12));
    assertEquals("[[a, b], [c, d]]", res.getString(13));
    assertEquals("{1={11=12, 13=14}, 2={21=22}}", res.getString(14));
    assertEquals("[1, [2, x]]", res.getString(15));
    assertEquals("[[{}, 1], [{c=d, a=b}, 2]]", res.getString(16));

    // test getBoolean rules on non-boolean columns
    assertEquals(true, res.getBoolean(1));
    assertEquals(true, res.getBoolean(4));

    // no more rows
    assertFalse(res.next());
  }

  private void doTestSelectAll(String tableName, int maxRows, int fetchSize) throws Exception {
    boolean isPartitionTable = tableName.equals(partitionedTableName);

    Statement stmt = con.createStatement();
    if (maxRows >= 0) {
      stmt.setMaxRows(maxRows);
    }
    if (fetchSize > 0) {
      stmt.setFetchSize(fetchSize);
      assertEquals(fetchSize, stmt.getFetchSize());
    }

    // JDBC says that 0 means return all, which is the default
    int expectedMaxRows = maxRows < 1 ? 0 : maxRows;

    assertNotNull("Statement is null", stmt);
    assertEquals("Statement max rows not as expected", expectedMaxRows, stmt
        .getMaxRows());
    assertFalse("Statement should not be closed", stmt.isClosed());

    ResultSet res;

    // run some queries
    res = stmt.executeQuery("select * from " + tableName);
    assertNotNull("ResultSet is null", res);
    assertTrue("getResultSet() not returning expected ResultSet", res == stmt
        .getResultSet());
    assertEquals("get update count not as expected", 0, stmt.getUpdateCount());
    int i = 0;

    ResultSetMetaData meta = res.getMetaData();
    int expectedColCount = isPartitionTable ? 3 : 2;
    assertEquals(
      "Unexpected column count", expectedColCount, meta.getColumnCount());

    boolean moreRow = res.next();
    while (moreRow) {
      try {
        i++;
        assertEquals(res.getInt(1), res.getInt("under_col"));
        assertEquals(res.getString(1), res.getString("under_col"));
        assertEquals(res.getString(2), res.getString("value"));
        if (isPartitionTable) {
          assertEquals(res.getString(3), partitionedColumnValue);
          assertEquals(res.getString(3), res.getString(partitionedColumnName));
        }
        assertFalse("Last result value was not null", res.wasNull());
        assertNull("No warnings should be found on ResultSet", res
            .getWarnings());
        res.clearWarnings(); // verifying that method is supported

        // System.out.println(res.getString(1) + " " + res.getString(2));
        assertEquals(
            "getInt and getString don't align for the same result value",
            String.valueOf(res.getInt(1)), res.getString(1));
        assertEquals("Unexpected result found", "val_" + res.getString(1), res
            .getString(2));
        moreRow = res.next();
      } catch (SQLException e) {
        System.out.println(e.toString());
        e.printStackTrace();
        throw new Exception(e.toString());
      }
    }

    // supposed to get 500 rows if maxRows isn't set
    int expectedRowCount = maxRows > 0 ? maxRows : 500;
    assertEquals("Incorrect number of rows returned", expectedRowCount, i);

    // should have no more rows
    assertEquals(false, moreRow);

    assertNull("No warnings should be found on statement", stmt.getWarnings());
    stmt.clearWarnings(); // verifying that method is supported

    assertNull("No warnings should be found on connection", con.getWarnings());
    con.clearWarnings(); // verifying that method is supported

    stmt.close();
    assertTrue("Statement should be closed", stmt.isClosed());
  }

  public void testErrorMessages() throws SQLException {
    String invalidSyntaxSQLState = "42000";

    // These tests inherently cause exceptions to be written to the test output
    // logs. This is undesirable, since you it might appear to someone looking
    // at the test output logs as if something is failing when it isn't. Not
    // sure
    // how to get around that.
    doTestErrorCase("SELECTT * FROM " + tableName,
        "cannot recognize input near 'SELECTT' '*' 'FROM'",
        invalidSyntaxSQLState, 40000);
    doTestErrorCase("SELECT * FROM some_table_that_does_not_exist",
        "Table not found", "42S02", 10001);
    doTestErrorCase("drop table some_table_that_does_not_exist",
        "Table not found", "42S02", 10001);
    doTestErrorCase("SELECT invalid_column FROM " + tableName,
        "Invalid table alias or column reference", invalidSyntaxSQLState, 10004);
    doTestErrorCase("SELECT invalid_function(under_col) FROM " + tableName,
    "Invalid function", invalidSyntaxSQLState, 10011);

    // TODO: execute errors like this currently don't return good error
    // codes and messages. This should be fixed.
    doTestErrorCase(
        "create table " + tableName + " (key int, value string)",
        "FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask",
        "08S01", 1);
  }

  private void doTestErrorCase(String sql, String expectedMessage,
      String expectedSQLState, int expectedErrorCode) throws SQLException {
    Statement stmt = con.createStatement();
    boolean exceptionFound = false;
    try {
      stmt.executeQuery(sql);
    } catch (SQLException e) {
      assertTrue("Adequate error messaging not found for '" + sql + "': "
          + e.getMessage(), e.getMessage().contains(expectedMessage));
      assertEquals("Expected SQLState not found for '" + sql + "'",
          expectedSQLState, e.getSQLState());
      assertEquals("Expected error code not found for '" + sql + "'",
          expectedErrorCode, e.getErrorCode());
      exceptionFound = true;
    }

    assertNotNull("Exception should have been thrown for query: " + sql,
        exceptionFound);
  }

  public void testShowTables() throws SQLException {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res = stmt.executeQuery("show tables");

    boolean testTableExists = false;
    while (res.next()) {
      assertNotNull("table name is null in result set", res.getString(1));
      if (tableName.equalsIgnoreCase(res.getString(1))) {
        testTableExists = true;
      }
    }

    assertTrue("table name " + tableName
        + " not found in SHOW TABLES result set", testTableExists);
  }

  public void testMetaDataGetTables() throws SQLException {
    Map<String, Object[]> tests = new HashMap<String, Object[]>();
    tests.put("test%jdbc%", new Object[]{"testhivejdbcdriver_table"
            , "testhivejdbcdriverpartitionedtable"
            , "testhivejdbcdriverview"});
    tests.put("%jdbcdriver\\_table", new Object[]{"testhivejdbcdriver_table"});
    tests.put("testhivejdbcdriver\\_table", new Object[]{"testhivejdbcdriver_table"});
    tests.put("test_ivejdbcdri_er\\_table", new Object[]{"testhivejdbcdriver_table"});
    tests.put("test_ivejdbcdri_er_table", new Object[]{"testhivejdbcdriver_table"});
    tests.put("test_ivejdbcdri_er%table", new Object[]{
        "testhivejdbcdriver_table", "testhivejdbcdriverpartitionedtable" });
    tests.put("%jdbc%", new Object[]{ "testhivejdbcdriver_table"
            , "testhivejdbcdriverpartitionedtable"
            , "testhivejdbcdriverview"});
    tests.put("", new Object[]{});

    for (String checkPattern: tests.keySet()) {
      ResultSet rs = (ResultSet)con.getMetaData().getTables("default", null, checkPattern, null);
      int cnt = 0;
      while (rs.next()) {
        String resultTableName = rs.getString("TABLE_NAME");
        assertEquals("Get by index different from get by name.", rs.getString(3), resultTableName);
        assertEquals("Excpected a different table.", tests.get(checkPattern)[cnt], resultTableName);
        String resultTableComment = rs.getString("REMARKS");
        assertTrue("Missing comment on the table.", resultTableComment.length()>0);
        String tableType = rs.getString("TABLE_TYPE");
        if (resultTableName.endsWith("view")) {
          assertEquals("Expected a tabletype view but got something else.", "VIEW", tableType);
        }
        cnt++;
      }
      rs.close();
      assertEquals("Received an incorrect number of tables.", tests.get(checkPattern).length, cnt);
    }

    // only ask for the views.
    ResultSet rs = (ResultSet)con.getMetaData().getTables("default", null, null
            , new String[]{"VIEW"});
    int cnt=0;
    while (rs.next()) {
      cnt++;
    }
    rs.close();
    assertEquals("Incorrect number of views found.", 1, cnt);
  }

  public void testMetaDataGetCatalogs() throws SQLException {
    ResultSet rs = (ResultSet)con.getMetaData().getCatalogs();
    int cnt = 0;
    while (rs.next()) {
      String catalogname = rs.getString("TABLE_CAT");
      assertEquals("Get by index different from get by name", rs.getString(1), catalogname);
      switch(cnt) {
        case 0:
          assertEquals("default", catalogname);
          break;
        default:
          fail("More then one catalog found.");
          break;
      }
      cnt++;
    }
    rs.close();
    assertEquals("Incorrect catalog count", 1, cnt);
  }

  public void testMetaDataGetSchemas() throws SQLException {
    ResultSet rs = (ResultSet)con.getMetaData().getSchemas();
    int cnt = 0;
    while (rs.next()) {
      cnt++;
    }
    rs.close();
    assertEquals("Incorrect schema count", 0, cnt);
  }

  public void testMetaDataGetTableTypes() throws SQLException {
    ResultSet rs = (ResultSet)con.getMetaData().getTableTypes();
    Set<String> tabletypes = new HashSet();
    tabletypes.add("TABLE");
    tabletypes.add("EXTERNAL TABLE");
    tabletypes.add("VIEW");

    int cnt = 0;
    while (rs.next()) {
      String tabletype = rs.getString("TABLE_TYPE");
      assertEquals("Get by index different from get by name", rs.getString(1), tabletype);
      tabletypes.remove(tabletype);
      cnt++;
    }
    rs.close();
    assertEquals("Incorrect tabletype count.", 0, tabletypes.size());
    assertTrue("Found less tabletypes then we test for.", cnt >= tabletypes.size());
  }

  public void testMetaDataGetColumns() throws SQLException {
    Map<String[], Integer> tests = new HashMap<String[], Integer>();
    tests.put(new String[]{"testhivejdbcdriver\\_table", null}, 2);
    tests.put(new String[]{"testhivejdbc%", null}, 7);
    tests.put(new String[]{"testhiveJDBC%", null}, 7);
    tests.put(new String[]{"testhiveJDB\\C%", null}, 0);
    tests.put(new String[]{"%jdbcdriver\\_table", null}, 2);
    tests.put(new String[]{"%jdbcdriver\\_table%", "under\\_col"}, 1);
    tests.put(new String[]{"%jdbcdriver\\_table%", "under\\_COL"}, 1);
    tests.put(new String[]{"%jdbcdriver\\_table%", "under\\_co_"}, 1);
    tests.put(new String[]{"%jdbcdriver\\_table%", "under_col"}, 1);
    tests.put(new String[]{"%jdbcdriver\\_table%", "und%"}, 1);
    tests.put(new String[]{"%jdbcdriver\\_table%", "%"}, 2);
    tests.put(new String[]{"%jdbcdriver\\_table%", "_%"}, 2);

    for (String[] checkPattern: tests.keySet()) {
      ResultSet rs = con.getMetaData().getColumns(null, null, checkPattern[0],
          checkPattern[1]);

      // validate the metadata for the getColumns result set
      ResultSetMetaData rsmd = rs.getMetaData();
      assertEquals("TABLE_CAT", rsmd.getColumnName(1));

      int cnt = 0;
      while (rs.next()) {
        String columnname = rs.getString("COLUMN_NAME");
        int ordinalPos = rs.getInt("ORDINAL_POSITION");
        switch(cnt) {
          case 0:
            assertEquals("Wrong column name found", "under_col", columnname);
            assertEquals("Wrong ordinal position found", ordinalPos, 1);
            break;
          case 1:
            assertEquals("Wrong column name found", "value", columnname);
            assertEquals("Wrong ordinal position found", ordinalPos, 2);
            break;
          default:
            break;
        }
        cnt++;
      }
      rs.close();
      assertEquals("Found less columns then we test for.", tests.get(checkPattern).intValue(), cnt);
    }
  }

  /**
   * Validate the Metadata for the result set of a metadata getColumns call.
   */
  public void testMetaDataGetColumnsMetaData() throws SQLException {
    ResultSet rs = (ResultSet)con.getMetaData().getColumns(null, null
            , "testhivejdbcdriver\\_table", null);

    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals("TABLE_CAT", rsmd.getColumnName(1));
    assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
    assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(1));

    assertEquals("ORDINAL_POSITION", rsmd.getColumnName(17));
    assertEquals(Types.INTEGER, rsmd.getColumnType(17));
    assertEquals(11, rsmd.getColumnDisplaySize(17));
  }

  public void testConversionsBaseResultSet() throws SQLException {
    ResultSet rs = new HiveMetaDataResultSet(Arrays.asList("key")
            , Arrays.asList("long")
            , Arrays.asList(1234, "1234", "abc")) {
      private int cnt=1;
      public boolean next() throws SQLException {
        if (cnt<data.size()) {
          row = Arrays.asList(data.get(cnt));
          cnt++;
          return true;
        } else {
          return false;
        }
      }
    };

    while (rs.next()) {
      String key = rs.getString("key");
      if ("1234".equals(key)) {
        assertEquals("Converting a string column into a long failed.", rs.getLong("key"), 1234L);
        assertEquals("Converting a string column into a int failed.", rs.getInt("key"), 1234);
      } else if ("abc".equals(key)) {
        Object result = null;
        Exception expectedException = null;
        try {
          result = rs.getLong("key");
        } catch (SQLException e) {
          expectedException = e;
        }
        assertTrue("Trying to convert 'abc' into a long should not work.", expectedException!=null);
        try {
          result = rs.getInt("key");
        } catch (SQLException e) {
          expectedException = e;
        }
        assertTrue("Trying to convert 'abc' into a int should not work.", expectedException!=null);
      }
    }
  }

  public void testDescribeTable() throws SQLException {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res = stmt.executeQuery("describe " + tableName);

    res.next();
    assertEquals("Column name 'under_col' not found", "under_col", res.getString(1));
    assertEquals("Column type 'under_col' for column under_col not found", "int", res
        .getString(2));
    res.next();
    assertEquals("Column name 'value' not found", "value", res.getString(1));
    assertEquals("Column type 'string' for column key not found", "string", res
        .getString(2));

    assertFalse("More results found than expected", res.next());

  }

  public void testDatabaseMetaData() throws SQLException {
    DatabaseMetaData meta = con.getMetaData();

    assertEquals("Hive", meta.getDatabaseProductName());
    assertEquals("1", meta.getDatabaseProductVersion());
    assertEquals(DatabaseMetaData.sqlStateSQL99, meta.getSQLStateType());
    assertNull(meta.getProcedures(null, null, null));
    assertFalse(meta.supportsCatalogsInTableDefinitions());
    assertFalse(meta.supportsSchemasInTableDefinitions());
    assertFalse(meta.supportsSchemasInDataManipulation());
    assertFalse(meta.supportsMultipleResultSets());
    assertFalse(meta.supportsStoredProcedures());
    assertTrue(meta.supportsAlterTableWithAddColumn());
  }

  public void testResultSetMetaData() throws SQLException {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "select c1, c2, c3, c4, c5 as a, c6, c7, c8, c9, c10, c11, c12, " +
        "c1*2, sentences(null, null, null) as b from " + dataTypeTableName + " limit 1");
    ResultSetMetaData meta = res.getMetaData();

    ResultSet colRS = con.getMetaData().getColumns(null, null,
        dataTypeTableName.toLowerCase(), null);

    assertEquals(14, meta.getColumnCount());

    assertTrue(colRS.next());

    assertEquals("c1", meta.getColumnName(1));
    assertEquals(Types.INTEGER, meta.getColumnType(1));
    assertEquals("int", meta.getColumnTypeName(1));
    assertEquals(11, meta.getColumnDisplaySize(1));
    assertEquals(10, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));

    assertEquals("c1", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.INTEGER, colRS.getInt("DATA_TYPE"));
    assertEquals("int", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(1), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(1), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c2", meta.getColumnName(2));
    assertEquals("boolean", meta.getColumnTypeName(2));
    assertEquals(Types.BOOLEAN, meta.getColumnType(2));
    assertEquals(1, meta.getColumnDisplaySize(2));
    assertEquals(1, meta.getPrecision(2));
    assertEquals(0, meta.getScale(2));

    assertEquals("c2", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.BOOLEAN, colRS.getInt("DATA_TYPE"));
    assertEquals("boolean", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(2), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(2), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c3", meta.getColumnName(3));
    assertEquals(Types.DOUBLE, meta.getColumnType(3));
    assertEquals("double", meta.getColumnTypeName(3));
    assertEquals(25, meta.getColumnDisplaySize(3));
    assertEquals(15, meta.getPrecision(3));
    assertEquals(15, meta.getScale(3));

    assertEquals("c3", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.DOUBLE, colRS.getInt("DATA_TYPE"));
    assertEquals("double", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(3), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(3), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c4", meta.getColumnName(4));
    assertEquals(Types.VARCHAR, meta.getColumnType(4));
    assertEquals("string", meta.getColumnTypeName(4));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(4));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(4));
    assertEquals(0, meta.getScale(4));

    assertEquals("c4", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.VARCHAR, colRS.getInt("DATA_TYPE"));
    assertEquals("string", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(4), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(4), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("a", meta.getColumnName(5));
    assertEquals(Types.VARCHAR, meta.getColumnType(5));
    assertEquals("string", meta.getColumnTypeName(5));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(5));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(5));
    assertEquals(0, meta.getScale(5));

    assertEquals("c5", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.VARCHAR, colRS.getInt("DATA_TYPE"));
    assertEquals("array<int>", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(5), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(5), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c6", meta.getColumnName(6));
    assertEquals(Types.VARCHAR, meta.getColumnType(6));
    assertEquals("string", meta.getColumnTypeName(6));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(6));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(6));
    assertEquals(0, meta.getScale(6));

    assertEquals("c6", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.VARCHAR, colRS.getInt("DATA_TYPE"));
    assertEquals("map<int,string>", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(6), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(6), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c7", meta.getColumnName(7));
    assertEquals(Types.VARCHAR, meta.getColumnType(7));
    assertEquals("string", meta.getColumnTypeName(7));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(7));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(7));
    assertEquals(0, meta.getScale(7));

    assertEquals("c7", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.VARCHAR, colRS.getInt("DATA_TYPE"));
    assertEquals("map<string,string>", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(7), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(7), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c8", meta.getColumnName(8));
    assertEquals(Types.VARCHAR, meta.getColumnType(8));
    assertEquals("string", meta.getColumnTypeName(8));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(8));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(8));
    assertEquals(0, meta.getScale(8));

    assertEquals("c8", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.VARCHAR, colRS.getInt("DATA_TYPE"));
    assertEquals("struct<r:string,s:int,t:double>", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(8), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(8), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c9", meta.getColumnName(9));
    assertEquals(Types.TINYINT, meta.getColumnType(9));
    assertEquals("tinyint", meta.getColumnTypeName(9));
    assertEquals(4, meta.getColumnDisplaySize(9));
    assertEquals(3, meta.getPrecision(9));
    assertEquals(0, meta.getScale(9));

    assertEquals("c9", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.TINYINT, colRS.getInt("DATA_TYPE"));
    assertEquals("tinyint", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(9), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(9), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c10", meta.getColumnName(10));
    assertEquals(Types.SMALLINT, meta.getColumnType(10));
    assertEquals("smallint", meta.getColumnTypeName(10));
    assertEquals(6, meta.getColumnDisplaySize(10));
    assertEquals(5, meta.getPrecision(10));
    assertEquals(0, meta.getScale(10));

    assertEquals("c10", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.SMALLINT, colRS.getInt("DATA_TYPE"));
    assertEquals("smallint", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(10), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(10), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c11", meta.getColumnName(11));
    assertEquals(Types.FLOAT, meta.getColumnType(11));
    assertEquals("float", meta.getColumnTypeName(11));
    assertEquals(24, meta.getColumnDisplaySize(11));
    assertEquals(7, meta.getPrecision(11));
    assertEquals(7, meta.getScale(11));

    assertEquals("c11", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.FLOAT, colRS.getInt("DATA_TYPE"));
    assertEquals("float", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(11), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(11), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c12", meta.getColumnName(12));
    assertEquals(Types.BIGINT, meta.getColumnType(12));
    assertEquals("bigint", meta.getColumnTypeName(12));
    assertEquals(20, meta.getColumnDisplaySize(12));
    assertEquals(19, meta.getPrecision(12));
    assertEquals(0, meta.getScale(12));

    assertEquals("c12", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.BIGINT, colRS.getInt("DATA_TYPE"));
    assertEquals("bigint", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(12), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(12), colRS.getInt("DECIMAL_DIGITS"));

    assertEquals("_c12", meta.getColumnName(13));
    assertEquals(Types.INTEGER, meta.getColumnType(13));
    assertEquals("int", meta.getColumnTypeName(13));
    assertEquals(11, meta.getColumnDisplaySize(13));
    assertEquals(10, meta.getPrecision(13));
    assertEquals(0, meta.getScale(13));

    assertEquals("b", meta.getColumnName(14));
    assertEquals(Types.VARCHAR, meta.getColumnType(14));
    assertEquals("string", meta.getColumnTypeName(14));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(14));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(14));
    assertEquals(0, meta.getScale(14));

    for (int i = 1; i <= meta.getColumnCount(); i++) {
      assertFalse(meta.isAutoIncrement(i));
      assertFalse(meta.isCurrency(i));
      assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(i));
    }
  }

  // [url] [host] [port] [db]
  private static final String[][] URL_PROPERTIES = new String[][] {
      {"jdbc:hive://", "", "", "default"},
      {"jdbc:hive://localhost:10001/default", "localhost", "10001", "default"},
      {"jdbc:hive://localhost/notdefault", "localhost", "10000", "notdefault"},
      {"jdbc:hive://foo:1243", "foo", "1243", "default"}};

  public void testDriverProperties() throws SQLException {
    HiveDriver driver = new HiveDriver();

    for (String[] testValues : URL_PROPERTIES) {
      DriverPropertyInfo[] dpi = driver.getPropertyInfo(testValues[0], null);
      assertEquals("unexpected DriverPropertyInfo array size", 3, dpi.length);
      assertDpi(dpi[0], "HOST", testValues[1]);
      assertDpi(dpi[1], "PORT", testValues[2]);
      assertDpi(dpi[2], "DBNAME", testValues[3]);
    }

  }

  private static void assertDpi(DriverPropertyInfo dpi, String name,
      String value) {
    assertEquals("Invalid DriverPropertyInfo name", name, dpi.name);
    assertEquals("Invalid DriverPropertyInfo value", value, dpi.value);
    assertEquals("Invalid DriverPropertyInfo required", false, dpi.required);
  }


  /**
   * validate schema generated by "set" command
   * @throws SQLException
   */
  public void testSetCommand() throws SQLException {
    // execute set command
    String sql = "set -v";
    Statement stmt = con.createStatement();
    ResultSet res = stmt.executeQuery(sql);

    // Validate resultset columns
    ResultSetMetaData md = res.getMetaData() ;
    assertEquals(1, md.getColumnCount());
    assertEquals(SET_COLUMN_NAME, md.getColumnLabel(1));

    //check if there is data in the resultset
    assertTrue("Nothing returned by set -v", res.next());

    res.close();
    stmt.close();
  }

}
