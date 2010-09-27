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

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
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

/**
 * TestJdbcDriver.
 *
 */
public class TestJdbcDriver extends TestCase {
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static String tableName = "testHiveJdbcDriverTable";
  private static String tableComment = "Simple table";
  private static String viewName = "testHiveJdbcDriverView";
  private static String viewComment = "Simple view";
  private static String partitionedTableName = "testHiveJdbcDriverPartitionedTable";
  private static String partitionedTableComment = "Partitioned table";
  private static String dataTypeTableName = "testDataTypeTable";
  private static String dataTypeTableComment = "Table with many column data types";
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
        + " (key int comment 'the key', value string) comment '"+tableComment+"'");
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
        + " (key int, value string) comment '"+partitionedTableComment
            +"' partitioned by (dt STRING)");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '"
        + dataFilePath.toString() + "' into table " + partitionedTableName
        + " PARTITION (dt='20090619')");
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

  public final void testSelectAll() throws Exception {
    doTestSelectAll(tableName, -1); // tests not setting maxRows (return all)
    doTestSelectAll(tableName, 0); // tests setting maxRows to 0 (return all)
  }

  public final void testSelectAllPartioned() throws Exception {
    doTestSelectAll(partitionedTableName, -1); // tests not setting maxRows
    // (return all)
    doTestSelectAll(partitionedTableName, 0); // tests setting maxRows to 0
    // (return all)
  }

  public final void testSelectAllMaxRows() throws Exception {
    doTestSelectAll(tableName, 100);
  }

  public void testDataTypes() throws Exception {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "select * from " + dataTypeTableName + " order by c1");
    ResultSetMetaData meta = res.getMetaData();

    // row 1
    assertTrue(res.next());
    for (int i = 1; i <= meta.getColumnCount(); i++) {
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

  private void doTestSelectAll(String tableName, int maxRows) throws Exception {
    Statement stmt = con.createStatement();
    if (maxRows >= 0) {
      stmt.setMaxRows(maxRows);
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
    assertEquals("Unexpected column count", 2, meta.getColumnCount());

    boolean moreRow = res.next();
    while (moreRow) {
      try {
        i++;
        assertEquals(res.getInt(1), res.getInt("key"));
        assertEquals(res.getString(1), res.getString("key"));
        assertEquals(res.getString(2), res.getString("value"));
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
    int parseErrorCode = 10;

    // These tests inherently cause exceptions to be written to the test output
    // logs. This is undesirable, since you it might appear to someone looking
    // at the test output logs as if something is failing when it isn't. Not
    // sure
    // how to get around that.
    doTestErrorCase("SELECTT * FROM " + tableName,
        "cannot recognize input 'SELECTT'", invalidSyntaxSQLState, 11);
    doTestErrorCase("SELECT * FROM some_table_that_does_not_exist",
        "Table not found", "42S02", parseErrorCode);
    doTestErrorCase("drop table some_table_that_does_not_exist",
        "Table not found", "42S02", parseErrorCode);
    doTestErrorCase("SELECT invalid_column FROM " + tableName,
        "Invalid Table Alias or Column Reference", invalidSyntaxSQLState,
        parseErrorCode);
    doTestErrorCase("SELECT invalid_function(key) FROM " + tableName,
        "Invalid Function", invalidSyntaxSQLState, parseErrorCode);

    // TODO: execute errors like this currently don't return good messages (i.e.
    // 'Table already exists'). This is because the Driver class calls
    // Task.executeTask() which swallows meaningful exceptions and returns a
    // status
    // code. This should be refactored.
    doTestErrorCase(
        "create table " + tableName + " (key int, value string)",
        "Query returned non-zero code: 9, cause: FAILED: Execution Error, "
        + "return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask",
        "08S01", 9);
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
    tests.put("test%jdbc%", new Object[]{"testhivejdbcdriverpartitionedtable"
            , "testhivejdbcdrivertable"
            , "testhivejdbcdriverview"});
    tests.put("%jdbcdrivertable", new Object[]{"testhivejdbcdrivertable"});
    tests.put("testhivejdbcdrivertable", new Object[]{"testhivejdbcdrivertable"});
    tests.put("test_ivejdbcdri_ertable", new Object[]{"testhivejdbcdrivertable"});
    tests.put("%jdbc%", new Object[]{"testhivejdbcdriverpartitionedtable"
            , "testhivejdbcdrivertable"
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
    assertEquals("Incorrect schema count", 1, cnt);
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
    tests.put(new String[]{"testhivejdbcdrivertable", null}, 2);
    tests.put(new String[]{"testhivejdbc%", null}, 6);
    tests.put(new String[]{"%jdbcdrivertable", null}, 2);
    tests.put(new String[]{"%jdbcdrivertable%", "key"}, 1);
    tests.put(new String[]{"%jdbcdrivertable%", "ke_"}, 1);
    tests.put(new String[]{"%jdbcdrivertable%", "ke%"}, 1);

    for (String[] checkPattern: tests.keySet()) {
      ResultSet rs = (ResultSet)con.getMetaData().getColumns(null, null
              , checkPattern[0], checkPattern[1]);
      int cnt = 0;
      while (rs.next()) {
        String columnname = rs.getString("COLUMN_NAME");
        int ordinalPos = rs.getInt("ORDINAL_POSITION");
        switch(cnt) {
          case 0:
            assertEquals("Wrong column name found", "key", columnname);
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
    res.next();
    res.next();
    assertEquals("Column name 'key' not found", "key", res.getString(1).trim());
    assertEquals("Column type 'int' for column key not found", "int", res
        .getString(2).trim());
    res.next();
    assertEquals("Column name 'value' not found", "value", res.getString(1).trim());
    assertEquals("Column type 'string' for column key not found", "string", res
        .getString(2).trim());

    assertFalse("More results found than expected", res.next());

  }

  public void testDatabaseMetaData() throws SQLException {
    DatabaseMetaData meta = con.getMetaData();

    assertEquals("Hive", meta.getDatabaseProductName());
    assertEquals("0", meta.getDatabaseProductVersion());
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

    assertEquals(14, meta.getColumnCount());

    assertEquals("c1", meta.getColumnName(1));
    assertEquals("c2", meta.getColumnName(2));
    assertEquals("c3", meta.getColumnName(3));
    assertEquals("c4", meta.getColumnName(4));
    assertEquals("a", meta.getColumnName(5));
    assertEquals("c6", meta.getColumnName(6));
    assertEquals("c7", meta.getColumnName(7));
    assertEquals("c8", meta.getColumnName(8));
    assertEquals("c9", meta.getColumnName(9));
    assertEquals("c10", meta.getColumnName(10));
    assertEquals("c11", meta.getColumnName(11));
    assertEquals("c12", meta.getColumnName(12));
    assertEquals("_c12", meta.getColumnName(13));
    assertEquals("b", meta.getColumnName(14));

    assertEquals(Types.INTEGER, meta.getColumnType(1));
    assertEquals(Types.BOOLEAN, meta.getColumnType(2));
    assertEquals(Types.DOUBLE, meta.getColumnType(3));
    assertEquals(Types.VARCHAR, meta.getColumnType(4));
    assertEquals(Types.VARCHAR, meta.getColumnType(5));
    assertEquals(Types.VARCHAR, meta.getColumnType(6));
    assertEquals(Types.VARCHAR, meta.getColumnType(7));
    assertEquals(Types.VARCHAR, meta.getColumnType(8));
    assertEquals(Types.TINYINT, meta.getColumnType(9));
    assertEquals(Types.SMALLINT, meta.getColumnType(10));
    assertEquals(Types.FLOAT, meta.getColumnType(11));
    assertEquals(Types.BIGINT, meta.getColumnType(12));
    assertEquals(Types.INTEGER, meta.getColumnType(13));
    assertEquals(Types.VARCHAR, meta.getColumnType(14));

    assertEquals("int", meta.getColumnTypeName(1));
    assertEquals("boolean", meta.getColumnTypeName(2));
    assertEquals("double", meta.getColumnTypeName(3));
    assertEquals("string", meta.getColumnTypeName(4));
    assertEquals("string", meta.getColumnTypeName(5));
    assertEquals("string", meta.getColumnTypeName(6));
    assertEquals("string", meta.getColumnTypeName(7));
    assertEquals("string", meta.getColumnTypeName(8));
    assertEquals("tinyint", meta.getColumnTypeName(9));
    assertEquals("smallint", meta.getColumnTypeName(10));
    assertEquals("float", meta.getColumnTypeName(11));
    assertEquals("bigint", meta.getColumnTypeName(12));
    assertEquals("int", meta.getColumnTypeName(13));
    assertEquals("string", meta.getColumnTypeName(14));

    assertEquals(16, meta.getColumnDisplaySize(1));
    assertEquals(8, meta.getColumnDisplaySize(2));
    assertEquals(16, meta.getColumnDisplaySize(3));
    assertEquals(32, meta.getColumnDisplaySize(4));
    assertEquals(32, meta.getColumnDisplaySize(5));
    assertEquals(32, meta.getColumnDisplaySize(6));
    assertEquals(32, meta.getColumnDisplaySize(7));
    assertEquals(32, meta.getColumnDisplaySize(8));
    assertEquals(2, meta.getColumnDisplaySize(9));
    assertEquals(32, meta.getColumnDisplaySize(10));
    assertEquals(32, meta.getColumnDisplaySize(11));
    assertEquals(32, meta.getColumnDisplaySize(12));
    assertEquals(16, meta.getColumnDisplaySize(13));
    assertEquals(32, meta.getColumnDisplaySize(14));

    for (int i = 1; i <= meta.getColumnCount(); i++) {
      assertFalse(meta.isAutoIncrement(i));
      assertFalse(meta.isCurrency(i));
      assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(i));

      int expectedPrecision = i == 3 ? -1 : 0;
      int expectedScale = i == 3 ? -1 : 0;
      assertEquals(expectedPrecision, meta.getPrecision(i));
      assertEquals(expectedScale, meta.getScale(i));
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
}
