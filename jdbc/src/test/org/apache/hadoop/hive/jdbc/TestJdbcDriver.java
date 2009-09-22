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

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

public class TestJdbcDriver extends TestCase {
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static String tableName = "testHiveDriverTable";
  private static String partitionedTableName = "testHiveDriverPartitionedTable";
  private HiveConf conf;
  private Path dataFilePath;
  private Connection con;
  private boolean standAloneServer = false;

  public TestJdbcDriver(String name) {
    super(name);
    conf = new HiveConf(TestJdbcDriver.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    standAloneServer = "true".equals(System.getProperty("test.service.standalone.server"));
  }

  protected void setUp() throws Exception {
    super.setUp();
    Class.forName(driverName);
    if (standAloneServer) {
      // get connection
      con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    }
    else {
      con = DriverManager.getConnection("jdbc:hive://", "", "");
    }
    assertNotNull("Connection is null", con);
    assertFalse("Connection should not be closed", con.isClosed());
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    // drop table. ignore error.
    try {
      stmt.executeQuery("drop table " + tableName);
    } catch (Exception ex) {
    }

    // create table
    ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    assertFalse(res.next());

    // also initialize a paritioned table to test against.

    // drop table. ignore error.
    try {
      stmt.executeQuery("drop table " + partitionedTableName);
    } catch (Exception ex) {
    }

    res = stmt.executeQuery("create table " + partitionedTableName +
                            " (key int, value string) partitioned by (dt STRING)");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '" + dataFilePath.toString() +
                            "' into table " + partitionedTableName +
                            " PARTITION (dt='20090619')");
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

    con.close();
    assertTrue("Connection should be closed", con.isClosed());

    Exception expectedException = null;
    try {
      con.createStatement();
    }
    catch(Exception e) {
      expectedException = e;
    }
    
    assertNotNull("createStatement() on closed connection should throw exception",
                  expectedException);
  }

  public final void testSelectAll() throws Exception {
    doTestSelectAll(this.tableName, -1); // tests not setting maxRows  (return all)
    doTestSelectAll(this.tableName, 0);  // tests setting maxRows to 0 (return all)
  }

  public final void testSelectAllPartioned() throws Exception {
    doTestSelectAll(this.partitionedTableName, -1); // tests not setting maxRows  (return all)
    doTestSelectAll(this.partitionedTableName, 0);  // tests setting maxRows to 0 (return all)
  }

  public final void testSelectAllMaxRows() throws Exception {
    doTestSelectAll(this.tableName, 100);
  }

  private final void doTestSelectAll(String tableName, int maxRows) throws Exception {
    Statement stmt = con.createStatement();
    if (maxRows >= 0) stmt.setMaxRows(maxRows);

    //JDBC says that 0 means return all, which is the default
    int expectedMaxRows = maxRows < 1 ? 0 : maxRows;

    assertNotNull("Statement is null", stmt);
    assertEquals("Statement max rows not as expected", expectedMaxRows, stmt.getMaxRows());
    assertFalse("Statement should not be closed", stmt.isClosed());

    ResultSet res;

    // run some queries
    res = stmt.executeQuery("select * from " + tableName);
    assertNotNull("ResultSet is null", res);
    assertTrue("getResultSet() not returning expected ResultSet", res == stmt.getResultSet());
    assertEquals("get update count not as expected", 0, stmt.getUpdateCount());
    int i = 0;

    boolean moreRow = res.next();
    while (moreRow) {
      try {
        i++;
        res.getInt(1);
        res.getString(1);
        res.getString(2);
        assertFalse("Last result value was not null", res.wasNull());
        assertNull("No warnings should be found on ResultSet", res.getWarnings());
        res.clearWarnings(); //verifying that method is supported
        
        //System.out.println(res.getString(1) + " " + res.getString(2));
        assertEquals("getInt and getString don't align for the same result value",
                String.valueOf(res.getInt(1)), res.getString(1));
        assertEquals("Unexpected result found",
                "val_" + res.getString(1), res.getString(2));
        moreRow = res.next();
      }
      catch (SQLException e) {
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
    stmt.clearWarnings(); //verifying that method is supported

    assertNull("No warnings should be found on connection", con.getWarnings());
    con.clearWarnings(); //verifying that method is supported

    stmt.close();
    assertTrue("Statement should be closed", stmt.isClosed());
  }

  public void testErrorMessages() throws SQLException {
    String invalidSyntaxSQLState = "42000";
    int parseErrorCode = 10;

    //These tests inherently cause exceptions to be written to the test output
    //logs. This is undesirable, since you it might appear to someone looking
    //at the test output logs as if something is failing when it isn't. Not sure
    //how to get around that.
    doTestErrorCase("SELECTT * FROM " + tableName,
            "cannot recognize input 'SELECTT'",
            invalidSyntaxSQLState, 11);
    doTestErrorCase("SELECT * FROM some_table_that_does_not_exist",
            "Table not found", "42S02", parseErrorCode);
    doTestErrorCase("drop table some_table_that_does_not_exist",
            "Table not found", "42S02", parseErrorCode);
    doTestErrorCase("SELECT invalid_column FROM " + tableName,
            "Invalid Table Alias or Column Reference",
            invalidSyntaxSQLState, parseErrorCode);
    doTestErrorCase("SELECT invalid_function(key) FROM " + tableName,
            "Invalid Function", invalidSyntaxSQLState, parseErrorCode);

    //TODO: execute errors like this currently don't return good messages (i.e.
    //'Table already exists'). This is because the Driver class calls
    //Task.executeTask() which swallows meaningful exceptions and returns a status
    //code. This should be refactored.
    doTestErrorCase("create table " + tableName + " (key int, value string)",
            "Query returned non-zero code: 9, cause: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask",
            "08S01", 9);
  }

  private void doTestErrorCase(String sql, String expectedMessage,
                                           String expectedSQLState,
                                           int expectedErrorCode) throws SQLException {
    Statement stmt = con.createStatement();
    boolean exceptionFound = false;
    try {
      stmt.executeQuery(sql);
    }
    catch(SQLException e) {
      assertTrue("Adequate error messaging not found for '" + sql + "': " +
              e.getMessage(), e.getMessage().contains(expectedMessage));
      assertEquals("Expected SQLState not found for '" + sql + "'",
              expectedSQLState, e.getSQLState());
      assertEquals("Expected error code not found for '" + sql + "'",
              expectedErrorCode, e.getErrorCode());
      exceptionFound = true;
    }

    assertNotNull("Exception should have been thrown for query: " + sql, exceptionFound);
  }

  public void testShowTables() throws SQLException {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res = stmt.executeQuery("show tables");

    boolean testTableExists = false;
    while (res.next()) {
      assertNotNull("table name is null in result set", res.getString(1));
      if (tableName.equalsIgnoreCase(res.getString(1))) testTableExists = true;
    }

    assertTrue("table name " + tableName + " not found in SHOW TABLES result set",
               testTableExists);
  }

  public void testDescribeTable() throws SQLException {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res = stmt.executeQuery("describe " + tableName);

    res.next();
    assertEquals("Column name 'key' not found", "key", res.getString(1));
    assertEquals("Column type 'int' for column key not found",
                "int", res.getString(2));
    res.next();
    assertEquals("Column name 'value' not found", "value", res.getString(1));
    assertEquals("Column type 'string' for column key not found",
                "string", res.getString(2));

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
  }

  public void testResultSetMetaData() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet res = stmt.executeQuery("drop table " + tableName);

    //creating a table with tinyint is failing currently so not including
    res = stmt.executeQuery("create table " + tableName + " (a string, b boolean, c bigint, d int, f double)");
    res = stmt.executeQuery("select * from " + tableName + " limit 1");

    ResultSetMetaData meta = res.getMetaData();
    assertEquals("Unexpected column type", Types.VARCHAR, meta.getColumnType(1));
    assertEquals("Unexpected column type", Types.BOOLEAN, meta.getColumnType(2));
    assertEquals("Unexpected column type", Types.BIGINT,  meta.getColumnType(3));
    assertEquals("Unexpected column type", Types.INTEGER, meta.getColumnType(4));
    assertEquals("Unexpected column type", Types.DOUBLE,  meta.getColumnType(5));
    assertEquals("Unexpected column type name", "string", meta.getColumnTypeName(1));
    assertEquals("Unexpected column type name", "boolean",   meta.getColumnTypeName(2));
    assertEquals("Unexpected column type name", "bigint",    meta.getColumnTypeName(3));
    assertEquals("Unexpected column type name", "int",    meta.getColumnTypeName(4));
    assertEquals("Unexpected column type name", "double", meta.getColumnTypeName(5));
    assertEquals("Unexpected column display size", 32, meta.getColumnDisplaySize(1));
    assertEquals("Unexpected column display size", 8,  meta.getColumnDisplaySize(2));
    assertEquals("Unexpected column display size", 32, meta.getColumnDisplaySize(3));
    assertEquals("Unexpected column display size", 16, meta.getColumnDisplaySize(4));
    assertEquals("Unexpected column display size", 16, meta.getColumnDisplaySize(5));

    for (int i = 1; i <= 5; i++) {
      assertFalse(meta.isAutoIncrement(i));
      assertFalse(meta.isCurrency(i));
      assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(i));

      int expectedPrecision = i == 5 ? -1 : 0;
      int expectedScale     = i == 5 ? -1 : 0;
      assertEquals("Unexpected precision", expectedPrecision, meta.getPrecision(i));
      assertEquals("Unexpected scale", expectedScale, meta.getScale(i));
    }
  }

  // [url] [host] [port] [db]
  private static final String[][] URL_PROPERTIES = new String[][] {
          {"jdbc:hive://", "", "", "default"},
          {"jdbc:hive://localhost:10001/default", "localhost", "10001", "default"},
          {"jdbc:hive://localhost/notdefault", "localhost", "10000", "notdefault"},
          {"jdbc:hive://foo:1243", "foo", "1243", "default"}
  };
  
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

  private static void assertDpi(DriverPropertyInfo dpi, String name, String value) {
    assertEquals("Invalid DriverPropertyInfo name", name, dpi.name);
    assertEquals("Invalid DriverPropertyInfo value", value, dpi.value);
    assertEquals("Invalid DriverPropertyInfo required", false, dpi.required);
  }
}
