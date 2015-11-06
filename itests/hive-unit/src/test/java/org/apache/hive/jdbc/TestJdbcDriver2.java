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

import static org.apache.hadoop.hive.conf.SystemVariables.SET_COLUMN_NAME;
import static org.apache.hadoop.hive.ql.exec.ExplainTask.EXPL_COLUMN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.processors.DfsProcessor;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.service.cli.operation.ClassicTableTypeMapping;
import org.apache.hive.service.cli.operation.ClassicTableTypeMapping.ClassicTableTypes;
import org.apache.hive.service.cli.operation.HiveTableTypeMapping;
import org.apache.hive.service.cli.operation.TableTypeMappingFactory.TableTypeMappings;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * TestJdbcDriver2
 *
 */
public class TestJdbcDriver2 {
  private static final Log LOG = LogFactory.getLog(TestJdbcDriver2.class);
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final String tableName = "testHiveJdbcDriver_Table";
  private static final String tableComment = "Simple table";
  private static final String viewName = "testHiveJdbcDriverView";
  private static final String viewComment = "Simple view";
  private static final String partitionedTableName = "testHiveJdbcDriverPartitionedTable";
  private static final String partitionedColumnName = "partcolabc";
  private static final String partitionedColumnValue = "20090619";
  private static final String partitionedTableComment = "Partitioned table";
  private static final String dataTypeTableName = "testdatatypetable";
  private static final String dataTypeTableComment = "Table with many column data types";
  private final HiveConf conf;
  public static String dataFileDir;
  private final Path dataFilePath;
  private final Path dataTypeDataFilePath;
  private Connection con;
  private static boolean standAloneServer = false;
  private static final float floatCompareDelta = 0.0001f;

  public TestJdbcDriver2() {
    conf = new HiveConf(TestJdbcDriver2.class);
    dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    dataTypeDataFilePath = new Path(dataFileDir, "datatypes.txt");
    standAloneServer = "true".equals(System
        .getProperty("test.service.standalone.server"));
  }

  @BeforeClass
  public static void setUpBeforeClass() throws SQLException, ClassNotFoundException{
    Class.forName(driverName);
    Connection con1 = getConnection("default");
    System.setProperty(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "verbose");

    Statement stmt1 = con1.createStatement();
    assertNotNull("Statement is null", stmt1);

    stmt1.execute("set hive.support.concurrency = false");

    DatabaseMetaData metadata = con1.getMetaData();

    // Drop databases created by other test cases
    ResultSet databaseRes = metadata.getSchemas();

    while(databaseRes.next()){
      String db = databaseRes.getString(1);
      if(!db.equals("default")){
        System.err.println("Dropping database " + db);
        stmt1.execute("DROP DATABASE " + db + " CASCADE");
      }
    }
    stmt1.close();
    con1.close();
  }

  @Before
  public void setUp() throws Exception {
    con = getConnection("default");

    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    stmt.execute("set hive.support.concurrency = false");

    // drop table. ignore error.
    try {
      stmt.execute("drop table " + tableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    // create table
    stmt.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string) comment '"
        + tableComment + "'");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);

    // also initialize a paritioned table to test against.

    // drop table. ignore error.
    try {
      stmt.execute("drop table " + partitionedTableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    stmt.execute("create table " + partitionedTableName
        + " (under_col int, value string) comment '"+partitionedTableComment
        +"' partitioned by (" + partitionedColumnName + " STRING)");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + partitionedTableName
        + " PARTITION (" + partitionedColumnName + "="
        + partitionedColumnValue + ")");

    // drop table. ignore error.
    try {
      stmt.execute("drop table " + dataTypeTableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    stmt.execute("create table " + dataTypeTableName
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
        + ") comment'" + dataTypeTableComment
        +"' partitioned by (dt STRING)");

    stmt.execute("load data local inpath '"
        + dataTypeDataFilePath.toString() + "' into table " + dataTypeTableName
        + " PARTITION (dt='20090619')");

    // drop view. ignore error.
    try {
      stmt.execute("drop view " + viewName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    // create view
    stmt.execute("create view " + viewName + " comment '"+viewComment
        +"' as select * from "+ tableName);
  }

  private static Connection getConnection(String postfix) throws SQLException {
    Connection con1;
    if (standAloneServer) {
      // get connection
      con1 = DriverManager.getConnection("jdbc:hive2://localhost:10000/" + postfix,
          "", "");
    } else {
      con1 = DriverManager.getConnection("jdbc:hive2:///" + postfix, "", "");
    }
    assertNotNull("Connection is null", con1);
    assertFalse("Connection should not be closed", con1.isClosed());

    return con1;
  }

  @After
  public void tearDown() throws Exception {
    // drop table
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);
    stmt.execute("drop table " + tableName);
    stmt.execute("drop table " + partitionedTableName);
    stmt.execute("drop table " + dataTypeTableName);

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

  @Test
  public void testBadURL() throws Exception {
    checkBadUrl("jdbc:hive2://localhost:10000;principal=test");
    checkBadUrl("jdbc:hive2://localhost:10000;" +
        "principal=hive/HiveServer2Host@YOUR-REALM.COM");
    checkBadUrl("jdbc:hive2://localhost:10000test");
  }

  private void checkBadUrl(String url) throws SQLException {
    try{
      DriverManager.getConnection(url, "", "");
      fail("Should have thrown JdbcUriParseException but did not ");
    } catch(JdbcUriParseException e) {
      assertTrue(e.getMessage().contains("Bad URL format"));
    }
  }

  @Test
  public void testParentReferences() throws Exception {
    /* Test parent references from Statement */
    Statement s = this.con.createStatement();
    ResultSet rs = s.executeQuery("SELECT * FROM " + dataTypeTableName);

    assertTrue(s.getConnection() == this.con);
    assertTrue(rs.getStatement() == s);

    rs.close();
    s.close();

    /* Test parent references from PreparedStatement */
    PreparedStatement ps = this.con.prepareStatement("SELECT * FROM " + dataTypeTableName);
    rs = ps.executeQuery();

    assertTrue(ps.getConnection() == this.con);
    assertTrue(rs.getStatement() == ps);

    rs.close();
    ps.close();

    /* Test DatabaseMetaData queries which do not have a parent Statement */
    DatabaseMetaData md = this.con.getMetaData();

    assertTrue(md.getConnection() == this.con);

    rs = md.getCatalogs();
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getColumns(null, null, null, null);
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getFunctions(null, null, null);
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getImportedKeys(null, null, null);
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getPrimaryKeys(null, null, null);
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getProcedureColumns(null, null, null, null);
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getProcedures(null, null, null);
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getSchemas();
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getTableTypes();
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getTables(null, null, null, null);
    assertNull(rs.getStatement());
    rs.close();

    rs = md.getTypeInfo();
    assertNull(rs.getStatement());
    rs.close();
  }

  @Test
  public void testDataTypes2() throws Exception {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "select c5, c1 from " + dataTypeTableName + " order by c1");
    ResultSetMetaData meta = res.getMetaData();

    // row 1
    assertTrue(res.next());
    // skip the last (partitioning) column since it is always non-null
    for (int i = 1; i < meta.getColumnCount(); i++) {
      assertNull(res.getObject(i));
    }
  }

  @Test
  public void testErrorDiag() throws SQLException {
    Statement stmt = con.createStatement();
    // verify syntax error
    try {
      stmt.executeQuery("select from " + dataTypeTableName);
      fail("SQLException is expected");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    // verify table not fuond error
    try {
      stmt.executeQuery("select * from nonTable");
      fail("SQLException is expected");
    } catch (SQLException e) {
      assertEquals("42S02", e.getSQLState());
    }

    // verify invalid column error
    try {
      stmt.executeQuery("select zzzz from " + dataTypeTableName);
      fail("SQLException is expected");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
  }

  /**
   * verify 'explain ...' resultset
   * @throws SQLException
   */
  @Test
  public void testExplainStmt() throws SQLException {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "explain select c1, c2, c3, c4, c5 as a, c6, c7, c8, c9, c10, c11, c12, " +
            "c1*2, sentences(null, null, null) as b, c23 from " + dataTypeTableName + " limit 1");

    ResultSetMetaData md = res.getMetaData();
    // only one result column
    assertEquals(md.getColumnCount(), 1);
    // verify the column name
    assertEquals(md.getColumnLabel(1), EXPL_COLUMN_NAME);
    //verify that there is data in the resultset
    assertTrue("Nothing returned explain", res.next());
  }

  @Test
  public void testPrepareStatement() {

    String sql = "from (select count(1) from "
        + tableName
        + " where   'not?param?not?param' <> 'not_param??not_param' and ?=? "
        + " and 1=? and 2=? and 3.0=? and 4.0=? and 'test\\'string\"'=? and 5=? and ?=? "
        + " and date '2012-01-01' = date ?"
        + " ) t  select '2011-03-25' ddate,'China',true bv, 10 num limit 10";

    ///////////////////////////////////////////////
    //////////////////// correct testcase
    //////////////////// executed twice: once with the typed ps setters, once with the generic setObject
    //////////////////////////////////////////////
    try {
      PreparedStatement ps = createPreapredStatementUsingSetXXX(sql);
      ResultSet res = ps.executeQuery();
      assertPreparedStatementResultAsExpected(res);
      ps.close();

      ps = createPreapredStatementUsingSetObject(sql);
      res = ps.executeQuery();
      assertPreparedStatementResultAsExpected(res);
      ps.close();

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

    // setObject to the yet unknown type java.util.Date
    expectedException = null;
    try {
      PreparedStatement ps = con.prepareStatement(sql);
      ps.setObject(1, new Date());
      ps.executeQuery();
    } catch (Exception e) {
      expectedException = e;
    }
    assertNotNull(
        "Setting to an unknown type should throw an exception",
        expectedException);

  }

  private PreparedStatement createPreapredStatementUsingSetObject(String sql) throws SQLException {
    PreparedStatement ps = con.prepareStatement(sql);

    ps.setObject(1, true); //setBoolean
    ps.setObject(2, true); //setBoolean

    ps.setObject(3, Short.valueOf("1")); //setShort
    ps.setObject(4, 2); //setInt
    ps.setObject(5, 3f); //setFloat
    ps.setObject(6, Double.valueOf(4)); //setDouble
    ps.setObject(7, "test'string\""); //setString
    ps.setObject(8, 5L); //setLong
    ps.setObject(9, (byte) 1); //setByte
    ps.setObject(10, (byte) 1); //setByte
    ps.setString(11, "2012-01-01"); //setString

    ps.setMaxRows(2);
    return ps;
  }

  private PreparedStatement createPreapredStatementUsingSetXXX(String sql) throws SQLException {
    PreparedStatement ps = con.prepareStatement(sql);

    ps.setBoolean(1, true); //setBoolean
    ps.setBoolean(2, true); //setBoolean

    ps.setShort(3, Short.valueOf("1")); //setShort
    ps.setInt(4, 2); //setInt
    ps.setFloat(5, 3f); //setFloat
    ps.setDouble(6, Double.valueOf(4)); //setDouble
    ps.setString(7, "test'string\""); //setString
    ps.setLong(8, 5L); //setLong
    ps.setByte(9, (byte) 1); //setByte
    ps.setByte(10, (byte) 1); //setByte
    ps.setString(11, "2012-01-01"); //setString

    ps.setMaxRows(2);
    return ps;
  }

  private void assertPreparedStatementResultAsExpected(ResultSet res ) throws SQLException {
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
  }

  /**
   * Execute non-select statements using execute() and executeUpdated() APIs
   * of PreparedStatement interface
   * @throws Exception
   */
  @Test
  public void testExecutePreparedStatement() throws Exception {
    String key = "testKey";
    String val1 = "val1";
    String val2 = "val2";
    PreparedStatement ps = con.prepareStatement("set " + key + " = ?");

    // execute() of Prepared statement
    ps.setString(1, val1);
    ps.execute();
    verifyConfValue(con, key, val1);

    // executeUpdate() of Prepared statement
    ps.clearParameters();
    ps.setString(1, val2);
    ps.executeUpdate();
    verifyConfValue(con, key, val2);
  }

  @Test
  public void testSetOnConnection() throws Exception {
    Connection connection = getConnection("test?conf1=conf2;conf3=conf4#var1=var2;var3=var4");
    try {
      verifyConfValue(connection, "conf1", "conf2");
      verifyConfValue(connection, "conf3", "conf4");
      verifyConfValue(connection, "var1", "var2");
      verifyConfValue(connection, "var3", "var4");
    } catch (Exception e) {
      connection.close();
    }
  }

  /**
   * Execute "set x" and extract value from key=val format result
   * Verify the extracted value
   * @param key
   * @param expectedVal
   * @throws Exception
   */
  private void verifyConfValue(Connection con, String key, String expectedVal) throws Exception {
    Statement stmt = con.createStatement();
    ResultSet res = stmt.executeQuery("set " + key);
    assertTrue(res.next());
    String value = res.getString(1);
    String resultValues[] = value.split("="); // "key = 'val'"
    assertEquals("Result not in key = val format: " + value, 2, resultValues.length);
    if (resultValues[1].startsWith("'") && resultValues[1].endsWith("'")) {
      resultValues[1] = resultValues[1].substring(1, resultValues[1].length() -1); // remove '
    }
    assertEquals("Conf value should be set by execute()", expectedVal, resultValues[1]);
  }

  @Test
  public final void testSelectAll() throws Exception {
    doTestSelectAll(tableName, -1, -1); // tests not setting maxRows (return all)
    doTestSelectAll(tableName, 0, -1); // tests setting maxRows to 0 (return all)
  }

  @Test
  public final void testSelectAllFromView() throws Exception {
    doTestSelectAll(viewName, -1, -1); // tests not setting maxRows (return all)
    doTestSelectAll(viewName, 0, -1); // tests setting maxRows to 0 (return all)
  }

  @Test
  public final void testSelectAllPartioned() throws Exception {
    doTestSelectAll(partitionedTableName, -1, -1); // tests not setting maxRows
    // (return all)
    doTestSelectAll(partitionedTableName, 0, -1); // tests setting maxRows to 0
    // (return all)
  }

  @Test
  public final void testSelectAllMaxRows() throws Exception {
    doTestSelectAll(tableName, 100, -1);
  }

  @Test
  public final void testSelectAllFetchSize() throws Exception {
    doTestSelectAll(tableName, 100, 20);
  }

  @Test
  public void testNullType() throws Exception {
    Statement stmt = con.createStatement();
    try {
      ResultSet res = stmt.executeQuery("select null from " + dataTypeTableName);
      assertTrue(res.next());
      assertNull(res.getObject(1));
    } finally {
      stmt.close();
    }
  }

  // executeQuery should always throw a SQLException,
  // when it executes a non-ResultSet query (like create)
  @Test
  public void testExecuteQueryException() throws Exception {
    Statement stmt = con.createStatement();
    try {
      stmt.executeQuery("create table test_t2 (under_col int, value string)");
      fail("Expecting SQLException");
    }
    catch (SQLException e) {
      System.out.println("Caught an expected SQLException: " + e.getMessage());
    }
    finally {
      stmt.close();
    }
  }

  private void checkResultSetExpected(Statement stmt, List<String> setupQueries, String testQuery,
      boolean isExpectedResultSet) throws Exception {
    boolean hasResultSet;
    // execute the setup queries
    for(String setupQuery: setupQueries) {
      try {
        stmt.execute(setupQuery);
      } catch (Exception e) {
        failWithExceptionMsg(e);
      }
    }
    // execute the test query
    try {
      hasResultSet = stmt.execute(testQuery);
      assertEquals(hasResultSet, isExpectedResultSet);
    }
    catch(Exception e) {
      failWithExceptionMsg(e);
    }
  }

  private void failWithExceptionMsg(Exception e) {
    e.printStackTrace();
    fail(e.toString());
  }

  @Test
  public void testNullResultSet() throws Exception {
    List<String> setupQueries = new ArrayList<String>();
    String testQuery;
    Statement stmt = con.createStatement();

    // -select- should return a ResultSet
    try {
      stmt.executeQuery("select * from " + tableName);
      System.out.println("select: success");
    } catch(SQLException e) {
      failWithExceptionMsg(e);
    }

    // -create- should not return a ResultSet
    setupQueries.add("drop table test_t1");
    testQuery = "create table test_t1 (under_col int, value string)";
    checkResultSetExpected(stmt, setupQueries, testQuery, false);
    setupQueries.clear();

    // -create table as select- should not return a ResultSet
    setupQueries.add("drop table test_t1");
    testQuery = "create table test_t1 as select * from " + tableName;
    checkResultSetExpected(stmt, setupQueries, testQuery, false);
    setupQueries.clear();

    // -insert table as select- should not return a ResultSet
    setupQueries.add("drop table test_t1");
    setupQueries.add("create table test_t1 (under_col int, value string)");
    testQuery = "insert into table test_t1 select under_col, value from "  + tableName;
    checkResultSetExpected(stmt, setupQueries, testQuery, false);
    setupQueries.clear();

    stmt.close();
  }

  @Test
  public void testCloseResultSet() throws Exception {
    Statement stmt = con.createStatement();

    // execute query, ignore exception if any
    ResultSet res = stmt.executeQuery("select * from " + tableName);
    // close ResultSet, ignore exception if any
    res.close();
    // A statement should be open even after ResultSet#close
    assertFalse(stmt.isClosed());
    // A Statement#cancel after ResultSet#close should be a no-op
    try {
      stmt.cancel();
    } catch(SQLException e) {
      failWithExceptionMsg(e);
    }
    stmt.close();

    stmt = con.createStatement();
    // execute query, ignore exception if any
    res = stmt.executeQuery("select * from " + tableName);
    // close ResultSet, ignore exception if any
    res.close();
    // A Statement#execute after ResultSet#close should be fine too
    try {
      stmt.executeQuery("select * from " + tableName);
    } catch(SQLException e) {
      failWithExceptionMsg(e);
    }
    // A Statement#close after ResultSet#close should close the statement
    stmt.close();
    assertTrue(stmt.isClosed());
  }

  @Test
  public void testDataTypes() throws Exception {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "select * from " + dataTypeTableName + " order by c1");
    ResultSetMetaData meta = res.getMetaData();

    // row 1
    assertTrue(res.next());
    // skip the last (partitioning) column since it is always non-null
    for (int i = 1; i < meta.getColumnCount(); i++) {
      assertNull("Column " + i + " should be null", res.getObject(i));
    }
    // getXXX returns 0 for numeric types, false for boolean and null for other
    assertEquals(0, res.getInt(1));
    assertEquals(false, res.getBoolean(2));
    assertEquals(0d, res.getDouble(3), floatCompareDelta);
    assertEquals(null, res.getString(4));
    assertEquals(null, res.getString(5));
    assertEquals(null, res.getString(6));
    assertEquals(null, res.getString(7));
    assertEquals(null, res.getString(8));
    assertEquals(0, res.getByte(9));
    assertEquals(0, res.getShort(10));
    assertEquals(0f, res.getFloat(11), floatCompareDelta);
    assertEquals(0L, res.getLong(12));
    assertEquals(null, res.getString(13));
    assertEquals(null, res.getString(14));
    assertEquals(null, res.getString(15));
    assertEquals(null, res.getString(16));
    assertEquals(null, res.getString(17));
    assertEquals(null, res.getString(18));
    assertEquals(null, res.getString(19));
    assertEquals(null, res.getString(20));
    assertEquals(null, res.getDate(20));
    assertEquals(null, res.getString(21));
    assertEquals(null, res.getString(22));

    // row 2
    assertTrue(res.next());
    assertEquals(-1, res.getInt(1));
    assertEquals(false, res.getBoolean(2));
    assertEquals(-1.1d, res.getDouble(3), floatCompareDelta);
    assertEquals("", res.getString(4));
    assertEquals("[]", res.getString(5));
    assertEquals("{}", res.getString(6));
    assertEquals("{}", res.getString(7));
    assertEquals("{\"r\":null,\"s\":null,\"t\":null}", res.getString(8));
    assertEquals(-1, res.getByte(9));
    assertEquals(-1, res.getShort(10));
    assertEquals(-1.0f, res.getFloat(11), floatCompareDelta);
    assertEquals(-1, res.getLong(12));
    assertEquals("[]", res.getString(13));
    assertEquals("{}", res.getString(14));
    assertEquals("{\"r\":null,\"s\":null}", res.getString(15));
    assertEquals("[]", res.getString(16));
    assertEquals(null, res.getString(17));
    assertEquals(null, res.getTimestamp(17));
    assertEquals(null, res.getBigDecimal(18));
    assertEquals(null, res.getString(19));
    assertEquals(null, res.getString(20));
    assertEquals(null, res.getDate(20));
    assertEquals(null, res.getString(21));
    assertEquals(null, res.getString(22));
    assertEquals(null, res.getString(23));

    // row 3
    assertTrue(res.next());
    assertEquals(1, res.getInt(1));
    assertEquals(true, res.getBoolean(2));
    assertEquals(1.1d, res.getDouble(3), floatCompareDelta);
    assertEquals("1", res.getString(4));
    assertEquals("[1,2]", res.getString(5));
    assertEquals("{1:\"x\",2:\"y\"}", res.getString(6));
    assertEquals("{\"k\":\"v\"}", res.getString(7));
    assertEquals("{\"r\":\"a\",\"s\":9,\"t\":2.2}", res.getString(8));
    assertEquals(1, res.getByte(9));
    assertEquals(1, res.getShort(10));
    assertEquals(1.0f, res.getFloat(11), floatCompareDelta);
    assertEquals(1, res.getLong(12));
    assertEquals("[[\"a\",\"b\"],[\"c\",\"d\"]]", res.getString(13));
    assertEquals("{1:{11:12,13:14},2:{21:22}}", res.getString(14));
    assertEquals("{\"r\":1,\"s\":{\"a\":2,\"b\":\"x\"}}", res.getString(15));
    assertEquals("[{\"m\":{},\"n\":1},{\"m\":{\"a\":\"b\",\"c\":\"d\"},\"n\":2}]", res.getString(16));
    assertEquals("2012-04-22 09:00:00.123456789", res.getString(17));
    assertEquals("2012-04-22 09:00:00.123456789", res.getTimestamp(17).toString());
    assertEquals("123456789.0123456", res.getBigDecimal(18).toString());
    assertEquals("abcd", res.getString(19));
    assertEquals("2013-01-01", res.getString(20));
    assertEquals("2013-01-01", res.getDate(20).toString());
    assertEquals("abc123", res.getString(21));
    assertEquals("abc123         ", res.getString(22));

    byte[] bytes = "X'01FF'".getBytes("UTF-8");
    InputStream resultSetInputStream = res.getBinaryStream(23);
    int len = bytes.length;
    byte[] b = new byte[len];
    resultSetInputStream.read(b, 0, len);
    for ( int i = 0; i< len; i++) {
      assertEquals(bytes[i], b[i]);
    }

    // test getBoolean rules on non-boolean columns
    assertEquals(true, res.getBoolean(1));
    assertEquals(true, res.getBoolean(4));

    // test case sensitivity
    assertFalse(meta.isCaseSensitive(1));
    assertFalse(meta.isCaseSensitive(2));
    assertFalse(meta.isCaseSensitive(3));
    assertTrue(meta.isCaseSensitive(4));

    // no more rows
    assertFalse(res.next());
  }

  @Test
  public void testIntervalTypes() throws Exception {
    Statement stmt = con.createStatement();

    // Since interval types not currently supported as table columns, need to create them
    // as expressions.
    ResultSet res = stmt.executeQuery(
        "select case when c17 is null then null else interval '1' year end as col1,"
        + " c17 -  c17 as col2 from " + dataTypeTableName + " order by col1");
    ResultSetMetaData meta = res.getMetaData();

    assertEquals("col1", meta.getColumnLabel(1));
    assertEquals(java.sql.Types.OTHER, meta.getColumnType(1));
    assertEquals("interval_year_month", meta.getColumnTypeName(1));
    assertEquals(11, meta.getColumnDisplaySize(1));
    assertEquals(11, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals(HiveIntervalYearMonth.class.getName(), meta.getColumnClassName(1));

    assertEquals("col2", meta.getColumnLabel(2));
    assertEquals(java.sql.Types.OTHER, meta.getColumnType(2));
    assertEquals("interval_day_time", meta.getColumnTypeName(2));
    assertEquals(29, meta.getColumnDisplaySize(2));
    assertEquals(29, meta.getPrecision(2));
    assertEquals(0, meta.getScale(2));
    assertEquals(HiveIntervalDayTime.class.getName(), meta.getColumnClassName(2));

    // row 1 - results should be null
    assertTrue(res.next());
    // skip the last (partitioning) column since it is always non-null
    for (int i = 1; i < meta.getColumnCount(); i++) {
      assertNull("Column " + i + " should be null", res.getObject(i));
    }

    // row 2 - results should be null
    assertTrue(res.next());
    for (int i = 1; i < meta.getColumnCount(); i++) {
      assertNull("Column " + i + " should be null", res.getObject(i));
    }

    // row 3
    assertTrue(res.next());
    assertEquals("1-0", res.getString(1));
    assertEquals(1, ((HiveIntervalYearMonth) res.getObject(1)).getYears());
    assertEquals("0 00:00:00.000000000", res.getString(2));
    assertEquals(0, ((HiveIntervalDayTime) res.getObject(2)).getDays());
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
    assertEquals("get update count not as expected", -1, stmt.getUpdateCount());
    int i = 0;

    ResultSetMetaData meta = res.getMetaData();
    int expectedColCount = isPartitionTable ? 3 : 2;
    assertEquals(
        "Unexpected column count", expectedColCount, meta.getColumnCount());

    boolean moreRow = res.next();
    while (moreRow) {
      try {
        i++;
        assertEquals(res.getInt(1), res.getInt(tableName + ".under_col"));
        assertEquals(res.getInt(1), res.getInt("under_col"));
        assertEquals(res.getString(1), res.getString(tableName + ".under_col"));
        assertEquals(res.getString(1), res.getString("under_col"));
        assertEquals(res.getString(2), res.getString(tableName + ".value"));
        assertEquals(res.getString(2), res.getString("value"));
        if (isPartitionTable) {
          assertEquals(res.getString(3), partitionedColumnValue);
          assertEquals(res.getString(3), res.getString(partitionedColumnName));
          assertEquals(res.getString(3), res.getString(tableName + "."+ partitionedColumnName));
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

  @Test
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
      stmt.execute(sql);
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

  @Test
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

  @Test
  public void testMetaDataGetTables() throws SQLException {
    getTablesTest(ClassicTableTypes.TABLE.toString(), ClassicTableTypes.VIEW.toString());
  }

  @Test
  public  void testMetaDataGetTablesHive() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("set " + HiveConf.ConfVars.HIVE_SERVER2_TABLE_TYPE_MAPPING.varname +
        " = " + TableTypeMappings.HIVE.toString());
    getTablesTest(TableType.MANAGED_TABLE.toString(), TableType.VIRTUAL_VIEW.toString());
  }

  @Test
  public  void testMetaDataGetTablesClassic() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("set " + HiveConf.ConfVars.HIVE_SERVER2_TABLE_TYPE_MAPPING.varname +
        " = " + TableTypeMappings.CLASSIC.toString());
    stmt.close();
    getTablesTest(ClassicTableTypes.TABLE.toString(), ClassicTableTypes.VIEW.toString());
  }

  /**
   * Test the type returned for pre-created table type table and view type
   * table
   * @param tableTypeName expected table type
   * @param viewTypeName expected view type
   * @throws SQLException
   */
  private void getTablesTest(String tableTypeName, String viewTypeName) throws SQLException {
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
      ResultSet rs = con.getMetaData().getTables("default", null, checkPattern, null);
      ResultSetMetaData resMeta = rs.getMetaData();
      assertEquals(5, resMeta.getColumnCount());
      assertEquals("TABLE_CAT", resMeta.getColumnName(1));
      assertEquals("TABLE_SCHEM", resMeta.getColumnName(2));
      assertEquals("TABLE_NAME", resMeta.getColumnName(3));
      assertEquals("TABLE_TYPE", resMeta.getColumnName(4));
      assertEquals("REMARKS", resMeta.getColumnName(5));

      int cnt = 0;
      while (rs.next()) {
        String resultTableName = rs.getString("TABLE_NAME");
        assertEquals("Get by index different from get by name.", rs.getString(3), resultTableName);
        assertEquals("Excpected a different table.", tests.get(checkPattern)[cnt], resultTableName);
        String resultTableComment = rs.getString("REMARKS");
        assertTrue("Missing comment on the table.", resultTableComment.length()>0);
        String tableType = rs.getString("TABLE_TYPE");
        if (resultTableName.endsWith("view")) {
          assertEquals("Expected a tabletype view but got something else.", viewTypeName, tableType);
        } else {
          assertEquals("Expected a tabletype table but got something else.", tableTypeName, tableType);
        }
        cnt++;
      }
      rs.close();
      assertEquals("Received an incorrect number of tables.", tests.get(checkPattern).length, cnt);
    }

    // only ask for the views.
    ResultSet rs = con.getMetaData().getTables("default", null, null
        , new String[]{viewTypeName});
    int cnt=0;
    while (rs.next()) {
      cnt++;
    }
    rs.close();
    assertEquals("Incorrect number of views found.", 1, cnt);
  }

  @Test
  public void testMetaDataGetCatalogs() throws SQLException {
    ResultSet rs = con.getMetaData().getCatalogs();
    ResultSetMetaData resMeta = rs.getMetaData();
    assertEquals(1, resMeta.getColumnCount());
    assertEquals("TABLE_CAT", resMeta.getColumnName(1));

    assertFalse(rs.next());
  }

  @Test
  public void testMetaDataGetSchemas() throws SQLException {
    ResultSet rs = con.getMetaData().getSchemas();
    ResultSetMetaData resMeta = rs.getMetaData();
    assertEquals(2, resMeta.getColumnCount());
    assertEquals("TABLE_SCHEM", resMeta.getColumnName(1));
    assertEquals("TABLE_CATALOG", resMeta.getColumnName(2));

    assertTrue(rs.next());
    assertEquals("default", rs.getString(1));

    assertFalse(rs.next());
    rs.close();
  }

  // test default table types returned in
  // Connection.getMetaData().getTableTypes()
  @Test
  public void testMetaDataGetTableTypes() throws SQLException {
    metaDataGetTableTypeTest(new ClassicTableTypeMapping().getTableTypeNames());
  }

  // test default table types returned in
  // Connection.getMetaData().getTableTypes() when type config is set to "HIVE"
  @Test
  public void testMetaDataGetHiveTableTypes() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("set " + HiveConf.ConfVars.HIVE_SERVER2_TABLE_TYPE_MAPPING.varname +
        " = " + TableTypeMappings.HIVE.toString());
    stmt.close();
    metaDataGetTableTypeTest(new HiveTableTypeMapping().getTableTypeNames());
  }

  // test default table types returned in
  // Connection.getMetaData().getTableTypes() when type config is set to "CLASSIC"
  @Test
  public void testMetaDataGetClassicTableTypes() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("set " + HiveConf.ConfVars.HIVE_SERVER2_TABLE_TYPE_MAPPING.varname +
        " = " + TableTypeMappings.CLASSIC.toString());
    stmt.close();
    metaDataGetTableTypeTest(new ClassicTableTypeMapping().getTableTypeNames());
  }

  /**
   * Test if Connection.getMetaData().getTableTypes() returns expected
   *  tabletypes
   * @param tabletypes expected table types
   * @throws SQLException
   */
  private void metaDataGetTableTypeTest(Set<String> tabletypes)
      throws SQLException {
    ResultSet rs = con.getMetaData().getTableTypes();

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

  @Test
  public void testMetaDataGetColumns() throws SQLException {
    Map<String[], Integer> tests = new HashMap<String[], Integer>();
    tests.put(new String[]{"testhivejdbcdriver\\_table", null}, 2);
    tests.put(new String[]{"testhivejdbc%", null}, 7);
    tests.put(new String[]{"testhiveJDBC%", null}, 7);
    tests.put(new String[]{"%jdbcdriver\\_table", null}, 2);
    tests.put(new String[]{"%jdbcdriver\\_table%", "under\\_col"}, 1);
    //    tests.put(new String[]{"%jdbcdriver\\_table%", "under\\_COL"}, 1);
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
  @Test
  public void testMetaDataGetColumnsMetaData() throws SQLException {
    ResultSet rs = con.getMetaData().getColumns(null, null
        , "testhivejdbcdriver\\_table", null);

    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals("TABLE_CAT", rsmd.getColumnName(1));
    assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
    assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(1));

    assertEquals("ORDINAL_POSITION", rsmd.getColumnName(17));
    assertEquals(Types.INTEGER, rsmd.getColumnType(17));
    assertEquals(11, rsmd.getColumnDisplaySize(17));
  }

  /*
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
   */

  @Test
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

  @Test
  public void testShowColumns() throws SQLException {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res = stmt.executeQuery("show columns in " + tableName);
    res.next();
    assertEquals("Column name 'under_col' not found",
        "under_col", res.getString(1));

    res.next();
    assertEquals("Column name 'value' not found",
        "value", res.getString(1));
    assertFalse("More results found than expected", res.next());
  }

  @Test
  public void testDatabaseMetaData() throws SQLException {
    DatabaseMetaData meta = con.getMetaData();

    assertEquals("Apache Hive", meta.getDatabaseProductName());
    assertEquals(HiveVersionInfo.getVersion(), meta.getDatabaseProductVersion());
    assertEquals(System.getProperty("hive.version"), meta.getDatabaseProductVersion());
    assertTrue("verifying hive version pattern. got " + meta.getDatabaseProductVersion(),
        Pattern.matches("\\d+\\.\\d+\\.\\d+.*", meta.getDatabaseProductVersion()) );

    assertEquals(DatabaseMetaData.sqlStateSQL99, meta.getSQLStateType());
    assertFalse(meta.supportsCatalogsInTableDefinitions());
    assertTrue(meta.supportsSchemasInTableDefinitions());
    assertTrue(meta.supportsSchemasInDataManipulation());
    assertFalse(meta.supportsMultipleResultSets());
    assertFalse(meta.supportsStoredProcedures());
    assertTrue(meta.supportsAlterTableWithAddColumn());

    //-1 indicates malformed version.
    assertTrue(meta.getDatabaseMajorVersion() > -1);
    assertTrue(meta.getDatabaseMinorVersion() > -1);
  }

  @Test
  public void testResultSetColumnNameCaseInsensitive() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet res;

    res = stmt.executeQuery("select c1 from " + dataTypeTableName + " limit 1");
    try {
      int count = 0;
      while (res.next()) {
        res.findColumn("c1");
        res.findColumn("C1");
        count++;
      }
      assertEquals(count, 1);
    } catch (Exception e) {
      String msg = "Unexpected exception: " + e;
      LOG.info(msg, e);
      fail(msg);
    }

    res = stmt.executeQuery("select c1 C1 from " + dataTypeTableName + " limit 1");
    try {
      int count = 0;
      while (res.next()) {
        res.findColumn("c1");
        res.findColumn("C1");
        count++;
      }
      assertEquals(count, 1);
    } catch (Exception e) {
      String msg = "Unexpected exception: " + e;
      LOG.info(msg, e);
      fail(msg);
    }
  }

  @Test
  public void testResultSetMetaData() throws SQLException {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "select c1, c2, c3, c4, c5 as a, c6, c7, c8, c9, c10, c11, c12, " +
            "c1*2, sentences(null, null, null) as b, c17, c18, c20, c21, c22, c23 from " + dataTypeTableName +
        " limit 1");
    ResultSetMetaData meta = res.getMetaData();

    ResultSet colRS = con.getMetaData().getColumns(null, null,
        dataTypeTableName.toLowerCase(), null);

    assertEquals(20, meta.getColumnCount());

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
    assertEquals(Types.ARRAY, meta.getColumnType(5));
    assertEquals("array", meta.getColumnTypeName(5));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(5));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(5));
    assertEquals(0, meta.getScale(5));

    assertEquals("c5", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.ARRAY, colRS.getInt("DATA_TYPE"));
    assertEquals("array<int>", colRS.getString("TYPE_NAME").toLowerCase());

    assertTrue(colRS.next());

    assertEquals("c6", meta.getColumnName(6));
    assertEquals(Types.JAVA_OBJECT, meta.getColumnType(6));
    assertEquals("map", meta.getColumnTypeName(6));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(6));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(6));
    assertEquals(0, meta.getScale(6));

    assertEquals("c6", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.JAVA_OBJECT, colRS.getInt("DATA_TYPE"));
    assertEquals("map<int,string>", colRS.getString("TYPE_NAME").toLowerCase());

    assertTrue(colRS.next());

    assertEquals("c7", meta.getColumnName(7));
    assertEquals(Types.JAVA_OBJECT, meta.getColumnType(7));
    assertEquals("map", meta.getColumnTypeName(7));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(7));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(7));
    assertEquals(0, meta.getScale(7));

    assertEquals("c7", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.JAVA_OBJECT, colRS.getInt("DATA_TYPE"));
    assertEquals("map<string,string>", colRS.getString("TYPE_NAME").toLowerCase());

    assertTrue(colRS.next());

    assertEquals("c8", meta.getColumnName(8));
    assertEquals(Types.STRUCT, meta.getColumnType(8));
    assertEquals("struct", meta.getColumnTypeName(8));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(8));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(8));
    assertEquals(0, meta.getScale(8));

    assertEquals("c8", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.STRUCT, colRS.getInt("DATA_TYPE"));
    assertEquals("struct<r:string,s:int,t:double>", colRS.getString("TYPE_NAME").toLowerCase());

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
    assertEquals(Types.ARRAY, meta.getColumnType(14));
    assertEquals("array", meta.getColumnTypeName(14));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(14));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(14));
    assertEquals(0, meta.getScale(14));

    // Move the result of getColumns() forward to match the columns of the query
    assertTrue(colRS.next());  // c13
    assertTrue(colRS.next());  // c14
    assertTrue(colRS.next());  // c15
    assertTrue(colRS.next());  // c16
    assertTrue(colRS.next());  // c17

    assertEquals("c17", meta.getColumnName(15));
    assertEquals(Types.TIMESTAMP, meta.getColumnType(15));
    assertEquals("timestamp", meta.getColumnTypeName(15));
    assertEquals(29, meta.getColumnDisplaySize(15));
    assertEquals(29, meta.getPrecision(15));
    assertEquals(9, meta.getScale(15));

    assertEquals("c17", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.TIMESTAMP, colRS.getInt("DATA_TYPE"));
    assertEquals("timestamp", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(15), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(15), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c18", meta.getColumnName(16));
    assertEquals(Types.DECIMAL, meta.getColumnType(16));
    assertEquals("decimal", meta.getColumnTypeName(16));
    assertEquals(18, meta.getColumnDisplaySize(16));
    assertEquals(16, meta.getPrecision(16));
    assertEquals(7, meta.getScale(16));

    assertEquals("c18", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.DECIMAL, colRS.getInt("DATA_TYPE"));
    assertEquals("decimal", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(16), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(16), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());  // skip c19, since not selected by query
    assertTrue(colRS.next());

    assertEquals("c20", meta.getColumnName(17));
    assertEquals(Types.DATE, meta.getColumnType(17));
    assertEquals("date", meta.getColumnTypeName(17));
    assertEquals(10, meta.getColumnDisplaySize(17));
    assertEquals(10, meta.getPrecision(17));
    assertEquals(0, meta.getScale(17));

    assertEquals("c20", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.DATE, colRS.getInt("DATA_TYPE"));
    assertEquals("date", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(17), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(17), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c21", meta.getColumnName(18));
    assertEquals(Types.VARCHAR, meta.getColumnType(18));
    assertEquals("varchar", meta.getColumnTypeName(18));
    // varchar columns should have correct display size/precision
    assertEquals(20, meta.getColumnDisplaySize(18));
    assertEquals(20, meta.getPrecision(18));
    assertEquals(0, meta.getScale(18));

    assertEquals("c21", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.VARCHAR, colRS.getInt("DATA_TYPE"));
    assertEquals("varchar", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(18), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(18), colRS.getInt("DECIMAL_DIGITS"));

    assertTrue(colRS.next());

    assertEquals("c22", meta.getColumnName(19));
    assertEquals(Types.CHAR, meta.getColumnType(19));
    assertEquals("char", meta.getColumnTypeName(19));
    // char columns should have correct display size/precision
    assertEquals(15, meta.getColumnDisplaySize(19));
    assertEquals(15, meta.getPrecision(19));
    assertEquals(0, meta.getScale(19));

    assertEquals("c23", meta.getColumnName(20));
    assertEquals(Types.BINARY, meta.getColumnType(20));
    assertEquals("binary", meta.getColumnTypeName(20));
    assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(20));
    assertEquals(Integer.MAX_VALUE, meta.getPrecision(20));
    assertEquals(0, meta.getScale(20));

    assertEquals("c22", colRS.getString("COLUMN_NAME"));
    assertEquals(Types.CHAR, colRS.getInt("DATA_TYPE"));
    assertEquals("char", colRS.getString("TYPE_NAME").toLowerCase());
    assertEquals(meta.getPrecision(19), colRS.getInt("COLUMN_SIZE"));
    assertEquals(meta.getScale(19), colRS.getInt("DECIMAL_DIGITS"));

    for (int i = 1; i <= meta.getColumnCount(); i++) {
      assertFalse(meta.isAutoIncrement(i));
      assertFalse(meta.isCurrency(i));
      assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(i));
    }
  }

  // [url] [host] [port] [db]
  private static final String[][] URL_PROPERTIES = new String[][] {
    // binary mode
    // For embedded mode, the JDBC uri is of the form:
    // jdbc:hive2:///dbName;sess_var_list?hive_conf_list#hive_var_list
    // and does not contain host:port string.
    // As a result port is parsed to '-1' per the Java URI conventions
    {"jdbc:hive2://", "", "", "default"},
    {"jdbc:hive2://localhost:10001/default", "localhost", "10001", "default"},
    {"jdbc:hive2://localhost/notdefault", "localhost", "10000", "notdefault"},
    {"jdbc:hive2://foo:1243", "foo", "1243", "default"},

    // http mode
    {"jdbc:hive2://server:10002/db;user=foo;password=bar?" +
        "hive.server2.transport.mode=http;" +
        "hive.server2.thrift.http.path=hs2",
        "server", "10002", "db"},
  };

  @Test
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

  private static final String[][] HTTP_URL_PROPERTIES = new String[][] {
      { "jdbc:hive2://server:10002/db;user=foo;password=bar;transportMode=http;httpPath=hs2",
          "server", "10002", "db", "http", "hs2" },
      { "jdbc:hive2://server:10000/testdb;user=foo;password=bar;transportMode=binary;httpPath=",
          "server", "10000", "testdb", "binary", "" }, };

@Test
public void testParseUrlHttpMode() throws SQLException, JdbcUriParseException,
    ZooKeeperHiveClientException {
  new HiveDriver();
  for (String[] testValues : HTTP_URL_PROPERTIES) {
    JdbcConnectionParams params = Utils.parseURL(testValues[0]);
    assertEquals(params.getHost(), testValues[1]);
    assertEquals(params.getPort(), Integer.parseInt(testValues[2]));
    assertEquals(params.getDbName(), testValues[3]);
    assertEquals(params.getSessionVars().get("transportMode"), testValues[4]);
    assertEquals(params.getSessionVars().get("httpPath"), testValues[5]);
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
  @Test
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
    int numLines = 0;
    while (res.next()){
      numLines++;
      String rline = res.getString(1);
      assertFalse("set output must not contain hidden variables such as the metastore password:"+rline,
          rline.contains(HiveConf.ConfVars.METASTOREPWD.varname) && !(rline.contains(HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname)));
        // the only conf allowed to have the metastore pwd keyname is the hidden list configuration value
    }
    assertTrue("Nothing returned by set -v", numLines > 0);

    res.close();
    stmt.close();
  }

  /**
   * Validate error on closed resultset
   * @throws SQLException
   */
  @Test
  public void testPostClose() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet res = stmt.executeQuery("select * from " + tableName);
    assertNotNull("ResultSet is null", res);
    res.close();
    try { res.getInt(1); fail("Expected SQLException"); }
    catch (SQLException e) { }
    try { res.getMetaData(); fail("Expected SQLException"); }
    catch (SQLException e) { }
    try { res.setFetchSize(10); fail("Expected SQLException"); }
    catch (SQLException e) { }
  }

  /*
   * The JDBC spec says when you have duplicate column names,
   * the first one should be returned.
   */
  @Test
  public void testDuplicateColumnNameOrder() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS a, 2 AS a from " + tableName);
    assertTrue(rs.next());
    assertEquals(1, rs.getInt("a"));
    rs.close();
  }


  /**
   * Test bad args to getXXX()
   * @throws SQLException
   */
  @Test
  public void testOutOfBoundCols() throws SQLException {
    Statement stmt = con.createStatement();

    ResultSet res = stmt.executeQuery(
        "select * from " + tableName);

    // row 1
    assertTrue(res.next());

    try {
      res.getInt(200);
    } catch (SQLException e) {
    }

    try {
      res.getInt("zzzz");
    } catch (SQLException e) {
    }

  }

  /**
   * Verify selecting using builtin UDFs
   * @throws SQLException
   */
  @Test
  public void testBuiltInUDFCol() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet res = stmt.executeQuery("select c12, bin(c12) from " + dataTypeTableName
        + " where c1=1");
    ResultSetMetaData md = res.getMetaData();
    assertEquals(md.getColumnCount(), 2); // only one result column
    assertEquals(md.getColumnLabel(2), "c1" ); // verify the system generated column name
    assertTrue(res.next());
    assertEquals(res.getLong(1), 1);
    assertEquals(res.getString(2), "1");
    res.close();
  }

  /**
   * Verify selecting named expression columns
   * @throws SQLException
   */
  @Test
  public void testExprCol() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet res = stmt.executeQuery("select c1+1 as col1, length(c4) as len from " + dataTypeTableName
        + " where c1=1");
    ResultSetMetaData md = res.getMetaData();
    assertEquals(md.getColumnCount(), 2); // only one result column
    assertEquals(md.getColumnLabel(1), "col1" ); // verify the column name
    assertEquals(md.getColumnLabel(2), "len" ); // verify the column name
    assertTrue(res.next());
    assertEquals(res.getInt(1), 2);
    assertEquals(res.getInt(2), 1);
    res.close();
  }

  /**
   * test getProcedureColumns()
   * @throws SQLException
   */
  @Test
  public void testProcCols() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    // currently getProcedureColumns always returns an empty resultset for Hive
    ResultSet res = dbmd.getProcedureColumns(null, null, null, null);
    ResultSetMetaData md = res.getMetaData();
    assertEquals(md.getColumnCount(), 20);
    assertFalse(res.next());
  }

  /**
   * test testProccedures()
   * @throws SQLException
   */
  @Test
  public void testProccedures() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    // currently testProccedures always returns an empty resultset for Hive
    ResultSet res = dbmd.getProcedures(null, null, null);
    ResultSetMetaData md = res.getMetaData();
    assertEquals(md.getColumnCount(), 9);
    assertFalse(res.next());
  }

  /**
   * test getPrimaryKeys()
   * @throws SQLException
   */
  @Test
  public void testPrimaryKeys() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    // currently getPrimaryKeys always returns an empty resultset for Hive
    ResultSet res = dbmd.getPrimaryKeys(null, null, null);
    ResultSetMetaData md = res.getMetaData();
    assertEquals(md.getColumnCount(), 6);
    assertFalse(res.next());
  }

  /**
   * test getImportedKeys()
   * @throws SQLException
   */
  @Test
  public void testImportedKeys() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    // currently getImportedKeys always returns an empty resultset for Hive
    ResultSet res = dbmd.getImportedKeys(null, null, null);
    ResultSetMetaData md = res.getMetaData();
    assertEquals(md.getColumnCount(), 14);
    assertFalse(res.next());
  }

  /**
   * If the Driver implementation understands the URL, it will return a Connection object;
   * otherwise it returns null
   */
  @Test
  public void testInvalidURL() throws Exception {
    HiveDriver driver = new HiveDriver();
    Connection conn = driver.connect("jdbc:derby://localhost:10000/default", new Properties());
    assertNull(conn);
  }

  /**
   * Test the cursor repositioning to start of resultset
   * @throws Exception
   */
  @Test
  public void testFetchFirstQuery() throws Exception {
    execFetchFirst("select c4, c1 from " + dataTypeTableName + " order by c1", "c4", false);
    execFetchFirst("select c4, c1 from " + dataTypeTableName + " order by c1", "c4",  true);
  }

  /**
   * Test the cursor repositioning to start of resultset from non-mr query
   * @throws Exception
   */
  @Test
  public void testFetchFirstNonMR() throws Exception {
    execFetchFirst("select * from " + dataTypeTableName, dataTypeTableName.toLowerCase() + "." + "c4", false);
  }

  /**
   *  Test for cursor repositioning to start of resultset for non-sql commands
   * @throws Exception
   */
  @Test
  public void testFetchFirstSetCmds() throws Exception {
    execFetchFirst("set -v", SET_COLUMN_NAME, false);
  }

  /**
   *  Test for cursor repositioning to start of resultset for non-sql commands
   * @throws Exception
   */
  @Test
  public void testFetchFirstDfsCmds() throws Exception {
    String wareHouseDir = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    execFetchFirst("dfs -ls " + wareHouseDir, DfsProcessor.DFS_RESULT_HEADER, false);
  }


  /**
   * Negative Test for cursor repositioning to start of resultset
   * Verify unsupported JDBC resultset attributes
   * @throws Exception
   */
  @Test
  public void testUnsupportedFetchTypes() throws Exception {
    try {
      con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
          ResultSet.CONCUR_READ_ONLY);
      fail("createStatement with TYPE_SCROLL_SENSITIVE should fail");
    } catch(SQLException e) {
      assertEquals("HYC00", e.getSQLState().trim());
    }

    try {
      con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
          ResultSet.CONCUR_UPDATABLE);
      fail("createStatement with CONCUR_UPDATABLE should fail");
    } catch(SQLException e) {
      assertEquals("HYC00", e.getSQLState().trim());
    }
  }

  /**
   * Negative Test for cursor repositioning to start of resultset
   * Verify unsupported JDBC resultset methods
   * @throws Exception
   */
  @Test
  public void testFetchFirstError() throws Exception {
    Statement stmt = con.createStatement();
    ResultSet res = stmt.executeQuery("select * from " + tableName);
    try {
      res.beforeFirst();
      fail("beforeFirst() should fail for normal resultset");
    } catch (SQLException e) {
      assertEquals("Method not supported for TYPE_FORWARD_ONLY resultset", e.getMessage());
    }
  }

  /**
   * Read the results locally. Then reset the read position to start and read the
   * rows again verify that we get the same results next time.
   * @param sqlStmt - SQL statement to execute
   * @param colName - columns name to read
   * @param oneRowOnly -  read and compare only one row from the resultset
   * @throws Exception
   */
  private void execFetchFirst(String sqlStmt, String colName, boolean oneRowOnly)
      throws Exception {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
    ResultSet res = stmt.executeQuery(sqlStmt);

    List<String> results = new ArrayList<String> ();
    assertTrue(res.isBeforeFirst());
    int rowNum = 0;
    while (res.next()) {
      results.add(res.getString(colName));
      assertEquals(++rowNum, res.getRow());
      assertFalse(res.isBeforeFirst());
      if (oneRowOnly) {
        break;
      }
    }
    // reposition at the begining
    res.beforeFirst();
    assertTrue(res.isBeforeFirst());
    rowNum = 0;
    while (res.next()) {
      // compare the results fetched last time
      assertEquals(results.get(rowNum++), res.getString(colName));
      assertEquals(rowNum, res.getRow());
      assertFalse(res.isBeforeFirst());
      if (oneRowOnly) {
        break;
      }
    }
  }

  @Test
  public void testShowGrant() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("grant select on table " + dataTypeTableName + " to user hive_test_user");
    stmt.execute("show grant user hive_test_user on table " + dataTypeTableName);

    ResultSet res = stmt.getResultSet();
    assertTrue(res.next());
    assertEquals("default", res.getString(1));
    assertEquals(dataTypeTableName, res.getString(2));
    assertEquals("", res.getString(3));     // partition
    assertEquals("", res.getString(4));     // column
    assertEquals("hive_test_user", res.getString(5));
    assertEquals("USER", res.getString(6));
    assertEquals("SELECT", res.getString(7));
    assertEquals(false, res.getBoolean(8)); // grant option
    assertEquals(-1, res.getLong(9));
    assertNotNull(res.getString(10));       // grantor
    assertFalse(res.next());
    res.close();
  }

  @Test
  public void testShowRoleGrant() throws SQLException {
    Statement stmt = con.createStatement();

    // drop role. ignore error.
    try {
      stmt.execute("drop role role1");
    } catch (Exception ex) {
      LOG.warn("Ignoring error during drop role: " + ex);
    }

    stmt.execute("create role role1");
    stmt.execute("grant role role1 to user hive_test_user");
    stmt.execute("show role grant user hive_test_user");

    ResultSet res = stmt.getResultSet();
    assertTrue(res.next());
    assertEquals("public", res.getString(1));
    assertTrue(res.next());
    assertEquals("role1", res.getString(1));
    res.close();
  }

  /**
   * Test the cancellation of a query that is running.
   * We spawn 2 threads - one running the query and
   * the other attempting to cancel.
   * We're using a dummy udf to simulate a query,
   * that runs for a sufficiently long time.
   * @throws Exception
   */
  @Test
  public void testQueryCancel() throws Exception {
    String udfName = SleepUDF.class.getName();
    Statement stmt1 = con.createStatement();
    stmt1.execute("create temporary function sleepUDF as '" + udfName + "'");
    stmt1.close();
    final Statement stmt = con.createStatement();
    // Thread executing the query
    Thread tExecute = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("Executing query: ");
          stmt.executeQuery("select sleepUDF(t1.under_col) as u0, t1.under_col as u1, " +
              "t2.under_col as u2 from " + tableName +  "t1 join " + tableName +
              " t2 on t1.under_col = t2.under_col");
          fail("Expecting SQLException");
        } catch (SQLException e) {
          // This thread should throw an exception
          assertNotNull(e);
          System.out.println(e.toString());
        }
      }
    });
    // Thread cancelling the query
    Thread tCancel = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
          System.out.println("Cancelling query: ");
          stmt.cancel();
        } catch (Exception e) {
          // No-op
        }
      }
    });
    tExecute.start();
    tCancel.start();
    tExecute.join();
    tCancel.join();
    stmt.close();
  }

  // A udf which sleeps for 100ms to simulate a long running query
  public static class SleepUDF extends UDF {
    public Integer evaluate(final Integer value) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // No-op
      }
      return value;
    }
  }

  /**
   * Loads data from a table containing non-ascii value column
   * Runs a query and compares the return value
   * @throws Exception
   */
  @Test
  public void testNonAsciiReturnValues() throws Exception {
    String nonAsciiTableName = "nonAsciiTable";
    String nonAsciiString = "Garçu Kôkaku kidôtai";
    Path nonAsciiFilePath = new Path(dataFileDir, "non_ascii_tbl.txt");
    Statement stmt = con.createStatement();
    stmt.execute("set hive.support.concurrency = false");

    // Create table
    stmt.execute("create table " + nonAsciiTableName + " (key int, value string) " +
        "row format delimited fields terminated by '|'");

    // Load data
    stmt.execute("load data local inpath '"
        + nonAsciiFilePath.toString() + "' into table " + nonAsciiTableName);

    ResultSet rs = stmt.executeQuery("select value from " + nonAsciiTableName +  " limit 1");
    while(rs.next()) {
      String resultValue = rs.getString(1);
      assertTrue(resultValue.equalsIgnoreCase(nonAsciiString));
    }

    // Drop table, ignore error.
    try {
      stmt.execute("drop table " + nonAsciiTableName);
    } catch (Exception ex) {
      // no-op
    }
    stmt.close();
  }

  /**
   * Test getting query log method in Jdbc
   * @throws Exception
   */
  @Test
  public void testGetQueryLog() throws Exception {
    // Prepare
    String[] expectedLogs = {
        "Parsing command",
        "Parse Completed",
        "Starting Semantic Analysis",
        "Semantic Analysis Completed",
        "Starting command"
    };
    String sql = "select count(*) from " + tableName;

    // Verify the fetched log (from the beginning of log file)
    HiveStatement stmt = (HiveStatement)con.createStatement();
    assertNotNull("Statement is null", stmt);
    stmt.executeQuery(sql);
    List<String> logs = stmt.getQueryLog(false, 10000);
    stmt.close();
    verifyFetchedLog(logs, expectedLogs);

    // Verify the fetched log (incrementally)
    final HiveStatement statement = (HiveStatement)con.createStatement();
    assertNotNull("Statement is null", statement);
    statement.setFetchSize(10000);
    final List<String> incrementalLogs = new ArrayList<String>();

    Runnable logThread = new Runnable() {
      @Override
      public void run() {
        while (statement.hasMoreLogs()) {
          try {
            incrementalLogs.addAll(statement.getQueryLog());
            Thread.sleep(500);
          } catch (SQLException e) {
            LOG.error("Failed getQueryLog. Error message: " + e.getMessage());
            fail("error in getting log thread");
          } catch (InterruptedException e) {
            LOG.error("Getting log thread is interrupted. Error message: " + e.getMessage());
            fail("error in getting log thread");
          }
        }
      }
    };

    Thread thread = new Thread(logThread);
    thread.setDaemon(true);
    thread.start();
    statement.executeQuery(sql);
    thread.interrupt();
    thread.join(10000);
    // fetch remaining logs
    List<String> remainingLogs;
    do {
      remainingLogs = statement.getQueryLog();
      incrementalLogs.addAll(remainingLogs);
    } while (remainingLogs.size() > 0);
    statement.close();

    verifyFetchedLog(incrementalLogs, expectedLogs);
  }

  private void verifyFetchedLog(List<String> logs, String[] expectedLogs) {
    StringBuilder stringBuilder = new StringBuilder();

    for (String log : logs) {
      stringBuilder.append(log);
    }

    String accumulatedLogs = stringBuilder.toString();
    for (String expectedLog : expectedLogs) {
      assertTrue(accumulatedLogs.contains(expectedLog));
    }
  }
  @Test
  public void testPrepareSetDate() throws Exception {
    try {
      String sql = "select * from " + dataTypeTableName + " where c20 = ?";
      PreparedStatement ps = con.prepareStatement(sql);
      java.sql.Date dtValue = java.sql.Date.valueOf("2013-01-01");
      ps.setDate(1, dtValue);
      ResultSet res = ps.executeQuery();
      assertTrue(res.next());
      assertEquals("2013-01-01", res.getString(20));
      ps.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }
}
