package org.apache.hive.jdbc;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class HiveStatementTest {

  @Test
  public void testSetFetchSize1() throws SQLException {
    HiveStatement stmt = new HiveStatement(null, null, null);
    stmt.setFetchSize(123);
    assertEquals(123, stmt.getFetchSize());
  }

  @Test
  public void testSetFetchSize2() throws SQLException {
    HiveStatement stmt = new HiveStatement(null, null, null);
    int initial = stmt.getFetchSize();
    stmt.setFetchSize(0);
    assertEquals(initial, stmt.getFetchSize());
  }

  @Test(expected = SQLException.class)
  public void testSetFetchSize3() throws SQLException {
    HiveStatement stmt = new HiveStatement(null, null, null);
    stmt.setFetchSize(-1);
  }
}
