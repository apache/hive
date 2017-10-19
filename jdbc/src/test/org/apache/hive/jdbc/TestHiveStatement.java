/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.jdbc;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class TestHiveStatement {

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

  @Test
  public void testaddBatch() throws SQLException {
    HiveStatement stmt = new HiveStatement(null, null, null);
    try {
      stmt.addBatch(null);
    } catch (SQLException e) {
      assertEquals("java.sql.SQLFeatureNotSupportedException: Method not supported", e.toString());
    }
  }
}
