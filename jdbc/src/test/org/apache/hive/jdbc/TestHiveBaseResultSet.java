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

import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.service.cli.TableSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test suite for {@link HiveBaseResultSet} class.
 */
public class TestHiveBaseResultSet {

  @Test
  public void testGetStringString() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("varchar(64)");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {"ABC"};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getString(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals("ABC", resultSet.getString(1));
    Assert.assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetStringByteArray() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("varchar(64)");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {"ABC".getBytes(StandardCharsets.UTF_8)};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getString(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals("ABC", resultSet.getString(1));
    Assert.assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetStringNull() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("varchar(64)");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {null};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getString(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertNull(resultSet.getString(1));
    Assert.assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetBooleanBoolean() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("boolean");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {Boolean.TRUE};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getBoolean(1)).thenCallRealMethod();
    when(resultSet.getObject(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals(true, resultSet.getBoolean(1));
    Assert.assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetBooleanNull() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("boolean");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {null};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getBoolean(1)).thenCallRealMethod();
    when(resultSet.getObject(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals(false, resultSet.getBoolean(1));
    Assert.assertTrue(resultSet.wasNull());
  }

  /**
   * If the designated column has a datatype of BIT, TINYINT, SMALLINT, INTEGER
   * or BIGINT and contains a 0, a value of false is returned.
   *
   * @see ResultSet#getBoolean(int)
   */
  @Test
  public void testGetBooleanNumberFalse() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("int");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {new Integer(0)};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getBoolean(1)).thenCallRealMethod();
    when(resultSet.getObject(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals(false, resultSet.getBoolean(1));
    Assert.assertFalse(resultSet.wasNull());
  }

  /**
   * If the designated column has a datatype of BIT, TINYINT, SMALLINT, INTEGER
   * or BIGINT and contains a 0, a value of false is returned or true otherwise.
   *
   * @see ResultSet#getBoolean(int)
   */
  @Test
  public void testGetBooleanNumberTrue() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("int");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {new Integer(3)};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getBoolean(1)).thenCallRealMethod();
    when(resultSet.getObject(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals(true, resultSet.getBoolean(1));
    Assert.assertFalse(resultSet.wasNull());
  }

  /**
   * If the designated column has a datatype of CHAR or VARCHAR and contains a
   * "0" a value of false is returned.
   *
   * @see ResultSet#getBoolean(int)
   */
  @Test
  public void testGetBooleanStringFalse() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("varchar(16)");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {"0"};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getBoolean(1)).thenCallRealMethod();
    when(resultSet.getObject(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals(false, resultSet.getBoolean(1));
    Assert.assertFalse(resultSet.wasNull());
  }

  /**
   * If the designated column has a datatype of CHAR or VARCHAR and contains a
   * "1" a value of true is returned.
   *
   * @see ResultSet#getBoolean(int)
   */
  @Test
  public void testGetBooleanStringTrue() throws SQLException {
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setType("varchar(16)");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] {"1"};

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.getBoolean(1)).thenCallRealMethod();
    when(resultSet.getObject(1)).thenCallRealMethod();
    when(resultSet.wasNull()).thenCallRealMethod();

    Assert.assertEquals(true, resultSet.getBoolean(1));
    Assert.assertFalse(resultSet.wasNull());
  }

}
