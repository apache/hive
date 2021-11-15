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

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.service.cli.TableSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

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

  @Test
  public void testFindColumnUnqualified() throws Exception {
    FieldSchema fieldSchema1 = new FieldSchema();
    fieldSchema1.setType("int");

    FieldSchema fieldSchema2 = new FieldSchema();
    fieldSchema2.setType("int");

    FieldSchema fieldSchema3 = new FieldSchema();
    fieldSchema3.setType("int");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema1, fieldSchema2, fieldSchema3);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] { new Integer(1), new Integer(2), new Integer(3) };
    resultSet.normalizedColumnNames = Arrays.asList("one", "two", "three");

    Field executorField = HiveBaseResultSet.class.getDeclaredField("columnNameIndexCache");
    FieldSetter.setField(resultSet, executorField, new HashMap<>());

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.findColumn("one")).thenCallRealMethod();
    when(resultSet.findColumn("Two")).thenCallRealMethod();
    when(resultSet.findColumn("THREE")).thenCallRealMethod();

    Assert.assertEquals(1, resultSet.findColumn("one"));
    Assert.assertEquals(2, resultSet.findColumn("Two"));
    Assert.assertEquals(3, resultSet.findColumn("THREE"));
  }

  @Test
  public void testFindColumnQualified() throws Exception {
    FieldSchema fieldSchema1 = new FieldSchema();
    fieldSchema1.setType("int");

    FieldSchema fieldSchema2 = new FieldSchema();
    fieldSchema2.setType("int");

    FieldSchema fieldSchema3 = new FieldSchema();
    fieldSchema3.setType("int");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema1, fieldSchema2, fieldSchema3);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] { new Integer(1), new Integer(2), new Integer(3) };
    resultSet.normalizedColumnNames = Arrays.asList("table.one", "table.two", "table.three");

    Field executorField = HiveBaseResultSet.class.getDeclaredField("columnNameIndexCache");
    FieldSetter.setField(resultSet, executorField, new HashMap<>());

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.findColumn("one")).thenCallRealMethod();
    when(resultSet.findColumn("Two")).thenCallRealMethod();
    when(resultSet.findColumn("THREE")).thenCallRealMethod();

    Assert.assertEquals(1, resultSet.findColumn("one"));
    Assert.assertEquals(2, resultSet.findColumn("Two"));
    Assert.assertEquals(3, resultSet.findColumn("THREE"));
  }

  @Test(expected = SQLException.class)
  public void testFindColumnNull() throws Exception {
    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    when(resultSet.findColumn(null)).thenCallRealMethod();
    Assert.assertEquals(0, resultSet.findColumn(null));
  }

  @Test(expected = SQLException.class)
  public void testFindColumnUnknownColumn() throws Exception {
    FieldSchema fieldSchema1 = new FieldSchema();
    fieldSchema1.setType("int");

    List<FieldSchema> fieldSchemas = Arrays.asList(fieldSchema1);
    TableSchema schema = new TableSchema(fieldSchemas);

    HiveBaseResultSet resultSet = Mockito.mock(HiveBaseResultSet.class);
    resultSet.row = new Object[] { new Integer(1) };
    resultSet.normalizedColumnNames = Arrays.asList("table.one");

    Field executorField = HiveBaseResultSet.class.getDeclaredField("columnNameIndexCache");
    FieldSetter.setField(resultSet, executorField, new HashMap<>());

    when(resultSet.getSchema()).thenReturn(schema);
    when(resultSet.findColumn("zero")).thenCallRealMethod();

    Assert.assertEquals(1, resultSet.findColumn("zero"));
  }

}
