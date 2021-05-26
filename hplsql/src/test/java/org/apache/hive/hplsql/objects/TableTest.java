/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hive.hplsql.objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hive.hplsql.ColumnDefinition;
import org.apache.hive.hplsql.ColumnType;
import org.apache.hive.hplsql.Row;
import org.junit.Test;

public class TableTest {
  private static final TableClass TWO_COLUMN_TABLE = new TableClass("type1", Arrays.asList(
          new ColumnDefinition("col1", ColumnType.parse("STRING")),
          new ColumnDefinition("col2", ColumnType.parse("NUMBER"))),
          true);
  public static final Row ROW_1 = new Row();
  public static final Row ROW_2 = new Row();
  public static final Row ROW_3 = new Row();
  public static final Row ROW_4 = new Row();
  public static final Row ROW_5 = new Row();
  public static final Row ROW_6 = new Row();

  @Test
  public void testLookup() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(10, ROW_1);
    table.put(20, ROW_2);
    assertEquals(ROW_1, table.at(10));
    assertEquals(ROW_2, table.at(20));
  }

  @Test
  public void testNext() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(10, ROW_1);
    table.put(20, ROW_2);
    assertEquals(ROW_1, table.at(10));
    assertEquals(ROW_2, table.at(20));
    assertEquals(20, table.nextKey(10));
  }

  @Test
  public void testPrior() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(10, ROW_1);
    table.put(20, ROW_2);
    assertEquals(ROW_1, table.at(10));
    assertEquals(ROW_2, table.at(20));
    assertEquals(10, table.priorKey(20));
  }

  @Test
  public void testIterationForward() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(100, ROW_1);
    table.put(120, ROW_2);
    table.put(340, ROW_3);
    List<Object> result = new ArrayList<>();
    Object key = table.firstKey();
    while (key != null) {
      result.add(key);
      key = table.nextKey(key);
    }
    assertEquals(Arrays.asList(100, 120, 340), result);
  }

  @Test
  public void testIterationBackward() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(100, ROW_1);
    table.put(120, ROW_2);
    table.put(340, ROW_3);
    List<Object> result = new ArrayList<>();
    Object key = table.lastKey();
    while (key != null) {
      result.add(key);
      key = table.priorKey(key);
    }
    assertEquals(Arrays.asList(340, 120, 100), result);
  }

  @Test
  public void testRemove() {
    Table table = new Table(TWO_COLUMN_TABLE);
    assertFalse(table.removeAt(ROW_1));
    table.put(100, ROW_1);
    table.put(120, ROW_2);
    assertEquals(2, table.count());
    assertTrue(table.removeAt(100));
    assertEquals(1, table.count());
    assertTrue(table.removeAt(120));
    assertEquals(0, table.count());
  }

  @Test
  public void testIterationForwardAfterRemove() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(1, ROW_1);
    table.put(2, ROW_2);
    table.put(3, ROW_3);
    table.put(4, ROW_4);
    table.put(5, ROW_5);
    table.put(6, ROW_6);
    table.removeAt(2);
    assertEquals(Arrays.asList(1, 3, 4, 5, 6), collectKeysForward(table));
    table.removeAt(6);
    assertEquals(Arrays.asList(1, 3, 4, 5), collectKeysForward(table));
    table.removeAt(1);
    assertEquals(Arrays.asList(3, 4, 5), collectKeysForward(table));
    table.removeFromTo(4, 5);
    assertEquals(Arrays.asList(3), collectKeysForward(table));
    table.removeAt(3);
    assertEquals(Collections.emptyList(), collectKeysForward(table));
  }

  private List<Object> collectKeysForward(Table table) {
    List<Object> result = new ArrayList<>();
    Object key = table.firstKey();
    while (key != null) {
      result.add(key);
      key = table.nextKey(key);
    }
    return result;
  }

  @Test
  public void testIterationBackwardAfterRemove() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(1, ROW_1);
    table.put(2, ROW_2);
    table.put(3, ROW_3);
    table.put(4, ROW_4);
    table.put(5, ROW_5);
    table.put(6, ROW_6);
    table.removeAt(2);
    assertEquals(Arrays.asList(6, 5, 4, 3, 1), collectKeysBackward(table));
    table.removeAt(6);
    assertEquals(Arrays.asList(5, 4, 3, 1), collectKeysBackward(table));
    table.removeAt(1);
    assertEquals(Arrays.asList(5, 4, 3), collectKeysBackward(table));
    table.removeFromTo(4, 5);
    assertEquals(Arrays.asList(3), collectKeysBackward(table));
    table.removeAt(3);
    assertEquals(Collections.emptyList(), collectKeysBackward(table));
  }

  @Test
  public void testExists() {
    Table table = new Table(TWO_COLUMN_TABLE);
    table.put(1, ROW_1);
    assertTrue(table.existsAt(1));
    assertFalse(table.existsAt(2));
  }

  private List<Object> collectKeysBackward(Table table) {
    List<Object> result = new ArrayList<>();
    Object key = table.lastKey();
    while (key != null) {
      result.add(key);
      key = table.priorKey(key);
    }
    return result;
  }

  @Test
  public void testRemoveRange() {
    Table table = new Table(TWO_COLUMN_TABLE);
    assertFalse(table.removeAt(ROW_1));
    table.put(1, ROW_1);
    table.put(2, ROW_2);
    table.put(3, ROW_3);
    table.put(4, ROW_4);
    table.put(5, ROW_5);
    table.put(6, ROW_6);
    table.removeFromTo(2, 4);
    assertEquals(3, table.count());
    assertEquals(ROW_1, table.at(1));
    assertEquals(ROW_5, table.at(5));
    assertEquals(ROW_6, table.at(6));
    table.removeFromTo(5, 6);
    assertEquals(1, table.count());
    assertEquals(ROW_1, table.at(1));
    table.removeFromTo(1, 1);
    assertEquals(0, table.count());
  }

  @Test
  public void testRemoveAll() {
    Table table = new Table(TWO_COLUMN_TABLE);
    assertFalse(table.removeAt(ROW_1));
    table.put(1, ROW_1);
    table.put(2, ROW_2);
    table.put(3, ROW_3);
    table.removeAll();
    assertEquals(0, table.count());
  }

  @Test
  public void testUpdate() {
    Table table = new Table(TWO_COLUMN_TABLE);
    assertFalse(table.removeAt(ROW_1));
    table.put(1, ROW_1);
    table.put(2, ROW_2);
    table.put(3, ROW_3);
    table.removeAll();
    assertEquals(0, table.count());
  }
}