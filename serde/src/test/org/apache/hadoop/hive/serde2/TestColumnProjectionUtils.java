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

package org.apache.hadoop.hive.serde2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class TestColumnProjectionUtils {

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }

  @Test
  public void testReadAllColumns() {
    // test that the all columns will be read by default
    assertTrue(ColumnProjectionUtils.isReadAllColumns(conf));
    // test that setting read all resets column ids
    ColumnProjectionUtils.appendReadColumns(conf, Arrays.asList(new Integer[]{0, 1, 2}));
    ColumnProjectionUtils.setReadAllColumns(conf);
    assertEquals(Collections.EMPTY_LIST, ColumnProjectionUtils.getReadColumnIDs(conf));
    assertTrue(ColumnProjectionUtils.isReadAllColumns(conf));
  }

  @Test
  public void testReadColumnIds() {
    List<Integer> columnIds = new ArrayList<Integer>();
    List<Integer> actual;

    assertEquals(Collections.EMPTY_LIST, ColumnProjectionUtils.getReadColumnIDs(conf));

    // test that read columns are initially an empty list
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    assertEquals(Collections.EMPTY_LIST, actual);
    // set setting read column ids with an empty list
    ColumnProjectionUtils.appendReadColumns(conf, columnIds);
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    assertEquals(Collections.EMPTY_LIST, actual);
    // test that setting read column ids set read all columns to false
    assertFalse(ColumnProjectionUtils.isReadAllColumns(conf));
    // add needed columns
    columnIds.add(1);
    ColumnProjectionUtils.appendReadColumns(conf, Collections.singletonList(1));
    assertEquals(columnIds, ColumnProjectionUtils.getReadColumnIDs(conf));
    columnIds.add(2);
    ColumnProjectionUtils.appendReadColumns(conf, Collections.singletonList(2));
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    Collections.sort(actual);
    assertEquals(columnIds, actual);
    columnIds.add(3);
    ColumnProjectionUtils.appendReadColumns(conf, Collections.singletonList(3));
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    Collections.sort(actual);
    assertEquals(columnIds, actual);
    assertFalse(ColumnProjectionUtils.isReadAllColumns(conf));
  }

  @Test
  public void testMultipleIdsWithEmpty() {
    List<Integer> ids1 = Arrays.asList(1, 2);
    List<Integer> ids2 = new ArrayList<Integer>();
    List<Integer> ids3 = Arrays.asList(2, 3);

    ColumnProjectionUtils.appendReadColumns(conf, ids1);
    ColumnProjectionUtils.appendReadColumns(conf, ids2);
    ColumnProjectionUtils.appendReadColumns(conf, ids3);

    List<Integer> actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    assertEquals(Arrays.asList(2, 3, 1), actual);
  }

  @Test
  public void testDeprecatedMethods() {
    List<Integer> columnIds = new ArrayList<Integer>();
    List<Integer> actual;

    assertEquals(Collections.EMPTY_LIST, ColumnProjectionUtils.getReadColumnIDs(conf));

    // test that read columns are initially an empty list
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    assertEquals(Collections.EMPTY_LIST, actual);
    // setting empty list results in reading none
    ColumnProjectionUtils.setReadColumnIDs(conf, columnIds);
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    assertEquals(Collections.EMPTY_LIST, actual);
    // test set and append methods
    columnIds.add(1);
    ColumnProjectionUtils.setReadColumnIDs(conf, Collections.singletonList(1));
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    Collections.sort(actual);
    assertEquals(columnIds, actual);
    columnIds.add(2);
    ColumnProjectionUtils.appendReadColumnIDs(conf, Collections.singletonList(2));
    actual = ColumnProjectionUtils.getReadColumnIDs(conf);
    Collections.sort(actual);
    assertEquals(columnIds, actual);
    // test that setting read column ids set read all columns to false
    assertFalse(ColumnProjectionUtils.isReadAllColumns(conf));
    ColumnProjectionUtils.setFullyReadColumns(conf);
    assertTrue(ColumnProjectionUtils.isReadAllColumns(conf));
  }
}
