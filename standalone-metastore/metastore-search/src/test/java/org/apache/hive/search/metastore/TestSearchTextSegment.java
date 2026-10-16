/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.metastore;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestSearchTextSegment {

  @Test
  public void headSegmentWhenTableHasComment() {
    Table table = tableWithColumns(0);
    table.setTableName("Orders");
    Map<String, String> params = new HashMap<>();
    params.put("comment", "daily orders");
    table.setParameters(params);

    List<String> segments = SearchTextSegment.build(table, 4, 1800);
    assertEquals("table: orders; comment: daily orders", segments.getFirst());
  }

  @Test
  public void headSegmentWithTableNameWhenNoComment() {
    Table table = tableWithColumns(0);
    List<String> segments = SearchTextSegment.build(table, 4, 1800);
    assertEquals("table: t", segments.getFirst());
  }

  @Test
  public void columnsPackAfterHead() {
    Table table = tableWithColumns(3);
    List<String> segments = SearchTextSegment.build(table, 2, 50);
    assertEquals(2, segments.size());
    assertEquals("table: t", segments.get(0));
    assertTrue(segments.get(1).contains("column col0"));
  }

  @Test
  public void segmentFieldNaming() {
    assertEquals("search_text_0", SearchTextSegment.segmentField(0));
    assertTrue(SearchTextSegment.isSegmentField("search_text_2"));
  }

  private static Table tableWithColumns(int count) {
    Table table = new Table();
    table.setTableName("t");
    table.setSd(new StorageDescriptor());
    List<org.apache.hadoop.hive.metastore.api.FieldSchema> cols = new java.util.ArrayList<>();
    for (int i = 0; i < count; i++) {
      cols.add(new org.apache.hadoop.hive.metastore.api.FieldSchema(
          "col" + i, "string", "comment " + i));
    }
    table.getSd().setCols(cols);
    return table;
  }
}
