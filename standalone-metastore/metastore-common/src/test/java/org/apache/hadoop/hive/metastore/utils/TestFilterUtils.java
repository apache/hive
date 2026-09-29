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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TestFilterUtils {

  private static final String CAT = Warehouse.DEFAULT_CATALOG_NAME;
  private static final String DB = "db1";

  /** Hook that drops every table except {@code allowedTable}. */
  private static class AllowOneTableHook extends DefaultMetaStoreFilterHookImpl {
    private final String allowedTable;

    AllowOneTableHook(String allowedTable) {
      super(new Configuration(false));
      this.allowedTable = allowedTable;
    }

    @Override
    public List<String> filterTableNames(String catName, String dbName, List<String> tableList)
        throws MetaException {
      tableList.removeIf(t -> !allowedTable.equals(t));
      return tableList;
    }
  }

  private static WriteEventInfo event(String db, String table, long writeId) {
    WriteEventInfo info = new WriteEventInfo();
    info.setDatabase(db);
    info.setTable(table);
    info.setWriteId(writeId);
    return info;
  }

  @Test
  public void testFilterWriteEventInfoIfEnabledFilteringDisabled() throws Exception {
    List<WriteEventInfo> input = Arrays.asList(
        event(DB, "t1", 1),
        event(DB, "t2", 2));
    AllowOneTableHook hook = new AllowOneTableHook("t1");

    List<WriteEventInfo> out = FilterUtils.filterWriteEventInfoIfEnabled(
        false, hook, CAT, input);

    assertSame(input, out);
    assertEquals(2, out.size());
  }

  @Test
  public void testFilterWriteEventInfoIfEnabledNullAndEmpty() throws Exception {
    AllowOneTableHook hook = new AllowOneTableHook("t1");

    assertNull(FilterUtils.filterWriteEventInfoIfEnabled(true, hook, CAT, null));
    assertEquals(Collections.emptyList(),
        FilterUtils.filterWriteEventInfoIfEnabled(true, hook, CAT, Collections.emptyList()));
  }

  @Test
  public void testFilterWriteEventInfoIfEnabledKeepsAllowedTablesOnly() throws Exception {
    List<WriteEventInfo> input = Arrays.asList(
        event(DB, "visible", 1),
        event(DB, "hidden", 2),
        event("other_db", "visible", 3));
    AllowOneTableHook hook = new AllowOneTableHook("visible");

    List<WriteEventInfo> out = FilterUtils.filterWriteEventInfoIfEnabled(
        true, hook, CAT, input);

    assertEquals(2, out.size());
    assertEquals(1, out.get(0).getWriteId());
    assertEquals(3, out.get(1).getWriteId());
  }

  @Test
  public void testFilterWriteEventInfoIfEnabledGroupsByDatabase() throws Exception {
    List<WriteEventInfo> input = Arrays.asList(
        event(DB, "visible", 10),
        event(DB, "visible", 11),
        event(DB, "hidden", 12));
    AllowOneTableHook hook = new AllowOneTableHook("visible");

    List<WriteEventInfo> out = FilterUtils.filterWriteEventInfoIfEnabled(
        true, hook, CAT, input);

    assertEquals(2, out.size());
  }
}
