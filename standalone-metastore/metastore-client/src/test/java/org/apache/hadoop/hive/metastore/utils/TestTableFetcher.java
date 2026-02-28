/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestTableFetcher {

  @Test
  public void testEmpty() {
    TableFetcher fetcher = new TableFetcher.Builder(mock(IMetaStoreClient.class), null, null, null).build();
    // full empty parameter list leads to empty table filter
    Assert.assertEquals("", fetcher.tableFilter);
  }

  @Test
  public void testAsterisk() {
    TableFetcher fetcher = new TableFetcher.Builder(mock(IMetaStoreClient.class), "hive", "*", "*").build();
    // full empty parameter list leads to empty table filter
    Assert.assertEquals("hive_filter_field_tableName__ like \".*\"", fetcher.tableFilter);
  }

  @Test
  public void testCustomCondition() {
    TableFetcher fetcher = new TableFetcher.Builder(mock(IMetaStoreClient.class), "hive", "*", "*")
        .tableCondition(hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "table_param like \"some_value\" ")
        .build();
    // full empty parameter list leads to empty table filter
    Assert.assertEquals(
        "hive_filter_field_params__table_param like \"some_value\"  and hive_filter_field_tableName__ like \".*\"",
        fetcher.tableFilter);
  }
}
