/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestLoadedTableHandoff {

  private final org.apache.iceberg.Table icebergTable = mock(org.apache.iceberg.Table.class);

  @AfterEach
  public void after() {
    LoadedTableHandoff.clear();
  }

  @Test
  public void testTakenOnceForTheSameVersion() {
    LoadedTableHandoff.offer(hmsTable("db", "t", "v1"), icebergTable);

    assertThat(LoadedTableHandoff.take(hmsTable("DB", "T", "v1"))).containsSame(icebergTable);
    assertThat(LoadedTableHandoff.take(hmsTable("db", "t", "v1"))).isEmpty();
  }

  @Test
  public void testNotTakenForAnotherTableOrVersion() {
    LoadedTableHandoff.offer(hmsTable("db", "t", "v1"), icebergTable);
    assertThat(LoadedTableHandoff.take(hmsTable("db", "t", "v2"))).isEmpty();

    LoadedTableHandoff.offer(hmsTable("db", "t", "v1"), icebergTable);
    assertThat(LoadedTableHandoff.take(hmsTable("db", "other", "v1"))).isEmpty();

    // a later offer replaces the earlier one
    LoadedTableHandoff.offer(hmsTable("db", "t", "v1"), icebergTable);
    LoadedTableHandoff.offer(hmsTable("db", "u", "v1"), mock(org.apache.iceberg.Table.class));
    assertThat(LoadedTableHandoff.take(hmsTable("db", "t", "v1"))).isEmpty();
  }

  @Test
  public void testNotTakenWithoutMetadataLocationOrOnAnotherThread() throws Exception {
    LoadedTableHandoff.offer(hmsTable("db", "t", null), icebergTable);
    assertThat(LoadedTableHandoff.take(hmsTable("db", "t", null))).isEmpty();

    LoadedTableHandoff.offer(hmsTable("db", "t", "v1"), icebergTable);
    assertThat(CompletableFuture.supplyAsync(() -> LoadedTableHandoff.take(hmsTable("db", "t", "v1"))).get())
        .isEmpty();
    LoadedTableHandoff.clear();
    assertThat(LoadedTableHandoff.take(hmsTable("db", "t", "v1"))).isEmpty();
  }

  private static Table hmsTable(String dbName, String tableName, String metadataLocation) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setParameters(Maps.newHashMap());
    if (metadataLocation != null) {
      table.putToParameters(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, metadataLocation);
    }
    return table;
  }
}
