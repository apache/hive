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

package org.apache.hive.search.testutil;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/** Mockito helpers for bootstrap and indexer initialization tests. */
public final class MetastoreBootstrapMocks {
  private MetastoreBootstrapMocks() {}

  public static void stubCatchUp(IMetaStoreClient client) throws Exception {
    NotificationEventResponse empty = new NotificationEventResponse();
    empty.setEvents(new ArrayList<>());
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(false), eq(null)))
        .thenReturn(empty);
  }

  public static void stubCurrentNotificationId(IMetaStoreClient client, long eventId)
      throws Exception {
    when(client.getCurrentNotificationEventId())
        .thenReturn(new CurrentNotificationEventId(eventId));
  }

  public static void stubBootstrapCatalog(IMetaStoreClient client, Table... tables)
      throws Exception {
    Map<String, List<Table>> tablesByDb = new LinkedHashMap<>();
    for (Table table : tables) {
      tablesByDb.computeIfAbsent(table.getDbName(), db -> new ArrayList<>()).add(table);
    }
    when(client.getAllDatabases()).thenReturn(new ArrayList<>(tablesByDb.keySet()));
    for (Map.Entry<String, List<Table>> entry : tablesByDb.entrySet()) {
      String db = entry.getKey();
      List<String> tableNames = entry.getValue().stream()
          .map(Table::getTableName)
          .collect(Collectors.toList());
      when(client.getAllTables(db)).thenReturn(tableNames);
      when(client.getTableObjectsByName(eq(db), anyList())).thenAnswer(invocation -> {
        @SuppressWarnings("unchecked")
        List<String> requested = invocation.getArgument(1);
        return requested.stream()
            .map(name -> findTable(entry.getValue(), name))
            .collect(Collectors.toList());
      });
    }
  }

  private static Table findTable(List<Table> tables, String name) {
    for (Table table : tables) {
      if (table.getTableName().equals(name)) {
        return table;
      }
    }
    throw new IllegalArgumentException("Unknown table: " + name);
  }
}
