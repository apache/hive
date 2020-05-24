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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ReplMetaStoreEventListenerTestImpl - Implementation of MetaStoreEventListener to test
 * isReplicated flag in some of the tests.
 */
public class ReplMetaStoreEventListenerTestImpl extends MetaStoreEventListener {
  protected static final Logger LOG =
          LoggerFactory.getLogger(ReplMetaStoreEventListenerTestImpl.class);
  static Map<String, Set<String>> replicatedDbsForEvents = new HashMap<>();
  static Map<String, Set<String>> nonReplicatedDbsForEvents = new HashMap<>();
  static Map<String, Set<String>> replicatedTablesForEvents = new HashMap<>();

  public ReplMetaStoreEventListenerTestImpl(Configuration conf) {
    super(conf);
  }

  private void addNameToEventMap(Map<String, Set<String>> eventMap, String name,
                                 ListenerEvent event) {
    String eventType = event.getClass().getName();
    Set<String> eventNames = eventMap.get(eventType);
    if (eventNames == null) {
      eventNames = new HashSet<>();
      eventMap.put(eventType, eventNames);
    }
    eventNames.add(name.toLowerCase());
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent createDatabaseEvent) {
    String dbName = createDatabaseEvent.getDatabase().getName().toLowerCase();
    if (createDatabaseEvent.isReplicated()) {
      addNameToEventMap(replicatedDbsForEvents, dbName, createDatabaseEvent);
    } else {
      addNameToEventMap(nonReplicatedDbsForEvents, dbName, createDatabaseEvent);
    }
  }

  @Override
  public void onAlterDatabase(AlterDatabaseEvent alterDatabaseEvent) {
    // The test doesn't create any database rename events, so it's fine to just check the new
    // database name.
    String dbName = alterDatabaseEvent.getNewDatabase().getName().toLowerCase();
    if (alterDatabaseEvent.isReplicated()) {
      addNameToEventMap(replicatedDbsForEvents, dbName, alterDatabaseEvent);
    } else {
      addNameToEventMap(nonReplicatedDbsForEvents, dbName, alterDatabaseEvent);
    }
  }

  @Override
  public void onCreateTable(CreateTableEvent createTableEvent) {
    String dbName = createTableEvent.getTable().getDbName();
    String tblName = createTableEvent.getTable().getTableName();
    if (createTableEvent.isReplicated()) {
      addNameToEventMap(replicatedDbsForEvents, dbName, createTableEvent);
      addNameToEventMap(replicatedTablesForEvents, tblName, createTableEvent);
    } else {
      addNameToEventMap(nonReplicatedDbsForEvents, dbName, createTableEvent);
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent alterTableEvent) {
    // Test doesn't have table rename events, since we are only interested in checking replication
    // status. So, it's fine to get the names from the new table.
    String dbName = alterTableEvent.getNewTable().getDbName();
    String tblName = alterTableEvent.getNewTable().getTableName();
    if (alterTableEvent.isReplicated()) {
      addNameToEventMap(replicatedDbsForEvents, dbName, alterTableEvent);
      addNameToEventMap(replicatedTablesForEvents, tblName, alterTableEvent);
    } else {
      addNameToEventMap(nonReplicatedDbsForEvents, dbName, alterTableEvent);
    }
  }

  @Override
  public void onDropTable(DropTableEvent dropTableEvent) {
    String dbName = dropTableEvent.getTable().getDbName();
    String tblName = dropTableEvent.getTable().getTableName();
    if (dropTableEvent.isReplicated()) {
      addNameToEventMap(replicatedDbsForEvents, dbName, dropTableEvent);
      addNameToEventMap(replicatedTablesForEvents, tblName, dropTableEvent);
    } else {
      addNameToEventMap(nonReplicatedDbsForEvents, dbName, dropTableEvent);
    }

  }

  static void checkEventSanity(Map<String, Set<String>> eventsMap, String replicaDbName) {
    replicaDbName = replicaDbName.toLowerCase();
    for (String event : eventsMap.keySet()) {
      Set<String> dbsForEvent = replicatedDbsForEvents.get(event);
      LOG.info("Examining dbs and tables for event " + event);
      // isreplicated should be true only for replicated database
      Assert.assertTrue(dbsForEvent.contains(replicaDbName));
      Assert.assertEquals(1, dbsForEvent.size());
      if (nonReplicatedDbsForEvents.get(event) != null) {
        Assert.assertFalse(nonReplicatedDbsForEvents.get(event).contains(replicaDbName));
      }

      Set<String> eventTables = replicatedTablesForEvents.get(event);
      Assert.assertEquals(eventsMap.get(event), eventTables);
    }
  }

  static void clearSanityMap(Map<String, Set<String>> map) {
    for (Set<String> eventEntry : map.values()) {
      if (eventEntry != null) {
        eventEntry.clear();
      }
    }
    map.clear();
  }

  static void clearSanityData() {
    clearSanityMap(replicatedDbsForEvents);
    clearSanityMap(nonReplicatedDbsForEvents);
    clearSanityMap(replicatedTablesForEvents);
  }
}
