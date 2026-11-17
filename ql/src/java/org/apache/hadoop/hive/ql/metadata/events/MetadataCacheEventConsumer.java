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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.metadata.events;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invalidates the tables in {@link TableVersionRegistry} that the HMS notification events changed.
 * An event it does not know, or cannot read, invalidates everything.
 */
public class MetadataCacheEventConsumer implements EventConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataCacheEventConsumer.class);

  private Configuration conf;
  private final TableVersionRegistry registry;

  public MetadataCacheEventConsumer() {
    this(TableVersionRegistry.get());
  }

  MetadataCacheEventConsumer(TableVersionRegistry registry) {
    this.registry = registry;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void accept(NotificationEvent event) {
    try {
      handle(event);
    } catch (Exception e) {
      LOG.warn("Could not read notification event {}, invalidating the whole metadata cache", event, e);
      registry.invalidateAll();
    }
  }

  @Override
  public void onEventsMissed() {
    LOG.warn("Notification events were missed, invalidating the whole metadata cache");
    registry.invalidateAll();
  }

  private void handle(NotificationEvent event) throws Exception {
    String catName = event.getCatName();
    String dbName = event.getDbName();
    String tblName = event.getTableName();

    switch (event.getEventType()) {
    case MessageBuilder.CREATE_TABLE_EVENT:
    case MessageBuilder.DROP_TABLE_EVENT:
    case MessageBuilder.ADD_PARTITION_EVENT:
    case MessageBuilder.ALTER_PARTITION_EVENT:
    case MessageBuilder.ALTER_PARTITIONS_EVENT:
    case MessageBuilder.DROP_PARTITION_EVENT:
    case MessageBuilder.INSERT_EVENT:
    case MessageBuilder.UPDATE_TBL_COL_STAT_EVENT:
    case MessageBuilder.DELETE_TBL_COL_STAT_EVENT:
    case MessageBuilder.UPDATE_PART_COL_STAT_EVENT:
    case MessageBuilder.UPDATE_PART_COL_STAT_EVENT_BATCH:
    case MessageBuilder.DELETE_PART_COL_STAT_EVENT:
    case MessageBuilder.ADD_PRIMARYKEY_EVENT:
    case MessageBuilder.ADD_UNIQUECONSTRAINT_EVENT:
    case MessageBuilder.ADD_NOTNULLCONSTRAINT_EVENT:
    case MessageBuilder.ADD_DEFAULTCONSTRAINT_EVENT:
    case MessageBuilder.ADD_CHECKCONSTRAINT_EVENT:
    case MessageBuilder.DROP_CONSTRAINT_EVENT:
    case MessageBuilder.COMMIT_COMPACTION_EVENT:
    case MessageBuilder.RELOAD_EVENT:
      invalidateTable(event, catName, dbName, tblName);
      break;
    case MessageBuilder.ALTER_TABLE_EVENT:
      // The event has the new name only; a rename makes the old one stale too
      Table before = deserializer(event).getAlterTableMessage(event.getMessage()).getTableObjBefore();
      if (before != null) {
        registry.invalidateTable(before.getCatName(), before.getDbName(), before.getTableName());
      }
      invalidateTable(event, catName, dbName, tblName);
      break;
    case MessageBuilder.ADD_FOREIGNKEY_EVENT:
      invalidateTable(event, catName, dbName, tblName);
      List<SQLForeignKey> foreignKeys =
          deserializer(event).getAddForeignKeyMessage(event.getMessage()).getForeignKeys();
      if (foreignKeys != null) {
        for (SQLForeignKey fk : foreignKeys) {
          registry.invalidateTable(fk.getCatName(), fk.getPktable_db(), fk.getPktable_name());
        }
      }
      break;
    case MessageBuilder.CREATE_DATABASE_EVENT:
    case MessageBuilder.ALTER_DATABASE_EVENT:
    case MessageBuilder.DROP_DATABASE_EVENT:
      if (dbName == null) {
        throw new IllegalArgumentException("No database name");
      }
      registry.invalidateDatabase(catName, dbName);
      break;
    case MessageBuilder.COMMIT_TXN_EVENT:
    case MessageBuilder.ABORT_TXN_EVENT:
      // The data of transactional tables is cached by write ids; only their Table objects are by version
      if (MessageBuilder.COMMIT_TXN_EVENT.equals(event.getEventType())) {
        CommitTxnMessage msg = deserializer(event).getCommitTxnMessage(event.getMessage());
        List<String> dbs = msg.getDatabases();
        List<String> tbls = msg.getTables();
        if (dbs != null && tbls != null) {
          for (int i = 0; i < Math.min(dbs.size(), tbls.size()); i++) {
            registry.invalidateTable(catName, dbs.get(i), tbls.get(i));
          }
        }
      }
      break;
    case MessageBuilder.OPEN_TXN_EVENT:
    case MessageBuilder.ALLOC_WRITE_ID_EVENT:
    case MessageBuilder.ACID_WRITE_EVENT:
    case MessageBuilder.BATCH_ACID_WRITE_EVENT:
    case MessageBuilder.CREATE_FUNCTION_EVENT:
    case MessageBuilder.DROP_FUNCTION_EVENT:
    case MessageBuilder.CREATE_ISCHEMA_EVENT:
    case MessageBuilder.ALTER_ISCHEMA_EVENT:
    case MessageBuilder.DROP_ISCHEMA_EVENT:
    case MessageBuilder.ADD_SCHEMA_VERSION_EVENT:
    case MessageBuilder.ALTER_SCHEMA_VERSION_EVENT:
    case MessageBuilder.DROP_SCHEMA_VERSION_EVENT:
    case MessageBuilder.CREATE_CATALOG_EVENT:
    case MessageBuilder.ALTER_CATALOG_EVENT:
    case MessageBuilder.DROP_CATALOG_EVENT:
    case MessageBuilder.CREATE_DATACONNECTOR_EVENT:
    case MessageBuilder.ALTER_DATACONNECTOR_EVENT:
    case MessageBuilder.DROP_DATACONNECTOR_EVENT:
    case MessageBuilder.CONFIG_CHANGE_EVENT:
      // nothing the cache holds
      break;
    default:
      LOG.warn("Unknown notification event type {}, invalidating the whole metadata cache", event.getEventType());
      registry.invalidateAll();
      break;
    }
  }

  private void invalidateTable(NotificationEvent event, String catName, String dbName, String tblName) {
    if (dbName == null || tblName == null) {
      throw new IllegalArgumentException("No table name in " + event.getEventType() + " event");
    }
    registry.invalidateTable(catName, dbName, tblName);
  }

  private static MessageDeserializer deserializer(NotificationEvent event) throws Exception {
    return MessageFactory.getInstance(event.getMessageFormat()).getDeserializer();
  }
}
