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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddCheckConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddDefaultConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterCatalogEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.AlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionsEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.BatchAcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.CreateDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEventBatch;
import org.apache.hadoop.hive.metastore.events.UpdateTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeleteTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeletePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.ReloadEvent;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import java.sql.Connection;

/**
 * This abstract class needs to be extended to  provide implementation of actions that needs
 * to be performed when a particular event occurs on a metastore. These methods
 * are called whenever an event occurs on metastore. Status of the event whether
 * it was successful or not is contained in container event object.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MetaStoreEventListener implements Configurable {

  private Configuration conf;

  public MetaStoreEventListener(Configuration config){
    this.conf = config;
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public void onDropTable (DropTableEvent tableEvent)  throws MetaException {
  }

  /**
   * @param tableEvent alter table event
   * @throws MetaException
   */
  public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
  }

  /**
   * @param partitionEvent add partition event
   * @throws MetaException
   */
  public void onAddPartition (AddPartitionEvent partitionEvent) throws MetaException {
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onDropPartition (DropPartitionEvent partitionEvent)  throws MetaException {
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onAlterPartition (AlterPartitionEvent partitionEvent)  throws MetaException {
  }

  /**
   * @param event alter partitions event
   * @throws MetaException
   */
  public void onAlterPartitions (AlterPartitionsEvent event)  throws MetaException {
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public void onCreateDatabase (CreateDatabaseEvent dbEvent) throws MetaException {
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException {
  }

  /**
   * @param connectorEvent  dataconnector event
   * @throws MetaException
   */
  public void onCreateDataConnector (CreateDataConnectorEvent connectorEvent) throws MetaException {
  }

  /**
   * @param connectorEvent dataconnector event
   * @throws MetaException
   */
  public void onDropDataConnector (DropDataConnectorEvent connectorEvent) throws MetaException {
  }

  /**
   * @param dcEvent alter data connector event
   * @throws MetaException
   */
  public void onAlterDataConnector(AlterDataConnectorEvent dcEvent) throws MetaException {
  }

  /**
   * @param dbEvent alter database event
   * @throws MetaException
   */
  public void onAlterDatabase(AlterDatabaseEvent dbEvent) throws MetaException {
  }

  /**
   * @param partSetDoneEvent
   * @throws MetaException
   */
  public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
  }

  /**
   * @param fnEvent function event
   * @throws MetaException
   */
  public void onCreateFunction (CreateFunctionEvent fnEvent) throws MetaException {
  }

  /**
   * @param fnEvent function event
   * @throws MetaException
   */
  public void onDropFunction (DropFunctionEvent fnEvent) throws MetaException {
  }

  /**
   * This will be called when an insert is executed that does not cause a partition to be added.
   * If an insert causes a partition to be added it will cause {@link #onAddPartition} to be
   * called instead.
   * @param insertEvent
   * @throws MetaException
   */
  public void onInsert(InsertEvent insertEvent) throws MetaException {
  }

  /**
   * @param addPrimaryKeyEvent add primary key event
   * @throws MetaException
   */
  public void onAddPrimaryKey(AddPrimaryKeyEvent addPrimaryKeyEvent) throws MetaException {
  }

  /**
   * @param addForeignKeyEvent add foreign key event
   * @throws MetaException
   */
  public void onAddForeignKey(AddForeignKeyEvent addForeignKeyEvent) throws MetaException {
  }

  /**
   * @param addUniqueConstraintEvent add unique constraint event
   * @throws MetaException
   */
  public void onAddUniqueConstraint(AddUniqueConstraintEvent addUniqueConstraintEvent) throws MetaException {
  }

  /**
   * @param addNotNullConstraintEvent add not null constraint event
   * @throws MetaException
   */
  public void onAddNotNullConstraint(AddNotNullConstraintEvent addNotNullConstraintEvent) throws MetaException {
  }

  /**
   * @param addDefaultConstraintEvent add default constraint event
   * @throws MetaException
   */
  public void onAddDefaultConstraint(AddDefaultConstraintEvent addDefaultConstraintEvent) throws MetaException {
  }

  /**
   * @param addCheckConstraintEvent add check constraint event
   * @throws MetaException
   */
  public void onAddCheckConstraint(AddCheckConstraintEvent addCheckConstraintEvent) throws MetaException {
  }

  /**
   * @param dropConstraintEvent drop constraint event
   * @throws MetaException
   */
  public void onDropConstraint(DropConstraintEvent dropConstraintEvent) throws MetaException {
  }

  public void onCreateISchema(CreateISchemaEvent createISchemaEvent) throws MetaException {
  }

  public void onAlterISchema(AlterISchemaEvent alterISchemaEvent) throws MetaException {
  }

  public void onDropISchema(DropISchemaEvent dropISchemaEvent) throws MetaException {
  }

  public void onAddSchemaVersion(AddSchemaVersionEvent addSchemaVersionEvent) throws MetaException {
  }

  public void onAlterSchemaVersion(AlterSchemaVersionEvent alterSchemaVersionEvent)
      throws MetaException {
  }

  public void onDropSchemaVersion(DropSchemaVersionEvent dropSchemaVersionEvent)
      throws MetaException {
  }

  public void onCreateCatalog(CreateCatalogEvent createCatalogEvent) throws MetaException {
  }

  public void onAlterCatalog(AlterCatalogEvent alterCatalogEvent) throws MetaException {
  }

  public void onDropCatalog(DropCatalogEvent dropCatalogEvent) throws MetaException {
  }

  /**
   * This will be called when a new transaction is started.
   * @param openTxnEvent event to be processed
   * @param dbConn jdbc connection to remote meta store db.
   * @param sqlGenerator helper class to generate db specific sql string.
   * @throws MetaException
   */
  public void onOpenTxn(OpenTxnEvent openTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
  }

  /**
   * This will be called to commit a transaction.
   * @param commitTxnEvent event to be processed
   * @param dbConn jdbc connection to remote meta store db.
   * @param sqlGenerator helper class to generate db specific sql string.
   * @throws MetaException
   */
  public void onCommitTxn(CommitTxnEvent commitTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws
          MetaException {
  }

  /**
   * This will be called to abort a transaction.
   * @param abortTxnEvent event to be processed
   * @param dbConn jdbc connection to remote meta store db.
   * @param sqlGenerator helper class to generate db specific sql string.
   * @throws MetaException
   */
  public void onAbortTxn(AbortTxnEvent abortTxnEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
  }

  /**
   * This will be called to alloc a new write id.
   * @param allocWriteIdEvent event to be processed
   * @param dbConn jdbc connection to remote meta store db.
   * @param sqlGenerator helper class to generate db specific sql string.
   * @throws MetaException
   */
  public void onAllocWriteId(AllocWriteIdEvent allocWriteIdEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
  }

  /**
   * This will be called to perform acid write operation.
   * @param acidWriteEvent event to be processed
   * @param dbConn jdbc connection to remote meta store db.
   * @param sqlGenerator helper class to generate db specific sql string.
   * @throws MetaException
   */
  public void onAcidWrite(AcidWriteEvent acidWriteEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
  }

  /**
   * This will be called to perform acid write operation in a batch.
   * @param batchAcidWriteEvent event to be processed
   * @param dbConn jdbc connection to remote meta store db.
   * @param sqlGenerator helper class to generate db specific sql string.
   * @throws MetaException
   */
  public void onBatchAcidWrite(BatchAcidWriteEvent batchAcidWriteEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
  }

  /**
   * This will be called to update table column stats
   * @param updateTableColumnStatEvent event to be processed
   * @throws MetaException
   */
  public void onUpdateTableColumnStat(UpdateTableColumnStatEvent updateTableColumnStatEvent)
          throws MetaException {
  }

  /**
   * This will be called to delete table column stats
   * @param deleteTableColumnStatEvent event to be processed
   * @throws MetaException
   */
  public void onDeleteTableColumnStat(DeleteTableColumnStatEvent deleteTableColumnStatEvent)
          throws MetaException {
  }

  /**
   * This will be called to update partition column stats
   * @param updatePartColStatEvent event to be processed
   * @throws MetaException
   */
  public void onUpdatePartitionColumnStat(UpdatePartitionColumnStatEvent updatePartColStatEvent)
          throws MetaException {
  }

  /**
   * This will be called to update batch of partition column stats.The backend RDBMS operations are done using
   * direct sql mode.
   * @param updatePartColStatEvent event to be processed
   * @throws MetaException
   */
  public void onUpdatePartitionColumnStatInBatch(UpdatePartitionColumnStatEventBatch updatePartColStatEvent,
                                                 Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
  }

  /**
   * This will be called to delete partition column stats
   * @param deletePartColStatEvent event to be processed
   * @throws MetaException
   */
  public void onDeletePartitionColumnStat(DeletePartitionColumnStatEvent deletePartColStatEvent)
          throws MetaException {
  }

  /**
   * This will be called to commit a compaction transaction.
   * @param commitCompactionEvent event to be processed
   * @param dbConn jdbc connection to remote meta store db.
   * @param sqlGenerator helper class to generate db specific sql string.
   * @throws MetaException ex
   */
  public void onCommitCompaction(CommitCompactionEvent commitCompactionEvent, Connection dbConn,
      SQLGenerator sqlGenerator) throws MetaException {
  }

  /**
   * This will be called to reload table/partition
   * @param reloadEvent event to be processed
   * @throws MetaException
   */
  public void onReload(ReloadEvent reloadEvent)
          throws MetaException {
  }

  /**
   * This is to check if the listener adds the event info to notification log table.
   */
  public boolean doesAddEventsToNotificationLogTable() {
    return false;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration config) {
    this.conf = config;
  }
}
