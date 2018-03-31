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
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;

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

  public void onDropCatalog(DropCatalogEvent dropCatalogEvent) throws MetaException {
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
