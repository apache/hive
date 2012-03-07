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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;

/**
 * This abstract class needs to be extended to  provide implementation of actions that needs
 * to be performed when a particular event occurs on a metastore. These methods
 * are called whenever an event occurs on metastore. Status of the event whether
 * it was successful or not is contained in container event object.
 */

public abstract class MetaStoreEventListener implements Configurable {

  private Configuration conf;

  public MetaStoreEventListener(Configuration config){
    this.conf = config;
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public abstract void onCreateTable (CreateTableEvent tableEvent) throws MetaException;

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public abstract void onDropTable (DropTableEvent tableEvent)  throws MetaException;

  /**
   * @param add partition event
   * @throws MetaException
   */

  /**
   * @param tableEvent alter table event
   * @throws MetaException
   */
  public abstract void onAlterTable (AlterTableEvent tableEvent) throws MetaException;


  public abstract void onAddPartition (AddPartitionEvent partitionEvent)  throws MetaException;

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public abstract void onDropPartition (DropPartitionEvent partitionEvent)  throws MetaException;

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public abstract void onAlterPartition (AlterPartitionEvent partitionEvent)  throws MetaException;

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public abstract void onCreateDatabase (CreateDatabaseEvent dbEvent) throws MetaException;

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public abstract void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException;

  /**
   * @param partSetDoneEvent
   * @throws MetaException
   */
  public abstract void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException;

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration config) {
    this.conf = config;
  }



}
