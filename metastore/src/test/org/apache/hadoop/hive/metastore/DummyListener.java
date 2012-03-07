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

import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;

/** A dummy implementation for
 * {@link org.apache.hadoop.hive.metastore.MetaStoreEventListener}
 * for testing purposes.
 */
public class DummyListener extends MetaStoreEventListener{

  public static final List<ListenerEvent> notifyList = new ArrayList<ListenerEvent>();

  public DummyListener(Configuration config) {
    super(config);
  }

  @Override
  public void onAddPartition(AddPartitionEvent partition) throws MetaException {
    notifyList.add(partition);
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent db) throws MetaException {
    notifyList.add(db);
  }

  @Override
  public void onCreateTable(CreateTableEvent table) throws MetaException {
    notifyList.add(table);
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent db) throws MetaException {
    notifyList.add(db);
  }

  @Override
  public void onDropPartition(DropPartitionEvent partition) throws MetaException {
    notifyList.add(partition);
  }

  @Override
  public void onDropTable(DropTableEvent table) throws MetaException {
    notifyList.add(table);
  }

  @Override
  public void onAlterTable(AlterTableEvent event) throws MetaException {
    notifyList.add(event);
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) throws MetaException {
    notifyList.add(event);
  }

  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent partEvent) throws MetaException {
    notifyList.add(partEvent);
  }
}
