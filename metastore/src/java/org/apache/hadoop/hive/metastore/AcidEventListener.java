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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;


/**
 * It handles cleanup of dropped partition/table/database in ACID related metastore tables
 */
public class AcidEventListener extends MetaStoreEventListener {

  private TxnHandler txnHandler;
  private HiveConf hiveConf;

  public AcidEventListener(Configuration configuration) {
    super(configuration);
    hiveConf = (HiveConf) configuration;
  }

  @Override
  public void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException {
    // We can loop thru all the tables to check if they are ACID first and then perform cleanup,
    // but it's more efficient to unconditionally perform cleanup for the database, especially
    // when there are a lot of tables
    txnHandler = new TxnHandler(hiveConf);
    txnHandler.cleanupRecords(HiveObjectType.DATABASE, dbEvent.getDatabase(), null, null);
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent)  throws MetaException {
    if (TxnHandler.isAcidTable(tableEvent.getTable())) {
      txnHandler = new TxnHandler(hiveConf);
      txnHandler.cleanupRecords(HiveObjectType.TABLE, null, tableEvent.getTable(), null);
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)  throws MetaException {
    if (TxnHandler.isAcidTable(partitionEvent.getTable())) {
      txnHandler = new TxnHandler(hiveConf);
      txnHandler.cleanupRecords(HiveObjectType.PARTITION, null, partitionEvent.getTable(),
          partitionEvent.getPartitionIterator());
    }
  }
}
