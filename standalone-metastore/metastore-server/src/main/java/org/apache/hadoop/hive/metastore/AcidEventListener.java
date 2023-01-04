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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.hadoop.hive.metastore.HMSHandler.isMustPurge;
import static org.apache.hadoop.hive.metastore.HMSHandler.getWriteId;
import static org.apache.hadoop.hive.metastore.HiveMetaStoreClient.RENAME_PARTITION_MAKE_COPY;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.throwMetaException;


/**
 * It handles cleanup of dropped partition/table/database in ACID related metastore tables
 */
public class AcidEventListener extends TransactionalMetaStoreEventListener {

  private TxnStore txnHandler;
  private Configuration conf;

  public AcidEventListener(Configuration configuration) {
    super(configuration);
    conf = configuration;
  }

  @Override
  public void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException {
    // We can loop thru all the tables to check if they are ACID first and then perform cleanup,
    // but it's more efficient to unconditionally perform cleanup for the database, especially
    // when there are a lot of tables
    txnHandler = getTxnHandler();
    long currentTxn = getTxnId(dbEvent.getEnvironmentContext());
    txnHandler.cleanupRecords(HiveObjectType.DATABASE, dbEvent.getDatabase(), null, null, currentTxn);
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    Table table = tableEvent.getTable();
    
    if (TxnUtils.isTransactionalTable(table)) {
      txnHandler = getTxnHandler();
      txnHandler.cleanupRecords(HiveObjectType.TABLE, null, table, null, !tableEvent.getDeleteData());
      
      if (!tableEvent.getDeleteData()) {
        long currentTxn = getTxnId(tableEvent.getEnvironmentContext());
        
        if (currentTxn > 0) {
          try {
            CompactionRequest rqst = new CompactionRequest(table.getDbName(), table.getTableName(), CompactionType.MAJOR);
            rqst.setRunas(TxnUtils.findUserToRunAs(table.getSd().getLocation(), table, conf));
            rqst.putToProperties("location", table.getSd().getLocation());
            rqst.putToProperties("ifPurge", Boolean.toString(isMustPurge(tableEvent.getEnvironmentContext(), table)));
            txnHandler.submitForCleanup(rqst, table.getWriteId(), currentTxn);
          } catch (InterruptedException | IOException e) {
            throwMetaException(e);
          }
        }
      }
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)  throws MetaException {
    Table table = partitionEvent.getTable();
    EnvironmentContext context = partitionEvent.getEnvironmentContext();

    if (TxnUtils.isTransactionalTable(table)) {
      txnHandler = getTxnHandler();
      txnHandler.cleanupRecords(HiveObjectType.PARTITION, null, table, partitionEvent.getPartitionIterator());

      if (!partitionEvent.getDeleteData()) {
        long currentTxn = getTxnId(context);
        
        if (currentTxn > 0) {
          long writeId = getWriteId(context);
          try {
            CompactionRequest rqst = new CompactionRequest(
              table.getDbName(), table.getTableName(), CompactionType.MAJOR);
            rqst.setRunas(TxnUtils.findUserToRunAs(table.getSd().getLocation(), table, conf));
            rqst.putToProperties("ifPurge", Boolean.toString(isMustPurge(context, table)));

            Iterator<Partition> partitionIterator = partitionEvent.getPartitionIterator();
            while (partitionIterator.hasNext()) {
              Partition p = partitionIterator.next();

              List<FieldSchema> partCols = partitionEvent.getTable().getPartitionKeys();  // partition columns
              List<String> partVals = p.getValues();
              rqst.setPartitionname(Warehouse.makePartName(partCols, partVals));
              rqst.putToProperties("location", p.getSd().getLocation());

              txnHandler.submitForCleanup(rqst, writeId, currentTxn);
            }
          } catch (InterruptedException | IOException e) {
            throwMetaException(e);
          }
        }
      }
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    if (!TxnUtils.isTransactionalTable(tableEvent.getNewTable())) {
      return;
    }
    Table oldTable = tableEvent.getOldTable();
    Table newTable = tableEvent.getNewTable();
    if (!oldTable.getCatName().equalsIgnoreCase(newTable.getCatName()) ||
        !oldTable.getDbName().equalsIgnoreCase(newTable.getDbName()) ||
        !oldTable.getTableName().equalsIgnoreCase(newTable.getTableName())) {
      txnHandler = getTxnHandler();
      txnHandler.onRename(
          oldTable.getCatName(), oldTable.getDbName(), oldTable.getTableName(), null,
          newTable.getCatName(), newTable.getDbName(), newTable.getTableName(), null);
    }
  }
  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent)  throws MetaException {
    if (!TxnUtils.isTransactionalTable(partitionEvent.getTable())) {
      return;
    }
    Partition oldPart = partitionEvent.getOldPartition();
    Partition newPart = partitionEvent.getNewPartition();
    Table t = partitionEvent.getTable();
    String oldPartName = Warehouse.makePartName(t.getPartitionKeys(), oldPart.getValues());
    String newPartName = Warehouse.makePartName(t.getPartitionKeys(), newPart.getValues());
    if (!oldPartName.equals(newPartName)) {
      txnHandler = getTxnHandler();
      txnHandler.onRename(t.getCatName(), t.getDbName(), t.getTableName(), oldPartName,
          t.getCatName(), t.getDbName(), t.getTableName(), newPartName);

      EnvironmentContext context = partitionEvent.getEnvironmentContext();
      Table table = partitionEvent.getTable();

      boolean clonePart = Optional.ofNullable(context)
        .map(EnvironmentContext::getProperties)
        .map(prop -> prop.get(RENAME_PARTITION_MAKE_COPY))
        .map(Boolean::parseBoolean)
        .orElse(false);
      
      if (clonePart) {
        long currentTxn = getTxnId(context);
        
        if (currentTxn > 0) {
          try {
            CompactionRequest rqst = new CompactionRequest(
              table.getDbName(), table.getTableName(), CompactionType.MAJOR);
            rqst.setRunas(TxnUtils.findUserToRunAs(table.getSd().getLocation(), table, conf));
            rqst.setPartitionname(oldPartName);

            rqst.putToProperties("location", oldPart.getSd().getLocation());
            rqst.putToProperties("ifPurge", Boolean.toString(isMustPurge(context, table)));
            txnHandler.submitForCleanup(rqst, partitionEvent.getWriteId(), currentTxn);
          } catch (InterruptedException | IOException e) {
            throwMetaException(e);
          }
        }
      }
    }
  }
  
  @Override
  public void onAlterDatabase(AlterDatabaseEvent dbEvent) throws MetaException {
    Database oldDb = dbEvent.getOldDatabase();
    Database newDb = dbEvent.getNewDatabase();
    if (!oldDb.getCatalogName().equalsIgnoreCase(newDb.getCatalogName()) ||
        !oldDb.getName().equalsIgnoreCase(newDb.getName())) {
      txnHandler = getTxnHandler();
      txnHandler.onRename(
          oldDb.getCatalogName(), oldDb.getName(), null, null,
          newDb.getCatalogName(), newDb.getName(), null, null);
    }
  }

  private TxnStore getTxnHandler() {
    boolean hackOn = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) ||
        MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEZ_TEST);
    String origTxnMgr = null;
    boolean origConcurrency = false;

    // Since TxnUtils.getTxnStore calls TxnHandler.setConf -> checkQFileTestHack -> TxnDbUtil.setConfValues,
    // which may change the values of below two entries, we need to avoid polluting the original values
    if (hackOn) {
      origTxnMgr = MetastoreConf.getVar(conf, ConfVars.HIVE_TXN_MANAGER);
      origConcurrency = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY);
    }

    txnHandler = TxnUtils.getTxnStore(conf);

    // Set them back
    if (hackOn) {
      MetastoreConf.setVar(conf, ConfVars.HIVE_TXN_MANAGER, origTxnMgr);
      MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, origConcurrency);
    }

    return txnHandler;
  }

  private long getTxnId(EnvironmentContext context) {
    return Optional.ofNullable(context)
      .map(EnvironmentContext::getProperties)
      .map(prop -> prop.get(hive_metastoreConstants.TXN_ID))
      .map(Long::parseLong)
      .orElse(0L);
  }
}
