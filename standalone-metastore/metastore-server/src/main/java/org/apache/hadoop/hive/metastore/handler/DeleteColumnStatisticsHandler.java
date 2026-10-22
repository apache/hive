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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DeletePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeleteTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartValsFromName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@SuppressWarnings("unused")
@RequestHandler(requestBody = DeleteColumnStatisticsRequest.class)
public class DeleteColumnStatisticsHandler
    extends AbstractRequestHandler<DeleteColumnStatisticsRequest,
    DeleteColumnStatisticsHandler.DeleteColumnStatisticsResult> {
  private RawStore ms;
  private Configuration conf;
  private String catName;
  private String dbName;
  private String tableName;
  private List<String> colNames;
  private String engine;
  private List<ListenerEvent> events;
  private EventType eventType;

  DeleteColumnStatisticsHandler(IHMSHandler handler, DeleteColumnStatisticsRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.conf = handler.getConf();
    this.ms = handler.getMS();
    this.colNames = request.getCol_names();
    this.engine = request.getEngine();
    String normalizedDbName = normalizeIdentifier(request.getDb_name());
    this.tableName = normalizeIdentifier(request.getTbl_name());
    String[] parsedDbName = parseDbName(normalizedDbName, conf);
    if (request.getCat_name() != null) {
      parsedDbName[CAT_NAME] = normalizeIdentifier(request.getCat_name());
    }
    this.catName = parsedDbName[CAT_NAME];
    this.dbName = parsedDbName[DB_NAME];
    this.events = new ArrayList<>();
  }

  @Override
  protected DeleteColumnStatisticsResult execute() throws TException, IOException {
    boolean ret = false;
    boolean committed = false;
    ms.openTransaction();
    try {
      Table table = ms.getTable(catName, dbName, tableName);
      boolean isPartitioned = table.getPartitionKeysSize() > 0;
      if (TxnUtils.isTransactionalTable(table)) {
        throw new MetaException("Cannot delete stats via this API for a transactional table");
      }
      if (!isPartitioned || request.isTableLevel()) {
        ret = ms.deleteTableColumnStatistics(catName, dbName, tableName, colNames, engine);
        if (ret) {
          eventType = EventType.DELETE_TABLE_COLUMN_STAT;
          for (String colName : colNames == null || colNames.isEmpty() ?
              table.getSd().getCols().stream().map(FieldSchema::getName).collect(Collectors.toList())
              : colNames) {
            if (!handler.getTransactionalListeners().isEmpty()) {
              MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(), eventType,
                  new DeleteTableColumnStatEvent(catName, dbName, tableName, colName, engine, handler));
            }
            events.add(new DeleteTableColumnStatEvent(catName, dbName, tableName, colName, engine, handler));
          }
        }
      } else {
        List<String> partNames = new ArrayList<>();
        if (request.getPart_namesSize() > 0) {
          partNames.addAll(request.getPart_names());
        } else {
          partNames.addAll(ms.listPartitionNames(catName, dbName, tableName, (short) -1));
        }
        if (partNames.isEmpty()) {
          // no partition found, bail out early
          return new DeleteColumnStatisticsResult(true, events, eventType);
        }
        ret = ms.deletePartitionColumnStatistics(catName, dbName, tableName, partNames, colNames, engine);
        if (ret) {
          eventType = EventType.DELETE_PARTITION_COLUMN_STAT;
          for (String colName : colNames == null || colNames.isEmpty() ?
              table.getSd().getCols().stream().map(FieldSchema::getName).collect(Collectors.toList())
              : colNames) {
            for (String partName : partNames) {
              List<String> partVals = getPartValsFromName(table, partName);
              if (!handler.getTransactionalListeners().isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(), eventType,
                    new DeletePartitionColumnStatEvent(catName, dbName, tableName,
                        partName, partVals, colName, engine, handler));
              }
              events.add(new DeletePartitionColumnStatEvent(catName, dbName, tableName,
                  partName, partVals, colName, engine, handler));
            }
          }
        }
      }
      committed = ms.commitTransaction();
    } finally {
      if (!committed) {
        ms.rollbackTransaction();
      }
    }
    return new DeleteColumnStatisticsResult(ret, events, eventType);
  }

  @Override
  protected void afterExecute(DeleteColumnStatisticsResult result) throws TException, IOException {
    if (result != null && !handler.getListeners().isEmpty()) {
      for (ListenerEvent event : result.events()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(), result.eventType(), event);
      }
    }
    super.afterExecute(result);
  }

  @Override
  public String toString() {
    return "DeleteColumnStatisticsHandler [" + id + "] - delete column stats for " +
        TableName.getQualified(catName, dbName, tableName) + ":";
  }

  public record DeleteColumnStatisticsResult(boolean success, List<ListenerEvent> events, EventType eventType)
      implements Result {

  }
}
