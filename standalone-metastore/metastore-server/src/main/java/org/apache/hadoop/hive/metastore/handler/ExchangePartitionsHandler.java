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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ExchangePartitionsRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

@RequestHandler(requestBody = ExchangePartitionsRequest.class)
public class ExchangePartitionsHandler
    extends AbstractRequestHandler<ExchangePartitionsRequest, ExchangePartitionsHandler.ExchangePartitionsResult> {
  private RawStore ms;
  private TableStore tableStore;
  private Warehouse wh;
  private Table sourceTable;
  private Table destinationTable;
  private Path sourcePath;
  private Path destPath;
  private List<Partition> partitionsToExchange;
  private TableName sourceName;
  private TableName destName;

  private Map<String, String> transactionalListenerResponsesForAddPartition = Collections.emptyMap();
  private final List<Map<String, String>> transactionalListenerResponsesForDropPartition = new ArrayList<>();

  ExchangePartitionsHandler(IHMSHandler handler, ExchangePartitionsRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    org.apache.hadoop.hive.metastore.api.TableName reqSource = request.getSourceTable();
    org.apache.hadoop.hive.metastore.api.TableName reqTarget = request.getTargetTable();
    if (request.getPartitionSpecs() == null || reqSource == null || reqTarget == null
        || reqSource.getDb_name() == null || reqSource.getTbl_name() == null
        || reqTarget.getDb_name() == null || reqTarget.getTbl_name() == null) {
      throw new MetaException("The DB and table name for the source and destination tables,"
          + " and the partition specs must not be null.");
    }

    String defaultCat = getDefaultCatalog(handler.getConf());
    if (!reqSource.isSetCat_name()) {
      reqSource.setCat_name(defaultCat);
    }
    if (!reqTarget.isSetCat_name()) {
      reqTarget.setCat_name(defaultCat);
    }
    if (!reqTarget.getCat_name().equals(reqSource.getCat_name())) {
      throw new MetaException("You cannot move a partition across catalogs");
    }

    sourceName = new TableName(reqSource.getCat_name(), reqSource.getDb_name(), reqSource.getTbl_name());
    destName = new TableName(reqTarget.getCat_name(), reqTarget.getDb_name(), reqTarget.getTbl_name());

    ms = handler.getMS();
    tableStore = ms.unwrap(TableStore.class);
    wh = handler.getWh();

    destinationTable = tableStore.getTable(destName, null, -1);
    if (destinationTable == null) {
      throw new MetaException("The destination table " + destName + " not found");
    }
    sourceTable = tableStore.getTable(sourceName, null, -1);
    if (sourceTable == null) {
      throw new MetaException("The source table " + sourceName + " not found");
    }

    ((HMSHandler) handler).firePreEvent(new PreReadTableEvent(sourceTable, handler));
    ((HMSHandler) handler).firePreEvent(new PreReadTableEvent(destinationTable, handler));

    List<String> partVals = MetaStoreUtils.getPvals(sourceTable.getPartitionKeys(), request.getPartitionSpecs());
    List<String> partValsPresent = new ArrayList<>();
    List<FieldSchema> partitionKeysPresent = new ArrayList<>();
    int i = 0;
    for (FieldSchema fs : sourceTable.getPartitionKeys()) {
      String partVal = partVals.get(i);
      if (partVal != null && !partVal.equals("")) {
        partValsPresent.add(partVal);
        partitionKeysPresent.add(fs);
      }
      i++;
    }
    GetPartitionsArgs getPartitionsArgs = new GetPartitionsArgs.GetPartitionsArgsBuilder()
        .part_vals(partVals)
        .max(-1)
        .build();
    partitionsToExchange = tableStore.listPartitionsPsWithAuth(sourceName, getPartitionsArgs);
    if (partitionsToExchange == null || partitionsToExchange.isEmpty()) {
      throw new MetaException("No partition is found with the values " + request.getPartitionSpecs()
          + " for the table " + sourceName.getTable());
    }
    boolean sameColumns = MetaStoreUtils.compareFieldColumns(
        sourceTable.getSd().getCols(), destinationTable.getSd().getCols());
    boolean samePartitions = MetaStoreUtils.compareFieldColumns(
        sourceTable.getPartitionKeys(), destinationTable.getPartitionKeys());
    if (!sameColumns || !samePartitions) {
      throw new MetaException("The tables have different schemas."
          + " Their partitions cannot be exchanged.");
    }

    sourcePath = new Path(sourceTable.getSd().getLocation(),
        Warehouse.makePartName(partitionKeysPresent, partValsPresent));
    destPath = new Path(destinationTable.getSd().getLocation(),
        Warehouse.makePartName(partitionKeysPresent, partValsPresent));

    List<String> destPartNames = new ArrayList<>();
    for (Partition partition : partitionsToExchange) {
      destPartNames.add(Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues()));
    }
    List<Partition> partsInDestTable = tableStore.getPartitionsByNames(destName,
        new GetPartitionsArgs.GetPartitionsArgsBuilder().partNames(destPartNames).build());
    if (!partsInDestTable.isEmpty()) {
      Partition partition = partsInDestTable.getFirst();
      throw new MetaException("The partition " + Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues())
          + " already exists in the table " + destName.getTable());
    }

    Database srcDb = ms.getDatabase(sourceName.getCat(), sourceName.getDb());
    Database destDb = ms.getDatabase(destName.getCat(), destName.getDb());
    if (!HiveMetaStore.isRenameAllowed(srcDb, destDb)) {
      throw new MetaException("Exchange partition not allowed for " + sourceName
          + " Dest db : " + destName.getDb());
    }
  }

  @Override
  protected void afterExecute(ExchangePartitionsResult result) throws TException, IOException {
    boolean success = result != null && result.success();
    List<Partition> destPartitions = result != null ? result.partitions() : Collections.emptyList();
    AddPartitionEvent addPartitionEvent = new AddPartitionEvent(destinationTable,
        destPartitions, success, handler);
    MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
        EventMessage.EventType.ADD_PARTITION,
        addPartitionEvent,
        null,
        transactionalListenerResponsesForAddPartition, ms);

    int i = 0;
    for (Partition partition : partitionsToExchange) {
      DropPartitionEvent dropPartitionEvent =
          new DropPartitionEvent(sourceTable, partition, success, true, handler);
      Map<String, String> parameters =
          (transactionalListenerResponsesForDropPartition.size() > i)
              ? transactionalListenerResponsesForDropPartition.get(i)
              : null;

      MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
          EventMessage.EventType.DROP_PARTITION,
          dropPartitionEvent,
          null,
          parameters, ms);
      i++;
    }
  }

  @Override
  protected ExchangePartitionsResult execute() throws TException, IOException {
    boolean success = false;
    boolean pathCreated = false;
    List<Partition> destPartitions = new ArrayList<>();
    ms.openTransaction();
    try {
      List<String> dropPartNames = new ArrayList<>();
      for (Partition partition : partitionsToExchange) {
        Partition destPartition = new Partition(partition);
        destPartition.setCatName(destName.getCat());
        destPartition.setDbName(destName.getDb());
        destPartition.setTableName(destinationTable.getTableName());
        Path destPartitionPath = new Path(destinationTable.getSd().getLocation(),
            Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues()));
        destPartition.getSd().setLocation(destPartitionPath.toString());
        ((HMSHandler) handler).firePreEvent(new PreAddPartitionEvent(destinationTable, destPartition, handler));
        destPartitions.add(destPartition);
        ((HMSHandler) handler).firePreEvent(new PreDropPartitionEvent(sourceTable, partition, true, handler));
        dropPartNames.add(Warehouse.makePartName(sourceTable.getPartitionKeys(), partition.getValues()));
      }
      tableStore.addPartitions(destName, destPartitions);
      tableStore.dropPartitions(sourceName, dropPartNames);
      Path destParentPath = destPath.getParent();
      if (!wh.isDir(destParentPath)) {
        if (!wh.mkdirs(destParentPath)) {
          throw new MetaException("Unable to create path " + destParentPath);
        }
      }
      /*
       * TODO: Use the hard link feature of hdfs
       * once https://issues.apache.org/jira/browse/HDFS-3370 is done
       */
      pathCreated = wh.renameDir(sourcePath, destPath, false);

      transactionalListenerResponsesForAddPartition =
          MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
              EventMessage.EventType.ADD_PARTITION,
              new AddPartitionEvent(destinationTable, destPartitions, true, handler));

      for (Partition partition : partitionsToExchange) {
        DropPartitionEvent dropPartitionEvent =
            new DropPartitionEvent(sourceTable, partition, true, true, handler);
        transactionalListenerResponsesForDropPartition.add(
            MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                EventMessage.EventType.DROP_PARTITION,
                dropPartitionEvent));
      }
      success = ms.commitTransaction();
      return new ExchangePartitionsResult(destPartitions, success);
    } finally {
      if (!success || !pathCreated) {
        ms.rollbackTransaction();
        if (pathCreated) {
          wh.renameDir(destPath, sourcePath, false);
        }
      }
    }
  }

  public record ExchangePartitionsResult(List<Partition> partitions, boolean success) implements Result {}

}
