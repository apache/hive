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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AppendPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.HMSHandler.getPartValsFromName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.canUpdateStats;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.updatePartitionStatsFast;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.validatePartitionNameCharacters;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@RequestHandler(requestBody = AppendPartitionsRequest.class)
public class AppendPartitionHandler
    extends AbstractRequestHandler<AppendPartitionsRequest, AppendPartitionHandler.AppendPartitionResult> {
  private static final Logger LOG = LoggerFactory.getLogger(AppendPartitionHandler.class);
  private RawStore ms;
  private String catName;
  private String dbName;
  private String tableName;
  private List<String> partVals;
  private Table tbl;
  private Warehouse wh;

  AppendPartitionHandler(IHMSHandler handler, AppendPartitionsRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    List<String> part_vals = request.getPartVals();
    dbName = normalizeIdentifier(request.getDbName());
    catName = normalizeIdentifier(request.isSetCatalogName() ?
        request.getCatalogName() : getDefaultCatalog(handler.getConf()));
    tableName = normalizeIdentifier(request.getTableName());
    String partName = request.getName();
    if (partName == null && (part_vals == null || part_vals.isEmpty())) {
      throw new MetaException("The partition values must not be null or empty.");
    }

    ms = handler.getMS();
    wh = handler.getWh();
    tbl = ms.getTable(catName, dbName, tableName,  null);
    if (tbl == null) {
      throw new InvalidObjectException(dbName + "." + tableName + " table not found");
    }
    if (tbl.getSd().getLocation() == null) {
      throw new MetaException("Cannot append a partition to a view");
    }
    if (part_vals == null || part_vals.isEmpty()) {
      // partition name is set, get partition vals and then append partition
      part_vals = getPartValsFromName(tbl, partName);
    }
    this.partVals = part_vals;
    Partition old_part;
    try {
      old_part = ms.getPartition(catName, dbName, tableName, partVals);
    } catch (NoSuchObjectException e) {
      // this means there is no existing partition
      old_part = null;
    }
    if (old_part != null) {
      throw new AlreadyExistsException("Partition already exists:" + part_vals);
    }
    LOG.debug("Append partition: {}", part_vals);
    validatePartitionNameCharacters(partVals, handler.getConf());
  }

  @Override
  protected AppendPartitionResult execute() throws TException, IOException {
    Partition part = new Partition();
    part.setCatName(catName);
    part.setDbName(dbName);
    part.setTableName(tableName);
    part.setValues(partVals);

    boolean success = false, madeDir = false;
    Path partLocation = null;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Database db = null;
    try {
      ms.openTransaction();
      db = handler.get_database_core(catName, dbName);
      ((HMSHandler) handler).firePreEvent(new PreAddPartitionEvent(tbl, part, handler));

      part.setSd(tbl.getSd().deepCopy());
      partLocation = new Path(tbl.getSd().getLocation(), Warehouse
          .makePartName(tbl.getPartitionKeys(), partVals));
      part.getSd().setLocation(partLocation.toString());

      if (!wh.isDir(partLocation)) {
        if (!wh.mkdirs(partLocation)) {
          throw new MetaException(partLocation
              + " is not a directory or unable to create one");
        }
        madeDir = true;
      }

      // set create time
      long time = System.currentTimeMillis() / 1000;
      part.setCreateTime((int) time);
      part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
      if (canUpdateStats(handler.getConf(), tbl)) {
        updatePartitionStatsFast(part, tbl, wh, madeDir, false, request.getEnvironmentContext(), true);
      }

      if (ms.addPartition(part)) {
        if (!handler.getTransactionalListeners().isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                  EventMessage.EventType.ADD_PARTITION,
                  new AddPartitionEvent(tbl, part, true, handler),
                  request.getEnvironmentContext());
        }

        success = ms.commitTransaction();
      }
    } finally {
      if (!success) {
        ms.rollbackTransaction();
        if (madeDir) {
          wh.deleteDir(partLocation, false, ReplChangeManager.shouldEnableCm(db, tbl));
        }
      }

      if (!handler.getListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
            EventMessage.EventType.ADD_PARTITION,
            new AddPartitionEvent(tbl, part, success, handler),
            request.getEnvironmentContext(),
            transactionalListenerResponses, ms);
      }
    }
    return new AppendPartitionResult(part, success);
  }

  @Override
  public String toString() {
    return "AppendPartitionHandler [" + id + "] -  Append partition for " +
        TableName.getQualified(catName, dbName, tableName) + ":";
  }

  public record AppendPartitionResult(Partition partition, boolean success) implements Result {

  }
}
