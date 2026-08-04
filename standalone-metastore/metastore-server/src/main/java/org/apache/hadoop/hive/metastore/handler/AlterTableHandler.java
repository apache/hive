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
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreMetadataTransformer;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

@SuppressWarnings("unused")
@RequestHandler(requestBody = AlterTableRequest.class)
public class AlterTableHandler
    extends AbstractRequestHandler<AlterTableRequest, AlterTableHandler.AlterTableResult> {
  private String catName;
  private String dbname;
  private String name;
  private Table newTable;
  private EnvironmentContext envContext;
  private String validWriteIdList;
  private List<String> processorCapabilities;
  private String processorId;

  AlterTableHandler(IHMSHandler handler, AlterTableRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.catName = request.isSetCatName() && request.getCatName() != null
        ? request.getCatName() : getDefaultCatalog(handler.getConf());
    this.dbname = request.getDbName();
    this.name = request.getTableName();
    this.newTable = request.getTable();
    this.validWriteIdList = request.getValidWriteIdList();
    this.processorCapabilities = request.getProcessorCapabilities();
    this.processorId = request.getProcessorIdentifier();

    // Build envContext, embedding expected-parameter hints so HiveAlterHandler can read them
    // without requiring an API change.
    this.envContext = request.getEnvironmentContext() != null
        ? request.getEnvironmentContext() : new EnvironmentContext();
    if (request.getExpectedParameterKey() != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY,
          request.getExpectedParameterKey());
    }
    if (request.getExpectedParameterValue() != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE,
          request.getExpectedParameterValue());
    }

    // HIVE-25282: Drop/Alter table in REMOTE db should fail
    try {
      Database db = handler.get_database_core(catName, dbname);
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        throw new MetaException("Alter table in REMOTE database " + db.getName() + " is not allowed");
      }
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException("Alter table in REMOTE database is not allowed");
    }

    // Update the time if it hasn't been specified.
    if (newTable.getParameters() == null
        || newTable.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
      newTable.putToParameters(hive_metastoreConstants.DDL_TIME,
          Long.toString(System.currentTimeMillis() / 1000));
    }

    // Normalise the new table location by adding missing scheme/authority.
    if (newTable.getSd() != null) {
      String newLocation = newTable.getSd().getLocation();
      if (StringUtils.isNotEmpty(newLocation)) {
        Path tblPath = handler.getWh().getDnsPath(new Path(newLocation));
        newTable.getSd().setLocation(tblPath.toString());
      }
    }

    // Ensure the catalog name is set on the new table.
    if (!newTable.isSetCatName()) {
      newTable.setCatName(catName);
    }

    // Fetch the current table so we can pass it to the pre-event and transformer.
    GetTableRequest getReq = new GetTableRequest(dbname, name);
    getReq.setCatName(catName);
    Table oldt = handler.get_table_core(getReq);

    IMetaStoreMetadataTransformer transformer = handler.getMetadataTransformer();
    if (transformer != null) {
      newTable = transformer.transformAlterTable(oldt, newTable, processorCapabilities, processorId);
    }

    ((HMSHandler) handler).firePreEvent(new PreAlterTableEvent(oldt, newTable, handler));
  }

  @Override
  protected AlterTableResult execute() throws TException, IOException {
    handler.getAlterHandler().alterTable(handler.getMS(), handler.getWh(),
        catName, dbname, name, newTable, envContext, handler, validWriteIdList);
    return new AlterTableResult(true);
  }

  @Override
  protected void afterExecute(AlterTableResult result) throws TException, IOException {
    // HiveAlterHandler fires both transactional and regular listeners internally.
    super.afterExecute(result);
  }

  @Override
  public String toString() {
    return "AlterTableHandler [" + id + "] - alter table "
        + TableName.getQualified(catName, dbname, name) + ":";
  }

  public record AlterTableResult(boolean success) implements Result {
  }
}
