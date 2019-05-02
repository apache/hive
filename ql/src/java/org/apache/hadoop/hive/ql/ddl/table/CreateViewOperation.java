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

package org.apache.hadoop.hive.ql.ddl.table;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.ImmutableSet;

/**
 * Operation process of creating a view.
 */
public class CreateViewOperation extends DDLOperation {
  private final CreateViewDesc desc;

  public CreateViewOperation(DDLOperationContext context, CreateViewDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws HiveException {
    Table oldview = context.getDb().getTable(desc.getViewName(), false);
    if (oldview != null) {
      // Check whether we are replicating
      if (desc.getReplicationSpec().isInReplicationScope()) {
        // if this is a replication spec, then replace-mode semantics might apply.
        if (desc.getReplicationSpec().allowEventReplacementInto(oldview.getParameters())){
          desc.setReplace(true); // we replace existing view.
        } else {
          LOG.debug("DDLTask: Create View is skipped as view {} is newer than update",
              desc.getViewName()); // no replacement, the existing table state is newer than our update.
          return 0;
        }
      }

      if (!desc.isReplace()) {
        if (desc.getIfNotExists()) {
          return 0;
        }

        // View already exists, thus we should be replacing
        throw new HiveException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(desc.getViewName()));
      }

      // It should not be a materialized view
      assert !desc.isMaterialized();

      // replace existing view
      // remove the existing partition columns from the field schema
      oldview.setViewOriginalText(desc.getViewOriginalText());
      oldview.setViewExpandedText(desc.getViewExpandedText());
      oldview.setFields(desc.getSchema());
      if (desc.getComment() != null) {
        oldview.setProperty("comment", desc.getComment());
      }
      if (desc.getTblProps() != null) {
        oldview.getTTable().getParameters().putAll(desc.getTblProps());
      }
      oldview.setPartCols(desc.getPartCols());
      if (desc.getInputFormat() != null) {
        oldview.setInputFormatClass(desc.getInputFormat());
      }
      if (desc.getOutputFormat() != null) {
        oldview.setOutputFormatClass(desc.getOutputFormat());
      }
      oldview.checkValidity(null);
      if (desc.getOwnerName() != null) {
        oldview.setOwner(desc.getOwnerName());
      }
      context.getDb().alterTable(desc.getViewName(), oldview, false, null, true);
      DDLUtils.addIfAbsentByName(new WriteEntity(oldview, WriteEntity.WriteType.DDL_NO_LOCK),
          context.getWork().getOutputs());
    } else {
      // We create new view
      Table tbl = desc.toTable(context.getConf());
      // We set the signature for the view if it is a materialized view
      if (tbl.isMaterializedView()) {
        CreationMetadata cm =
            new CreationMetadata(MetaStoreUtils.getDefaultCatalog(context.getConf()), tbl.getDbName(),
                tbl.getTableName(), ImmutableSet.copyOf(desc.getTablesUsed()));
        cm.setValidTxnList(context.getConf().get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
        tbl.getTTable().setCreationMetadata(cm);
      }
      context.getDb().createTable(tbl, desc.getIfNotExists());
      DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK),
          context.getWork().getOutputs());

      //set lineage info
      DataContainer dc = new DataContainer(tbl.getTTable());
      context.getQueryState().getLineageState().setLineage(new Path(desc.getViewName()), dc, tbl.getCols());
    }
    return 0;
  }
}
