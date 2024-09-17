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

package org.apache.hadoop.hive.ql.ddl.process.show.compactions;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import static org.apache.commons.collections.MapUtils.isNotEmpty;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.NO_VAL;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getHostFromId;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getThreadIdFromId;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;

import static org.apache.hadoop.hive.ql.io.AcidUtils.compactionStateStr2Enum;
import static org.apache.hadoop.hive.ql.io.AcidUtils.compactionTypeStr2ThriftType;

/**
 * Operation process of showing compactions.
 */
public class ShowCompactionsOperation extends DDLOperation<ShowCompactionsDesc> {
  public ShowCompactionsOperation(DDLOperationContext context, ShowCompactionsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    SessionState sessionState = SessionState.get();
    // Call the metastore to get the status of all known compactions (completed get purged eventually)
    ShowCompactRequest request = getShowCompactioRequest(desc);
    ShowCompactResponse rsp = context.getDb().showCompactions(request);
    // Write the results into the file
    try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      // Write a header for cliDriver
      if (!sessionState.isHiveServerQuery()) {
        writeHeader(os);
      }

      if (rsp.getCompacts() != null) {
        for (ShowCompactResponseElement e : rsp.getCompacts()) {
          writeRow(os, e);
        }
      }
    } catch (IOException e) {
      LOG.warn("show compactions: ", e);
      return 1;
    }
    return 0;
  }

  private ShowCompactRequest getShowCompactioRequest(ShowCompactionsDesc desc) throws SemanticException {
    ShowCompactRequest request = new ShowCompactRequest();
    if (isBlank(desc.getDbName()) && isNotBlank(desc.getTbName())) {
      request.setDbName(SessionState.get().getCurrentDatabase());
    } else {
      request.setDbName(desc.getDbName());
    }
    if (isNotBlank(desc.getTbName())) {
      request.setTbName(desc.getTbName());
    }
    if (isNotBlank(desc.getPoolName())) {
      request.setPoolName(desc.getPoolName());
    }
    if (isNotBlank(desc.getCompactionType())) {
      request.setType(compactionTypeStr2ThriftType(desc.getCompactionType()));
    }
    if (isNotBlank(desc.getCompactionStatus())) {
      request.setState(compactionStateStr2Enum(desc.getCompactionStatus()).getSqlConst());
    }
    if (isNotEmpty(desc.getPartSpec())) {
      request.setPartName(AcidUtils.getPartitionName(desc.getPartSpec()));
    }
    if(desc.getCompactionId()>0){
     request.setId(desc.getCompactionId());
    }
    if (desc.getLimit()>0) {
      request.setLimit(desc.getLimit());
    }
    if (isNotBlank(desc.getOrderBy())) {
      request.setOrder(desc.getOrderBy());
    }
    return request;
  }

  private void writeHeader(DataOutputStream os) throws IOException {
    os.writeBytes("CompactionId");
    os.write(Utilities.tabCode);
    os.writeBytes("Database");
    os.write(Utilities.tabCode);
    os.writeBytes("Table");
    os.write(Utilities.tabCode);
    os.writeBytes("Partition");
    os.write(Utilities.tabCode);
    os.writeBytes("Type");
    os.write(Utilities.tabCode);
    os.writeBytes("State");
    os.write(Utilities.tabCode);
    os.writeBytes("Worker host");
    os.write(Utilities.tabCode);
    os.writeBytes("Worker");
    os.write(Utilities.tabCode);
    os.writeBytes("Enqueue Time");
    os.write(Utilities.tabCode);
    os.writeBytes("Start Time");
    os.write(Utilities.tabCode);
    os.writeBytes("Duration(ms)");
    os.write(Utilities.tabCode);
    os.writeBytes("HadoopJobId");
    os.write(Utilities.tabCode);
    os.writeBytes("Error message");
    os.write(Utilities.tabCode);
    os.writeBytes("Initiator host");
    os.write(Utilities.tabCode);
    os.writeBytes("Initiator");
    os.write(Utilities.tabCode);
    os.writeBytes("Pool name");
    os.write(Utilities.tabCode);
    os.writeBytes("TxnId");
    os.write(Utilities.tabCode);
    os.writeBytes("Next TxnId");
    os.write(Utilities.tabCode);
    os.writeBytes("Commit Time");
    os.write(Utilities.tabCode);
    os.writeBytes("Highest WriteId");
    os.write(Utilities.newLineCode);
  }

  private void writeRow(DataOutputStream os, ShowCompactResponseElement e) throws IOException {
    os.writeBytes(Long.toString(e.getId()));
    os.write(Utilities.tabCode);
    os.writeBytes(e.getDbname());
    os.write(Utilities.tabCode);
    os.writeBytes(e.getTablename());
    os.write(Utilities.tabCode);
    String part = e.getPartitionname();
    os.writeBytes(part == null ? NO_VAL : part);
    os.write(Utilities.tabCode);
    os.writeBytes(e.getType().toString());
    os.write(Utilities.tabCode);
    os.writeBytes(e.getState());
    os.write(Utilities.tabCode);
    os.writeBytes(getHostFromId(e.getWorkerid()));
    os.write(Utilities.tabCode);
    os.writeBytes(getThreadIdFromId(e.getWorkerid()));
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetEnqueueTime() ? Long.toString(e.getEnqueueTime()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetStart() ? Long.toString(e.getStart()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetEndTime() ? Long.toString(e.getEndTime() - e.getStart()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetHadoopJobId() ?  e.getHadoopJobId() : NO_VAL);
    os.write(Utilities.tabCode);
    String error = e.getErrorMessage();
    os.writeBytes(error == null ? NO_VAL : error);
    os.write(Utilities.tabCode);
    os.writeBytes(getHostFromId(e.getInitiatorId()));
    os.write(Utilities.tabCode);
    os.writeBytes(getThreadIdFromId(e.getInitiatorId()));
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetPoolName() ? e.getPoolName() : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetTxnId() ? Long.toString(e.getTxnId()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetNextTxnId() ? Long.toString(e.getNextTxnId()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetCommitTime() ? Long.toString(e.getCommitTime()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetHightestWriteId() ? Long.toString(e.getHightestWriteId()) : NO_VAL);
    os.write(Utilities.newLineCode);
  }
}
