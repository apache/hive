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

package org.apache.hadoop.hive.ql.ddl.table.storage.compact;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.txn.compactor.InitiatorBase;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.ql.io.AcidUtils.compactionTypeStr2ThriftType;

/**
 * Operation process of compacting a table.
 */
public class AlterTableCompactOperation extends DDLOperation<AlterTableCompactDesc> {
  public AlterTableCompactOperation(DDLOperationContext context, AlterTableCompactDesc desc) {
    super(context, desc);
  }

  @Override public int execute() throws Exception {
    Table table = context.getDb().getTable(desc.getTableName());
    if (!AcidUtils.isTransactionalTable(table) && !AcidUtils.isNonNativeAcidTable(table)) {
      throw new HiveException(ErrorMsg.NONACID_COMPACTION_NOT_SUPPORTED, table.getDbName(), table.getTableName());
    }

    CompactionRequest compactionRequest = new CompactionRequest(table.getDbName(), table.getTableName(),
        compactionTypeStr2ThriftType(desc.getCompactionType()));

    compactionRequest.setPoolName(desc.getPoolName());
    compactionRequest.setProperties(desc.getProperties());
    compactionRequest.setInitiatorId(JavaUtils.hostname() + "-" + HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION);
    compactionRequest.setInitiatorVersion(HiveMetaStoreClient.class.getPackage().getImplementationVersion());
    compactionRequest.setOrderByClause(desc.getOrderByClause());

    if (desc.getNumberOfBuckets() > 0) {
      compactionRequest.setNumberOfBuckets(desc.getNumberOfBuckets());
    }

    InitiatorBase initiatorBase = new InitiatorBase();
    initiatorBase.setConf(context.getConf());
    initiatorBase.init(new AtomicBoolean());

    Map<String, org.apache.hadoop.hive.metastore.api.Partition> partitionMap =
        convertPartitionsFromThriftToDB(getPartitions(table, desc, context));

    if(desc.getPartitionSpec() != null){
      Optional<String> partitionName =  partitionMap.keySet().stream().findFirst();
      partitionName.ifPresent(compactionRequest::setPartitionname);
    }
    List<CompactionResponse> compactionResponses =
        initiatorBase.initiateCompactionForTable(compactionRequest, table.getTTable(), partitionMap);
    for (CompactionResponse compactionResponse : compactionResponses) {
      if (!compactionResponse.isAccepted()) {
        String message;
        if (compactionResponse.isSetErrormessage()) {
          message = compactionResponse.getErrormessage();
          throw new HiveException(ErrorMsg.COMPACTION_REFUSED, table.getDbName(), table.getTableName(),
              "CompactionId: " + compactionResponse.getId(), message);
        }
        context.getConsole().printInfo(
            "Compaction already enqueued with id " + compactionResponse.getId() + "; State is "
                + compactionResponse.getState());
        continue;
      }
      context.getConsole().printInfo("Compaction enqueued with id " + compactionResponse.getId());
      if (desc.isBlocking() && compactionResponse.isAccepted()) {
        waitForCompactionToFinish(compactionResponse, context);
      }
    }
    return 0;
  }

  private List<Partition> getPartitions(Table table, AlterTableCompactDesc desc, DDLOperationContext context)
      throws HiveException {
    List<Partition> partitions = new ArrayList<>();

    if (desc.getPartitionSpec() == null) {
      if (table.isPartitioned()) {
        // Compaction will get initiated for all the potential partitions that meets the criteria
        partitions = context.getDb().getPartitions(table);
      }
    } else {
      Map<String, String> partitionSpec = desc.getPartitionSpec();
      partitions = context.getDb().getPartitions(table, partitionSpec);
      if (partitions.size() > 1) {
        throw new HiveException(ErrorMsg.TOO_MANY_COMPACTION_PARTITIONS);
      } else if (partitions.size() == 0) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION_SPEC);
      }
    }
    return partitions;
  }

  private Map<String, org.apache.hadoop.hive.metastore.api.Partition> convertPartitionsFromThriftToDB(
      List<Partition> partitions) {
    Map<String, org.apache.hadoop.hive.metastore.api.Partition> partitionMap = new LinkedHashMap<>();
    partitions.forEach(partition -> partitionMap.put(partition.getName(), partition.getTPartition()));
    return partitionMap;
  }

  private void waitForCompactionToFinish(CompactionResponse resp, DDLOperationContext context) throws HiveException {
    StringBuilder progressDots = new StringBuilder();
    long waitTimeMs = 1000;
    long waitTimeOut = HiveConf.getLongVar(context.getConf(), HiveConf.ConfVars.HIVE_COMPACTOR_WAIT_TIMEOUT);
    wait:
    while (true) {
      //double wait time until 5min
      waitTimeMs = waitTimeMs * 2;
      waitTimeMs = Math.min(waitTimeMs, waitTimeOut);
      try {
        Thread.sleep(waitTimeMs);
      } catch (InterruptedException ex) {
        context.getConsole().printInfo("Interrupted while waiting for compaction with id=" + resp.getId());
        break;
      }
      ShowCompactRequest request = new ShowCompactRequest();
      request.setId(resp.getId());

      ShowCompactResponse compaction = context.getDb().showCompactions(request);
      if (compaction.getCompactsSize() == 1) {
        ShowCompactResponseElement comp = compaction.getCompacts().get(0);
        LOG.debug("Response for cid: "+comp.getId()+" is "+comp.getState());
        switch (comp.getState()) {
          case TxnStore.WORKING_RESPONSE:
          case TxnStore.INITIATED_RESPONSE:
            //still working
            context.getConsole().printInfo(progressDots.toString());
            progressDots.append(".");
            continue wait;
          default:
            //done
            context.getConsole()
                .printInfo("Compaction with id " + resp.getId() + " finished with status: " + comp.getState());
            break wait;
        }
      } else {
        throw new HiveException("No suitable compaction found");
      }
    }
  }
}
