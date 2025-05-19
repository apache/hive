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

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.io.AcidUtils.compactionTypeStr2ThriftType;

/**
 * Operation process of compacting a table.
 */
public class AlterTableCompactOperation extends DDLOperation<AlterTableCompactDesc> {

  private static MetadataCache metadataCache = new MetadataCache(true);
      
  public AlterTableCompactOperation(DDLOperationContext context, AlterTableCompactDesc desc) {
    super(context, desc);
  }

  @Override public int execute() throws Exception {
    Table table = context.getDb().getTable(desc.getTableName());
    if (!AcidUtils.isTransactionalTable(table) && !AcidUtils.isNonNativeAcidTable(table)) {
      throw new HiveException(ErrorMsg.NONACID_COMPACTION_NOT_SUPPORTED, 
          table.getDbName(), table.getTableName());
    }

    if (desc.getFilterExpr() != null) {
      if (!DDLUtils.isIcebergTable(table)) {
        throw new HiveException(ErrorMsg.NONICEBERG_COMPACTION_WITH_FILTER_NOT_SUPPORTED, 
            table.getDbName(), table.getTableName());
      }
      else if (desc.getPartitionSpec() != null) {
        throw new HiveException(ErrorMsg.ICEBERG_COMPACTION_WITH_PART_SPEC_AND_FILTER_NOT_SUPPORTED, 
            table.getDbName(), table.getTableName());
      }
    }

    if (desc.getPartitionSpec() != null && DDLUtils.hasTransformsInPartitionSpec(table)) {
      throw new HiveException(ErrorMsg.COMPACTION_NON_IDENTITY_PARTITION_SPEC, 
          table.getDbName(), table.getTableName());
    }

    Map<String, org.apache.hadoop.hive.metastore.api.Partition> partitionMap =
        convertPartitionsFromThriftToDB(getPartitions(table));

    TxnStore txnHandler = TxnUtils.getTxnStore(context.getConf());

    CompactionRequest compactionRequest = new CompactionRequest(table.getDbName(), table.getTableName(),
        compactionTypeStr2ThriftType(desc.getCompactionType()));

    String poolName = ObjectUtils.defaultIfNull(desc.getPoolName(),
        CompactorUtil.getPoolName(context.getConf(), table.getTTable(), metadataCache));

    compactionRequest.setPoolName(poolName);
    compactionRequest.setProperties(desc.getProperties());
    compactionRequest.setInitiatorId(JavaUtils.hostname() + "-" + HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION);
    compactionRequest.setInitiatorVersion(HiveMetaStoreClient.class.getPackage().getImplementationVersion());
    compactionRequest.setOrderByClause(desc.getOrderByClause());

    if (desc.getNumberOfBuckets() > 0) {
      compactionRequest.setNumberOfBuckets(desc.getNumberOfBuckets());
    }

    // End if filter doesn't match any data on the unpartitioned table    
    if (desc.getPartitionSpec() == null && desc.getFilterExpr() != null 
        && !table.getStorageHandler().hasDataMatchingFilterExpr(table, desc.getFilterExpr())) {
      return 0;
    }

    //Will directly initiate compaction if an un-partitioned table/a partition is specified in the request
    if (desc.getPartitionSpec() != null || !table.isPartitioned()) {
      if (desc.getPartitionSpec() != null) {
        Optional<String> partitionName = partitionMap.keySet().stream().findFirst();
        partitionName.ifPresent(compactionRequest::setPartitionname);
      }
      CompactionResponse compactionResponse = txnHandler.compact(compactionRequest);
      parseCompactionResponse(compactionResponse, table, compactionRequest.getPartitionname());
    } else { // Check for eligible partitions and initiate compaction
      for (Map.Entry<String, org.apache.hadoop.hive.metastore.api.Partition> partitionMapEntry : partitionMap.entrySet()) {
        compactionRequest.setPartitionname(partitionMapEntry.getKey());
        CompactionResponse compactionResponse =
            CompactorUtil.initiateCompactionForPartition(table.getTTable(), partitionMapEntry.getValue(),
                compactionRequest, ServerUtils.hostname(), txnHandler, context.getConf());
        parseCompactionResponse(compactionResponse, table, partitionMapEntry.getKey());
      }
      // If Iceberg table had partition evolution, it will create compaction request without partition specification,
      // and it will compact all files from old partition specs, besides compacting partitions of current spec in parallel.
      if (DDLUtils.isIcebergTable(table) && table.getStorageHandler().hasUndergonePartitionEvolution(table) && 
          (desc.getFilterExpr() == null || !table.getStorageHandler()
              .getPartitionsByExpr(table, desc.getFilterExpr(), false).isEmpty())) {
        compactionRequest.setPartitionname(null);
        CompactionResponse compactionResponse = txnHandler.compact(compactionRequest);
        parseCompactionResponse(compactionResponse, table, compactionRequest.getPartitionname());
      }
    }
    return 0;
  }

  private void parseCompactionResponse(CompactionResponse compactionResponse, Table table, String partitionName)
      throws HiveException {
    if (compactionResponse == null) {
      context.getConsole().printInfo(
          "Not enough deltas to initiate compaction for table=" + table.getTableName() + "partition=" + partitionName);
      return;
    }
    if (!compactionResponse.isAccepted()) {
      if (compactionResponse.isSetErrormessage()) {
        throw new HiveException(ErrorMsg.COMPACTION_REFUSED, table.getDbName(), table.getTableName(),
            partitionName == null ? "" : " partition(" + partitionName + ")", compactionResponse.getErrormessage());
      }
      context.getConsole().printInfo(
          "Compaction already enqueued with id " + compactionResponse.getId() + "; State is " + compactionResponse.getState());
      return;
    }
    context.getConsole().printInfo("Compaction enqueued with id " + compactionResponse.getId());
    if (desc.isBlocking() && compactionResponse.isAccepted()) {
      waitForCompactionToFinish(compactionResponse, context);
    }
  }

  private List<Partition> getPartitions(Table table) throws HiveException {
    List<Partition> partitions = new ArrayList<>();

    if (desc.getPartitionSpec() == null && table.isPartitioned()) {
      if (DDLUtils.isIcebergTable(table)) {
        HiveStorageHandler sh = table.getStorageHandler();
        
        partitions = (desc.getFilterExpr() != null) ?
            sh.getPartitionsByExpr(table, desc.getFilterExpr(), true) :
            sh.getPartitions(table, Maps.newHashMap(), true);
      } else {
        partitions = context.getDb().getPartitions(table);
      }
    } else if (desc.getPartitionSpec() != null) {
      Map<String, String> partitionSpec = desc.getPartitionSpec();
      partitions = context.getDb().getPartitions(table, partitionSpec);
      if (partitions.isEmpty()) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION_SPEC);
      }
      // This validates that the partition spec given in the compaction command matches exactly one partition 
      // in the table, not a partial partition spec.
      partitions = partitions.stream().filter(part -> part.getSpec().size() == partitionSpec.size())
          .collect(Collectors.toList());
      if (partitions.size() != 1) {
        throw new HiveException(ErrorMsg.TOO_MANY_COMPACTION_PARTITIONS);
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
