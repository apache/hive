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
package org.apache.hadoop.hive.metastore.txn.jdbc.commands;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.txn.entities.OperationType;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InsertTxnComponentsCommand implements ParameterizedBatchCommand<Object[]> {
  
  private final LockRequest lockRequest;
  private final Map<Pair<String, String>, Long> writeIds;
  private final AddDynamicPartitions dynamicPartitions;

  public InsertTxnComponentsCommand(LockRequest lockRequest, Map<Pair<String, String>, Long> writeIds) {
    this.lockRequest = lockRequest;
    this.writeIds = writeIds;
    this.dynamicPartitions = null;
  }

  public InsertTxnComponentsCommand(AddDynamicPartitions dynamicPartitions) {
    this.dynamicPartitions = dynamicPartitions;
    this.lockRequest = null;
    this.writeIds = null;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    return "INSERT INTO \"TXN_COMPONENTS\" (" +
        "\"TC_TXNID\", \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_OPERATION_TYPE\", \"TC_WRITEID\")" +
        " VALUES (?, ?, ?, ?, ?, ?)";
  }

  @Override
  public List<Object[]> getQueryParameters() {
    return dynamicPartitions == null ? getQueryParametersByLockRequest() : getQueryParametersByDynamicPartitions();
  }

  @Override
  public ParameterizedPreparedStatementSetter<Object[]> getPreparedStatementSetter() {
    return (ps, argument) -> {
      ps.setLong(1, (Long)argument[0]);
      ps.setString(2, (String)argument[1]);
      ps.setString(3, (String)argument[2]);
      ps.setString(4, (String)argument[3]);
      ps.setString(5, (String)argument[4]);
      ps.setObject(6, argument[5], Types.BIGINT);
    };
  }

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return ParameterizedCommand.EXACTLY_ONE_ROW;
  }
  
  private List<Object[]> getQueryParametersByLockRequest() {
    assert lockRequest != null;
    List<Object[]> params = new ArrayList<>(lockRequest.getComponentSize());
    Set<Pair<String, String>> alreadyAddedTables = new HashSet<>();

    for (LockComponent lc : lockRequest.getComponent()) {
      if (lc.isSetIsTransactional() && !lc.isIsTransactional()) {
        //we don't prevent using non-acid resources in a txn, but we do lock them
        continue;
      }
      if (!shouldUpdateTxnComponent(lockRequest.getTxnid(), lockRequest, lc)) {
        continue;
      }

      Function<LockComponent, Pair<String, String>> getWriteIdKey = lockComponent ->
          Pair.of(StringUtils.lowerCase(lockComponent.getDbname()), StringUtils.lowerCase(lockComponent.getTablename()));

      String dbName = StringUtils.lowerCase(lc.getDbname());
      String tblName = StringUtils.lowerCase(lc.getTablename());
      String partName = TxnUtils.normalizePartitionCase(lc.getPartitionname());
      OperationType opType = OperationType.fromDataOperationType(lc.getOperationType());
      Pair<String, String> writeIdKey = getWriteIdKey.apply(lc);


      Predicate<LockComponent> isDynPart = lockComponent -> lockComponent.isSetIsDynamicPartitionWrite() && lockComponent.isIsDynamicPartitionWrite();
      Set<Pair<String, String>> isDynPartUpdate = lockRequest.getComponent().stream().filter(isDynPart)
          .filter(lockComponent -> lockComponent.getOperationType() == DataOperationType.UPDATE || lockComponent.getOperationType() == DataOperationType.DELETE)
          .map(getWriteIdKey)
          .collect(Collectors.toSet());

      if (isDynPart.test(lc)) {
        partName = null;
        if (alreadyAddedTables.contains(writeIdKey)) {
          continue;
        }
        opType = isDynPartUpdate.contains(writeIdKey) ? OperationType.UPDATE : OperationType.INSERT;
      }
      Long writeId = writeIds.get(writeIdKey);

      params.add(new Object[]{lockRequest.getTxnid(), dbName, tblName, partName, opType.getSqlConst(), writeId});
      alreadyAddedTables.add(writeIdKey);
    }
    return params;    
  }

  private List<Object[]> getQueryParametersByDynamicPartitions() {
    assert dynamicPartitions != null;
    //for RU this may be null so we should default it to 'u' which is most restrictive
    OperationType ot = OperationType.UPDATE;
    if (dynamicPartitions.isSetOperationType()) {
      ot = OperationType.fromDataOperationType(dynamicPartitions.getOperationType());
    }
    
    List<Object[]> params = new ArrayList<>(dynamicPartitions.getPartitionnamesSize());
    for (String partName : dynamicPartitions.getPartitionnames()) {
      params.add(new Object[]{
          dynamicPartitions.getTxnid(),
          dynamicPartitions.getDbname().toLowerCase(),
          dynamicPartitions.getTablename().toLowerCase(),
          partName,
          ot.getSqlConst(),
          dynamicPartitions.getWriteid()
      });
    }
    return params;
  }

  private boolean shouldUpdateTxnComponent(long txnid, LockRequest rqst, LockComponent lc) {
    if(!lc.isSetOperationType()) {
      //request came from old version of the client
      return true; //this matches old behavior
    }
    else {
      switch (lc.getOperationType()) {
        case INSERT:
        case UPDATE:
        case DELETE:
          return true;
        case SELECT:
          return false;
        case NO_TXN:
              /*this constant is a bit of a misnomer since we now always have a txn context.  It
               just means the operation is such that we don't care what tables/partitions it
               affected as it doesn't trigger a compaction or conflict detection.  A better name
               would be NON_TRANSACTIONAL.*/
          return false;
        default:
          //since we have an open transaction, only 4 values above are expected
          throw new IllegalStateException("Unexpected DataOperationType: " + lc.getOperationType()
              + " agentInfo=" + rqst.getAgentInfo() + " " + JavaUtils.txnIdToString(txnid));
      }
    }
  }
  
}