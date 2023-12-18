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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.common.MaterializationSnapshot;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GetMaterializationInvalidationInfoFunction implements TransactionalFunction<Materialization> {

  private static final Logger LOG = LoggerFactory.getLogger(GetMaterializationInvalidationInfoFunction.class);

  private final CreationMetadata creationMetadata;
  private final String validTxnListStr;

  public GetMaterializationInvalidationInfoFunction(CreationMetadata creationMetadata, String validTxnListStr) {
    this.creationMetadata = creationMetadata;
    this.validTxnListStr = validTxnListStr;
  }

  @Override
  public Materialization execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    if (creationMetadata.getTablesUsed().isEmpty()) {
      // Bail out
      LOG.warn("Materialization creation metadata does not contain any table");
      return null;
    }

    // We are composing a query that returns a single row if an update happened after
    // the materialization was created. Otherwise, query returns 0 rows.

    // Parse validReaderWriteIdList from creation metadata
    MaterializationSnapshot mvSnapshot = MaterializationSnapshot.fromJson(creationMetadata.getValidTxnList());
    if (mvSnapshot.getTableSnapshots() != null && !mvSnapshot.getTableSnapshots().isEmpty()) {
      // Incremental rebuild of MVs on Iceberg sources is not supported.
      return null;
    }
    final ValidTxnWriteIdList validReaderWriteIdList = new ValidTxnWriteIdList(mvSnapshot.getValidTxnList());

    // Parse validTxnList
    final ValidReadTxnList currentValidTxnList = new ValidReadTxnList(validTxnListStr);
    // Get the valid write id list for the tables in current state
    final List<TableValidWriteIds> currentTblValidWriteIdsList = new ArrayList<>();
    for (String fullTableName : creationMetadata.getTablesUsed()) {
      currentTblValidWriteIdsList.add(new GetValidWriteIdsForTableFunction(currentValidTxnList, fullTableName).execute(jdbcResource));
    }
    final ValidTxnWriteIdList currentValidReaderWriteIdList = TxnCommonUtils.createValidTxnWriteIdList(
        currentValidTxnList.getHighWatermark(), currentTblValidWriteIdsList);

    List<String> params = new ArrayList<>();
    StringBuilder queryUpdateDelete = new StringBuilder();
    StringBuilder queryCompletedCompactions = new StringBuilder();
    StringBuilder queryCompactionQueue = new StringBuilder();
    // compose a query that select transactions containing an update...
    queryUpdateDelete.append("SELECT \"CTC_UPDATE_DELETE\" FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_UPDATE_DELETE\" ='Y' AND (");
    queryCompletedCompactions.append("SELECT 1 FROM \"COMPLETED_COMPACTIONS\" WHERE (");
    queryCompactionQueue.append("SELECT 1 FROM \"COMPACTION_QUEUE\" WHERE (");
    int i = 0;
    for (String fullyQualifiedName : creationMetadata.getTablesUsed()) {
      ValidWriteIdList tblValidWriteIdList =
          validReaderWriteIdList.getTableValidWriteIdList(fullyQualifiedName);
      if (tblValidWriteIdList == null) {
        LOG.warn("ValidWriteIdList for table {} not present in creation metadata, this should not happen", fullyQualifiedName);
        return null;
      }

      // First, we check whether the low watermark has moved for any of the tables.
      // If it has, we return true, since it is not incrementally refreshable, e.g.,
      // one of the commits that are not available may be an update/delete.
      ValidWriteIdList currentTblValidWriteIdList =
          currentValidReaderWriteIdList.getTableValidWriteIdList(fullyQualifiedName);
      if (currentTblValidWriteIdList == null) {
        LOG.warn("Current ValidWriteIdList for table {} not present in creation metadata, this should not happen", fullyQualifiedName);
        return null;
      }
      if (!Objects.equals(currentTblValidWriteIdList.getMinOpenWriteId(), tblValidWriteIdList.getMinOpenWriteId())) {
        LOG.debug("Minimum open write id do not match for table {}", fullyQualifiedName);
        return null;
      }

      // ...for each of the tables that are part of the materialized view,
      // where the transaction had to be committed after the materialization was created...
      if (i != 0) {
        queryUpdateDelete.append("OR");
        queryCompletedCompactions.append("OR");
        queryCompactionQueue.append("OR");
      }
      String[] names = TxnUtils.getDbTableName(fullyQualifiedName);
      assert (names.length == 2);
      queryUpdateDelete.append(" (\"CTC_DATABASE\"=? AND \"CTC_TABLE\"=?");
      queryCompletedCompactions.append(" (\"CC_DATABASE\"=? AND \"CC_TABLE\"=?");
      queryCompactionQueue.append(" (\"CQ_DATABASE\"=? AND \"CQ_TABLE\"=?");
      params.add(names[0]);
      params.add(names[1]);
      queryUpdateDelete.append(" AND (\"CTC_WRITEID\" > " + tblValidWriteIdList.getHighWatermark());
      queryCompletedCompactions.append(" AND (\"CC_HIGHEST_WRITE_ID\" > " + tblValidWriteIdList.getHighWatermark());
      queryUpdateDelete.append(tblValidWriteIdList.getInvalidWriteIds().length == 0 ? ") " :
          " OR \"CTC_WRITEID\" IN(" + StringUtils.join(",",
              Arrays.asList(ArrayUtils.toObject(tblValidWriteIdList.getInvalidWriteIds()))) + ") ) ");
      queryCompletedCompactions.append(tblValidWriteIdList.getInvalidWriteIds().length == 0 ? ") " :
          " OR \"CC_HIGHEST_WRITE_ID\" IN(" + StringUtils.join(",",
              Arrays.asList(ArrayUtils.toObject(tblValidWriteIdList.getInvalidWriteIds()))) + ") ) ");
      queryUpdateDelete.append(") ");
      queryCompletedCompactions.append(") ");
      queryCompactionQueue.append(") ");
      i++;
    }
    // ... and where the transaction has already been committed as per snapshot taken
    // when we are running current query
    queryUpdateDelete.append(") AND \"CTC_TXNID\" <= " + currentValidTxnList.getHighWatermark());
    queryUpdateDelete.append(currentValidTxnList.getInvalidTransactions().length == 0 ? " " :
        " AND \"CTC_TXNID\" NOT IN(" + StringUtils.join(",",
            Arrays.asList(ArrayUtils.toObject(currentValidTxnList.getInvalidTransactions()))) + ") ");
    queryCompletedCompactions.append(")");
    queryCompactionQueue.append(") ");

    boolean hasUpdateDelete = executeBoolean(jdbcResource, queryUpdateDelete.toString(), params,
        "Unable to retrieve materialization invalidation information: completed transaction components.");

    // Execute query
    queryCompletedCompactions.append(" UNION ");
    queryCompletedCompactions.append(queryCompactionQueue.toString());
    List<String> paramsTwice = new ArrayList<>(params);
    paramsTwice.addAll(params);
    boolean hasCompaction = executeBoolean(jdbcResource, queryCompletedCompactions.toString(), paramsTwice,
        "Unable to retrieve materialization invalidation information: compactions");

    return new Materialization(hasUpdateDelete, hasCompaction);
  }

  private boolean executeBoolean(MultiDataSourceJdbcResource jdbcResource, String queryText, List<String> params, String errorMessage) throws MetaException {
    try (PreparedStatement pst = jdbcResource.getSqlGenerator().prepareStmtWithParameters(jdbcResource.getConnection(), queryText, params)) {
      LOG.debug("Going to execute query <{}>", queryText);
      pst.setMaxRows(1);
      try (ResultSet rs = pst.executeQuery()) {
        return rs.next();
      }
    } catch (SQLException ex) {
      LOG.warn(errorMessage, ex);
      throw new MetaException(errorMessage + " " + StringUtils.stringifyException(ex));
    }
  }

}
