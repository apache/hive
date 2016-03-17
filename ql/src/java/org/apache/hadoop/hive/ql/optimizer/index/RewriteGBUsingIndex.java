/**
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

package org.apache.hadoop.hive.ql.optimizer.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.AggregateIndexHandler;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;


/**
 * RewriteGBUsingIndex is implemented as one of the Rule-based Optimizations.
 * Implements optimizations for GroupBy clause rewrite using aggregate index.
 * This optimization rewrites GroupBy query over base table to the query over simple table-scan
 * over index table, if there is index on the group by key(s) or the distinct column(s).
 * E.g.
 * <code>
 *   select count(key)
 *   from table
 *   group by key;
 * </code>
 *  to
 *  <code>
 *   select sum(_count_of_key)
 *   from idx_table
 *   group by key;
 *  </code>
 *
 *  The rewrite supports following queries:
 *  <ul>
 *  <li> Queries having only those col refs that are in the index key.
 *  <li> Queries that have index key col refs
 *  <ul>
 *    <li> in SELECT
 *    <li> in WHERE
 *    <li> in GROUP BY
 *  </ul>
 *  <li> Queries with agg func COUNT(index key col ref) in SELECT
 *  <li> Queries with SELECT DISTINCT index_key_col_refs
 *  <li> Queries having a subquery satisfying above condition (only the subquery is rewritten)
 *  </ul>
 *
 *  @see AggregateIndexHandler
 *  @see IndexUtils
 *  @see RewriteCanApplyCtx
 *  @see RewriteCanApplyProcFactory
 *  @see RewriteParseContextGenerator
 *  @see RewriteQueryUsingAggregateIndexCtx
 *  @see RewriteQueryUsingAggregateIndex
 *  For test cases, @see ql_rewrite_gbtoidx.q
 */

public class RewriteGBUsingIndex extends Transform {
  private ParseContext parseContext;
  // Assumes one instance of this + single-threaded compilation for each query.
  private Hive hiveDb;
  private HiveConf hiveConf;
  private static final Logger LOG = LoggerFactory.getLogger(RewriteGBUsingIndex.class.getName());

  /*
   * Stores the list of top TableScanOperator names for which the rewrite
   * can be applied and the action that needs to be performed for operator tree
   * starting from this TableScanOperator
   */
  private final Map<String, RewriteCanApplyCtx> tsOpToProcess =
    new LinkedHashMap<String, RewriteCanApplyCtx>();

  //Index Validation Variables
  private static final String IDX_BUCKET_COL = "_bucketname";
  private static final String IDX_OFFSETS_ARRAY_COL = "_offsets";


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    parseContext = pctx;
    hiveConf = parseContext.getConf();
    try {
      hiveDb = Hive.get(hiveConf);
    } catch (HiveException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }

    // Don't try to index optimize the query to build the index
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTINDEXFILTER, false);

    /* Check if the input query passes all the tests to be eligible for a rewrite
     * If yes, rewrite original query; else, return the current parseContext
     */
    if (shouldApplyOptimization()) {
      LOG.info("Rewriting Original Query using " + getName() + " optimization.");
      rewriteOriginalQuery();
    }
    return parseContext;
  }

  private String getName() {
    return "RewriteGBUsingIndex";
  }

  /**
   * We traverse the current operator tree to check for conditions in which the
   * optimization cannot be applied.
   *
   * At the end, we check if all conditions have passed for rewrite. If yes, we
   * determine if the the index is usable for rewrite. Else, we log the condition which
   * did not meet the rewrite criterion.
   *
   * @return
   * @throws SemanticException
   */
  boolean shouldApplyOptimization() throws SemanticException {
    Map<Table, List<Index>> tableToIndex = getIndexesForRewrite();
    if (tableToIndex.isEmpty()) {
      LOG.debug("No Valid Index Found to apply Rewrite, " +
          "skipping " + getName() + " optimization");
      return false;
    }
    /*
     * This code iterates over each TableScanOperator from the topOps map from ParseContext.
     * For each operator tree originating from this top TableScanOperator, we determine
     * if the optimization can be applied. If yes, we add the name of the top table to
     * the tsOpToProcess to apply rewrite later on.
     * */
    for (Map.Entry<String, TableScanOperator> entry : parseContext.getTopOps().entrySet()) {
      String alias = entry.getKey();
      TableScanOperator topOp = entry.getValue();
      Table table = topOp.getConf().getTableMetadata();
      List<Index> indexes = tableToIndex.get(table);
      if (indexes.isEmpty()) {
        continue;
      }
      if (table.isPartitioned()) {
        //if base table has partitions, we need to check if index is built for
        //all partitions. If not, then we do not apply the optimization
        if (!checkIfIndexBuiltOnAllTablePartitions(topOp, indexes)) {
          LOG.debug("Index is not built for all table partitions, " +
              "skipping " + getName() + " optimization");
          continue;
        }
      }
      //check if rewrite can be applied for operator tree
      //if there are no partitions on base table
      checkIfRewriteCanBeApplied(alias, topOp, table, indexes);
    }
    return !tsOpToProcess.isEmpty();
  }

  /**
   * This methods checks if rewrite can be applied using the index and also
   * verifies all conditions of the operator tree.
   *
   * @param topOp - TableScanOperator for a single the operator tree branch
   * @param indexes - Map of a table and list of indexes on it
   * @return - true if rewrite can be applied on the current branch; false otherwise
   * @throws SemanticException
   */
  private boolean checkIfRewriteCanBeApplied(String alias, TableScanOperator topOp,
      Table baseTable, List<Index> indexes) throws SemanticException{
    //Context for checking if this optimization can be applied to the input query
    RewriteCanApplyCtx canApplyCtx = RewriteCanApplyCtx.getInstance(parseContext);
    canApplyCtx.setAlias(alias);
    canApplyCtx.setBaseTableName(baseTable.getTableName());
    canApplyCtx.populateRewriteVars(topOp);
    Map<Index, String> indexTableMap = getIndexToKeysMap(indexes);
    for (Map.Entry<Index, String> entry : indexTableMap.entrySet()) {
      //we rewrite the original query using the first valid index encountered
      //this can be changed if we have a better mechanism to
      //decide which index will produce a better rewrite
      Index index = entry.getKey();
      String indexKeyName = entry.getValue();
      //break here if any valid index is found to apply rewrite
      if (canApplyCtx.getIndexKey() != null && canApplyCtx.getIndexKey().equals(indexKeyName)
          && checkIfAllRewriteCriteriaIsMet(canApplyCtx)) {
        canApplyCtx.setAggFunction("_count_of_" + indexKeyName + "");
        canApplyCtx.addTable(canApplyCtx.getBaseTableName(), index.getIndexTableName());
        canApplyCtx.setIndexTableName(index.getIndexTableName());
        tsOpToProcess.put(alias, canApplyCtx);
        return true;
      }
    }
    return false;
  }

  /**
   * Get a list of indexes which can be used for rewrite.
   * @return
   * @throws SemanticException
   */
  private Map<Table, List<Index>> getIndexesForRewrite() throws SemanticException{
    List<String> supportedIndexes = new ArrayList<String>();
    supportedIndexes.add(AggregateIndexHandler.class.getName());

    // query the metastore to know what columns we have indexed
    Collection<TableScanOperator> topTables = parseContext.getTopOps().values();
    Map<Table, List<Index>> indexes = new HashMap<Table, List<Index>>();
    for (TableScanOperator op : topTables) {
      TableScanOperator tsOP = op;
      List<Index> tblIndexes = IndexUtils.getIndexes(tsOP.getConf().getTableMetadata(),
          supportedIndexes);
      if (tblIndexes.size() > 0) {
        indexes.put(tsOP.getConf().getTableMetadata(), tblIndexes);
      }
    }

    return indexes;
  }

  /**
   * This method checks if the index is built on all partitions of the base
   * table. If not, then the method returns false as we do not apply optimization
   * for this case.
   * @param tableScan
   * @param indexes
   * @return
   * @throws SemanticException
   */
  private boolean checkIfIndexBuiltOnAllTablePartitions(TableScanOperator tableScan,
      List<Index> indexes) throws SemanticException {
    // check if we have indexes on all partitions in this table scan
    Set<Partition> queryPartitions;
    try {
      queryPartitions = IndexUtils.checkPartitionsCoveredByIndex(tableScan, parseContext, indexes);
      if (queryPartitions == null) { // partitions not covered
        return false;
      }
    } catch (HiveException e) {
      LOG.error("Fatal Error: problem accessing metastore", e);
      throw new SemanticException(e);
    }
    if (queryPartitions.size() != 0) {
      return true;
    }
    return false;
  }

  /**
   * This code block iterates over indexes on the table and populates the indexToKeys map
   * for all the indexes that satisfy the rewrite criteria.
   * @param indexTables
   * @return
   * @throws SemanticException
   */
  Map<Index, String> getIndexToKeysMap(List<Index> indexTables) throws SemanticException{
    Hive hiveInstance = hiveDb;
    Map<Index, String> indexToKeysMap = new LinkedHashMap<Index, String>();
     for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
      Index index = indexTables.get(idxCtr);
       //Getting index key columns
      StorageDescriptor sd = index.getSd();
      List<FieldSchema> idxColList = sd.getCols();
      assert idxColList.size()==1;
      String indexKeyName = idxColList.get(0).getName();
      // Check that the index schema is as expected. This code block should
      // catch problems of this rewrite breaking when the AggregateIndexHandler
      // index is changed.
      List<String> idxTblColNames = new ArrayList<String>();
      try {
        String[] qualified = Utilities.getDbTableName(index.getDbName(),
            index.getIndexTableName());
        Table idxTbl = hiveInstance.getTable(qualified[0], qualified[1]);
        for (FieldSchema idxTblCol : idxTbl.getCols()) {
          idxTblColNames.add(idxTblCol.getName());
        }
      } catch (HiveException e) {
        LOG.error("Got exception while locating index table, " +
            "skipping " + getName() + " optimization");
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }
      assert(idxTblColNames.contains(IDX_BUCKET_COL));
      assert(idxTblColNames.contains(IDX_OFFSETS_ARRAY_COL));
      // we add all index tables which can be used for rewrite
      // and defer the decision of using a particular index for later
      // this is to allow choosing a index if a better mechanism is
      // designed later to chose a better rewrite
      indexToKeysMap.put(index, indexKeyName);
    }
    return indexToKeysMap;
  }

  /**
   * Method to rewrite the input query if all optimization criteria is passed.
   * The method iterates over the tsOpToProcess {@link ArrayList} to apply the rewrites
   * @throws SemanticException
   *
   */
  private void rewriteOriginalQuery() throws SemanticException {
    for (RewriteCanApplyCtx canApplyCtx : tsOpToProcess.values()) {
      RewriteQueryUsingAggregateIndexCtx rewriteQueryCtx =
          RewriteQueryUsingAggregateIndexCtx.getInstance(parseContext, hiveDb, canApplyCtx);
      rewriteQueryCtx.invokeRewriteQueryProc();
      parseContext = rewriteQueryCtx.getParseContext();
    }
    LOG.info("Finished Rewriting query");
  }


  /**
   * This method logs the reason for which we cannot apply the rewrite optimization.
   * @return
   */
  boolean checkIfAllRewriteCriteriaIsMet(RewriteCanApplyCtx canApplyCtx) {
    if (canApplyCtx.isSelClauseColsFetchException()) {
      LOG.debug("Got exception while locating child col refs for select list, " + "skipping "
          + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isAggFuncIsNotCount()) {
      LOG.debug("Agg func other than count is " + "not supported by " + getName()
          + " optimization.");
      return false;
    }
    if (canApplyCtx.isAggParameterException()) {
      LOG.debug("Got exception while locating parameter refs for aggregation, " + "skipping "
          + getName() + " optimization.");
      return false;
    }
    return true;
  }
}

