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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.index.AggregateIndexHandler;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
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
 *   select sum(_count_Of_key)
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

public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  private HiveConf hiveConf;
  private static final Log LOG = LogFactory.getLog(RewriteGBUsingIndex.class.getName());

  /*
   * Stores the list of top TableScanOperator names for which the rewrite
   * can be applied and the action that needs to be performed for operator tree
   * starting from this TableScanOperator
   */
  private final Map<String, RewriteCanApplyCtx> tsOpToProcess =
    new LinkedHashMap<String, RewriteCanApplyCtx>();

  //Name of the current table on which rewrite is being performed
  private String baseTableName = null;
  //Name of the current index which is used for rewrite
  private String indexTableName = null;

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
    if(shouldApplyOptimization()){
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
  boolean shouldApplyOptimization() throws SemanticException{
    boolean canApply = false;
    if(ifQueryHasMultipleTables()){
      //We do not apply this optimization for this case as of now.
      return false;
    }else{
    /*
     * This code iterates over each TableScanOperator from the topOps map from ParseContext.
     * For each operator tree originating from this top TableScanOperator, we determine
     * if the optimization can be applied. If yes, we add the name of the top table to
     * the tsOpToProcess to apply rewrite later on.
     * */
      Map<TableScanOperator, Table> topToTable = parseContext.getTopToTable();
      Iterator<TableScanOperator> topOpItr = topToTable.keySet().iterator();
      while(topOpItr.hasNext()){

        TableScanOperator topOp = topOpItr.next();
        Table table = topToTable.get(topOp);
        baseTableName = table.getTableName();
        Map<Table, List<Index>> indexes = getIndexesForRewrite();
        if(indexes == null){
          LOG.debug("Error getting valid indexes for rewrite, " +
              "skipping " + getName() + " optimization");
          return false;
        }

        if(indexes.size() == 0){
          LOG.debug("No Valid Index Found to apply Rewrite, " +
              "skipping " + getName() + " optimization");
          return false;
        }else{
          //we need to check if the base table has confirmed or unknown partitions
          if(parseContext.getOpToPartList() != null && parseContext.getOpToPartList().size() > 0){
            //if base table has partitions, we need to check if index is built for
            //all partitions. If not, then we do not apply the optimization
            if(checkIfIndexBuiltOnAllTablePartitions(topOp, indexes)){
              //check if rewrite can be applied for operator tree
              //if partitions condition returns true
              canApply = checkIfRewriteCanBeApplied(topOp, table, indexes);
            }else{
              LOG.debug("Index is not built for all table partitions, " +
                  "skipping " + getName() + " optimization");
              return false;
            }
          }else{
            //check if rewrite can be applied for operator tree
            //if there are no partitions on base table
            canApply = checkIfRewriteCanBeApplied(topOp, table, indexes);
          }
        }
      }
    }
    return canApply;
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
  private boolean checkIfRewriteCanBeApplied(TableScanOperator topOp, Table baseTable,
      Map<Table, List<Index>> indexes) throws SemanticException{
    boolean canApply = false;
    //Context for checking if this optimization can be applied to the input query
    RewriteCanApplyCtx canApplyCtx = RewriteCanApplyCtx.getInstance(parseContext);
    Map<String, Operator<? extends Serializable>> topOps = parseContext.getTopOps();

    canApplyCtx.setBaseTableName(baseTableName);
    canApplyCtx.populateRewriteVars(topOp);

    Map<Index, Set<String>> indexTableMap = getIndexToKeysMap(indexes.get(baseTable));
    Iterator<Index> indexMapItr = indexTableMap.keySet().iterator();
    Index index = null;
    while(indexMapItr.hasNext()){
      //we rewrite the original query using the first valid index encountered
      //this can be changed if we have a better mechanism to
      //decide which index will produce a better rewrite
      index = indexMapItr.next();
      canApply = canApplyCtx.isIndexUsableForQueryBranchRewrite(index,
          indexTableMap.get(index));
      if(canApply){
        canApply = checkIfAllRewriteCriteriaIsMet(canApplyCtx);
        //break here if any valid index is found to apply rewrite
        if(canApply){
          //check if aggregation function is set.
          //If not, set it using the only indexed column
          if(canApplyCtx.getAggFunction() == null){
            //strip of the start and end braces [...]
            String aggregationFunction = indexTableMap.get(index).toString();
            aggregationFunction = aggregationFunction.substring(1,
                aggregationFunction.length() - 1);
            canApplyCtx.setAggFunction("_count_Of_" + aggregationFunction + "");
          }
        }
        break;
      }
    }
    indexTableName = index.getIndexTableName();

    if(canApply && topOps.containsValue(topOp)) {
      Iterator<String> topOpNamesItr = topOps.keySet().iterator();
      while(topOpNamesItr.hasNext()){
        String topOpName = topOpNamesItr.next();
        if(topOps.get(topOpName).equals(topOp)){
          tsOpToProcess.put(topOpName, canApplyCtx);
        }
      }
    }

    if(tsOpToProcess.size() == 0){
      canApply = false;
    }else{
      canApply = true;
    }
    return canApply;
  }

  /**
   * This block of code iterates over the topToTable map from ParseContext
   * to determine if the query has a scan over multiple tables.
   * @return
   */
  boolean ifQueryHasMultipleTables(){
    Map<TableScanOperator, Table> topToTable = parseContext.getTopToTable();
    Iterator<Table> valuesItr = topToTable.values().iterator();
    Set<String> tableNameSet = new HashSet<String>();
    while(valuesItr.hasNext()){
      Table table = valuesItr.next();
      tableNameSet.add(table.getTableName());
    }
    if(tableNameSet.size() > 1){
      LOG.debug("Query has more than one table " +
          "that is not supported with " + getName() + " optimization.");
      return true;
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
    Collection<Table> topTables = parseContext.getTopToTable().values();
    Map<Table, List<Index>> indexes = new HashMap<Table, List<Index>>();
    for (Table tbl : topTables){
      List<Index> tblIndexes = IndexUtils.getIndexes(tbl, supportedIndexes);
      if (tblIndexes.size() > 0) {
        indexes.put(tbl, tblIndexes);
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
      Map<Table, List<Index>> indexes) throws SemanticException{
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
    if(queryPartitions.size() != 0){
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
  Map<Index, Set<String>> getIndexToKeysMap(List<Index> indexTables) throws SemanticException{
    Index index = null;
    Hive hiveInstance = hiveDb;
    Map<Index, Set<String>> indexToKeysMap = new LinkedHashMap<Index, Set<String>>();
     for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
      final Set<String> indexKeyNames = new LinkedHashSet<String>();
      index = indexTables.get(idxCtr);
       //Getting index key columns
      StorageDescriptor sd = index.getSd();
      List<FieldSchema> idxColList = sd.getCols();
      for (FieldSchema fieldSchema : idxColList) {
        indexKeyNames.add(fieldSchema.getName());
      }
      assert indexKeyNames.size()==1;
      // Check that the index schema is as expected. This code block should
      // catch problems of this rewrite breaking when the AggregateIndexHandler
      // index is changed.
      List<String> idxTblColNames = new ArrayList<String>();
      try {
        Table idxTbl = hiveInstance.getTable(index.getDbName(),
            index.getIndexTableName());
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
      indexToKeysMap.put(index, indexKeyNames);
    }
    return indexToKeysMap;
  }

  /**
   * Method to rewrite the input query if all optimization criteria is passed.
   * The method iterates over the tsOpToProcess {@link ArrayList} to apply the rewrites
   * @throws SemanticException
   *
   */
  @SuppressWarnings("unchecked")
  private void rewriteOriginalQuery() throws SemanticException {
    Map<String, Operator<? extends Serializable>> topOpMap =
      (HashMap<String, Operator<? extends Serializable>>) parseContext.getTopOps().clone();
    Iterator<String> tsOpItr = tsOpToProcess.keySet().iterator();

    while(tsOpItr.hasNext()){
      baseTableName = tsOpItr.next();
      RewriteCanApplyCtx canApplyCtx = tsOpToProcess.get(baseTableName);
      TableScanOperator topOp = (TableScanOperator) topOpMap.get(baseTableName);
      RewriteQueryUsingAggregateIndexCtx rewriteQueryCtx =
        RewriteQueryUsingAggregateIndexCtx.getInstance(parseContext, hiveDb,
            indexTableName, baseTableName, canApplyCtx.getAggFunction());
      rewriteQueryCtx.invokeRewriteQueryProc(topOp);
      parseContext = rewriteQueryCtx.getParseContext();
      parseContext.setOpParseCtx((LinkedHashMap<Operator<? extends Serializable>,
          OpParseContext>) rewriteQueryCtx.getOpc());
    }
    LOG.info("Finished Rewriting query");
  }


  /**
   * This method logs the reason for which we cannot apply the rewrite optimization.
   * @return
   */
  boolean checkIfAllRewriteCriteriaIsMet(RewriteCanApplyCtx canApplyCtx){
    if (canApplyCtx.getAggFuncCnt() > 1){
      LOG.debug("More than 1 agg funcs: " +
          "Not supported by " + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isAggFuncIsNotCount()){
      LOG.debug("Agg func other than count is " +
          "not supported by " + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isCountOnAllCols()){
      LOG.debug("Currently count function needs group by on key columns. This is a count(*) case.,"
          + "Cannot apply this " + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isCountOfOne()){
      LOG.debug("Currently count function needs group by on key columns. This is a count(1) case.,"
          + "Cannot apply this " + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isAggFuncColsFetchException()){
      LOG.debug("Got exception while locating child col refs " +
          "of agg func, skipping " + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isWhrClauseColsFetchException()){
      LOG.debug("Got exception while locating child col refs for where clause, "
          + "skipping " + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isSelClauseColsFetchException()){
      LOG.debug("Got exception while locating child col refs for select list, "
          + "skipping " + getName() + " optimization.");
      return false;
    }
    if (canApplyCtx.isGbyKeysFetchException()){
      LOG.debug("Got exception while locating child col refs for GroupBy key, "
          + "skipping " + getName() + " optimization.");
      return false;
    }
    return true;
  }
}

