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

package org.apache.hadoop.hive.ql.index.compact;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.HiveIndexQueryContext;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.index.TableBasedIndexHandler;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;

public class CompactIndexHandler extends TableBasedIndexHandler {

  private Configuration configuration;
  // The names of the partition columns
  private Set<String> partitionCols;
  // Whether or not the conditions have been met to use the fact the index is sorted
  private boolean useSorted;
  private static final Log LOG = LogFactory.getLog(CompactIndexHandler.class.getName());


  @Override
  public void analyzeIndexDefinition(Table baseTable, Index index,
      Table indexTable) throws HiveException {
    StorageDescriptor storageDesc = index.getSd();
    if (this.usesIndexTable() && indexTable != null) {
      StorageDescriptor indexTableSd = storageDesc.deepCopy();
      List<FieldSchema> indexTblCols = indexTableSd.getCols();
      FieldSchema bucketFileName = new FieldSchema("_bucketname", "string", "");
      indexTblCols.add(bucketFileName);
      FieldSchema offSets = new FieldSchema("_offsets", "array<bigint>", "");
      indexTblCols.add(offSets);
      indexTable.setSd(indexTableSd);
    }
  }

  @Override
  protected Task<?> getIndexBuilderMapRedTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      List<FieldSchema> indexField, boolean partitioned,
      PartitionDesc indexTblPartDesc, String indexTableName,
      PartitionDesc baseTablePartDesc, String baseTableName, String dbName) throws HiveException {

    String indexCols = HiveUtils.getUnparsedColumnNamesFromFieldSchema(indexField);

    //form a new insert overwrite query.
    StringBuilder command= new StringBuilder();
    LinkedHashMap<String, String> partSpec = indexTblPartDesc.getPartSpec();

    command.append("INSERT OVERWRITE TABLE " + HiveUtils.unparseIdentifier(indexTableName ));
    if (partitioned && indexTblPartDesc != null) {
      command.append(" PARTITION ( ");
      List<String> ret = getPartKVPairStringArray(partSpec);
      for (int i = 0; i < ret.size(); i++) {
        String partKV = ret.get(i);
        command.append(partKV);
        if (i < ret.size() - 1) {
          command.append(",");
        }
      }
      command.append(" ) ");
    }

    command.append(" SELECT ");
    command.append(indexCols);
    command.append(",");

    command.append(VirtualColumn.FILENAME.getName());
    command.append(",");
    command.append(" collect_set (");
    command.append(VirtualColumn.BLOCKOFFSET.getName());
    command.append(") ");
    command.append(" FROM " + HiveUtils.unparseIdentifier(baseTableName) );
    LinkedHashMap<String, String> basePartSpec = baseTablePartDesc.getPartSpec();
    if(basePartSpec != null) {
      command.append(" WHERE ");
      List<String> pkv = getPartKVPairStringArray(basePartSpec);
      for (int i = 0; i < pkv.size(); i++) {
        String partKV = pkv.get(i);
        command.append(partKV);
        if (i < pkv.size() - 1) {
          command.append(" AND ");
        }
      }
    }
    command.append(" GROUP BY ");
    command.append(indexCols + ", " + VirtualColumn.FILENAME.getName());

    HiveConf builderConf = new HiveConf(getConf(), CompactIndexHandler.class);
    builderConf.setBoolVar(HiveConf.ConfVars.HIVEMERGEMAPFILES, false);
    builderConf.setBoolVar(HiveConf.ConfVars.HIVEMERGEMAPREDFILES, false);
    Task<?> rootTask = IndexUtils.createRootTask(builderConf, inputs, outputs,
        command, partSpec, indexTableName, dbName);
    return rootTask;
  }

  @Override
  public void generateIndexQuery(List<Index> indexes, ExprNodeDesc predicate,
    ParseContext pctx, HiveIndexQueryContext queryContext) {

    Index index = indexes.get(0);
    DecomposedPredicate decomposedPredicate = decomposePredicate(predicate, index,
                                                                  queryContext.getQueryPartitions());

    if (decomposedPredicate == null) {
      queryContext.setQueryTasks(null);
      return; // abort if we couldn't pull out anything from the predicate
    }

    // pass residual predicate back out for further processing
    queryContext.setResidualPredicate(decomposedPredicate.residualPredicate);
    // setup TableScanOperator to change input format for original query
    queryContext.setIndexInputFormat(HiveCompactIndexInputFormat.class.getName());

    // Build reentrant QL for index query
    StringBuilder qlCommand = new StringBuilder("INSERT OVERWRITE DIRECTORY ");

    String tmpFile = pctx.getContext().getMRTmpFileURI();
    queryContext.setIndexIntermediateFile(tmpFile);
    qlCommand.append( "\"" + tmpFile + "\" ");            // QL includes " around file name
    qlCommand.append("SELECT `_bucketname` ,  `_offsets` FROM ");
    qlCommand.append(HiveUtils.unparseIdentifier(index.getIndexTableName()));
    qlCommand.append(" WHERE ");

    String predicateString = decomposedPredicate.pushedPredicate.getExprString();
    qlCommand.append(predicateString);

    // generate tasks from index query string
    LOG.info("Generating tasks for re-entrant QL query: " + qlCommand.toString());
    HiveConf queryConf = new HiveConf(pctx.getConf(), CompactIndexHandler.class);
    HiveConf.setBoolVar(queryConf, HiveConf.ConfVars.COMPRESSRESULT, false);
    Driver driver = new Driver(queryConf);
    driver.compile(qlCommand.toString(), false);

    if (pctx.getConf().getBoolVar(ConfVars.HIVE_INDEX_COMPACT_BINARY_SEARCH) && useSorted) {
      // For now, only works if the predicate is a single condition
      MapWork work = null;
      String originalInputFormat = null;
      for (Task task : driver.getPlan().getRootTasks()) {
        // The index query should have one and only one map reduce task in the root tasks
        // Otherwise something is wrong, log the problem and continue using the default format
        if (task.getWork() instanceof MapredWork) {
          if (work != null) {
            LOG.error("Tried to use a binary search on a compact index but there were an " +
                      "unexpected number (>1) of root level map reduce tasks in the " +
                      "reentrant query plan.");
            work.setInputformat(null);
            work.setInputFormatSorted(false);
            break;
          }
          if (task.getWork() != null) {
            work = ((MapredWork)task.getWork()).getMapWork();
          }
          String inputFormat = work.getInputformat();
          originalInputFormat = inputFormat;
          if (inputFormat == null) {
            inputFormat = HiveConf.getVar(pctx.getConf(), HiveConf.ConfVars.HIVEINPUTFORMAT);
          }

          // We can only perform a binary search with HiveInputFormat and CombineHiveInputFormat
          // and BucketizedHiveInputFormat
          try {
            if (!HiveInputFormat.class.isAssignableFrom(Class.forName(inputFormat))) {
              work = null;
              break;
            }
          } catch (ClassNotFoundException e) {
            LOG.error("Map reduce work's input format class: " + inputFormat + " was not found. " +
                       "Cannot use the fact the compact index is sorted.");
            work = null;
            break;
          }

          work.setInputFormatSorted(true);
        }
      }

      if (work != null) {
        // Find the filter operator and expr node which act on the index column and mark them
        if (!findIndexColumnFilter(work.getAliasToWork().values())) {
          LOG.error("Could not locate the index column's filter operator and expr node. Cannot " +
                    "use the fact the compact index is sorted.");
          work.setInputformat(originalInputFormat);
          work.setInputFormatSorted(false);
        }
      }
    }


    queryContext.addAdditionalSemanticInputs(driver.getPlan().getInputs());
    queryContext.setQueryTasks(driver.getPlan().getRootTasks());
    return;
  }

  /**
   * Does a depth first search on the operator tree looking for a filter operator whose predicate
   * has one child which is a column which is not in the partition
   * @param operators
   * @return whether or not it has found its target
   */
  private boolean findIndexColumnFilter(
    Collection<Operator<? extends OperatorDesc>> operators) {
    for (Operator<? extends OperatorDesc> op : operators) {
      if (op instanceof FilterOperator &&
        ((FilterOperator)op).getConf().getPredicate().getChildren() != null) {
        // Is this the target
        if (findIndexColumnExprNodeDesc(((FilterOperator)op).getConf().getPredicate())) {
          ((FilterOperator)op).getConf().setSortedFilter(true);
          return true;
        }
      }

      // If the target has been found, no need to continue
      if (findIndexColumnFilter(op.getChildOperators())) {
        return true;
      }
    }
    return false;
  }

  private boolean findIndexColumnExprNodeDesc(ExprNodeDesc expression) {
    if (expression.getChildren() == null) {
      return false;
    }

    if (expression.getChildren().size() == 2) {
      ExprNodeColumnDesc columnDesc = null;
      if (expression.getChildren().get(0) instanceof ExprNodeColumnDesc) {
        columnDesc = (ExprNodeColumnDesc)expression.getChildren().get(0);
      } else if (expression.getChildren().get(1) instanceof ExprNodeColumnDesc) {
        columnDesc = (ExprNodeColumnDesc)expression.getChildren().get(1);
      }

      // Is this the target
      if (columnDesc != null && !partitionCols.contains(columnDesc.getColumn())) {
        assert expression instanceof ExprNodeGenericFuncDesc :
               "Expression containing index column is does not support sorting, should not try" +
               "and sort";
        ((ExprNodeGenericFuncDesc)expression).setSortedExpr(true);
        return true;
      }
    }

    for (ExprNodeDesc child : expression.getChildren()) {
      // If the target has been found, no need to continue
      if (findIndexColumnExprNodeDesc(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Split the predicate into the piece we can deal with (pushed), and the one we can't (residual)
   * @param predicate
   * @param index
   * @return
   */
  private DecomposedPredicate decomposePredicate(ExprNodeDesc predicate, Index index,
      Set<Partition> queryPartitions) {
    IndexPredicateAnalyzer analyzer = getIndexPredicateAnalyzer(index, queryPartitions);
    List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
    // split predicate into pushed (what we can handle), and residual (what we can't handle)
    ExprNodeGenericFuncDesc residualPredicate = (ExprNodeGenericFuncDesc)analyzer.
      analyzePredicate(predicate, searchConditions);

    if (searchConditions.size() == 0) {
      return null;
    }

    int numIndexCols = 0;
    for (IndexSearchCondition searchCondition : searchConditions) {
      if (!partitionCols.contains(searchCondition.getColumnDesc().getColumn())) {
        numIndexCols++;
      }
    }

    // For now, only works if the predicate has a single condition on an index column
    if (numIndexCols == 1) {
      useSorted = true;
    } else {
      useSorted = false;
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(searchConditions);
    decomposedPredicate.residualPredicate = residualPredicate;

    return decomposedPredicate;
  }

  /**
   * Instantiate a new predicate analyzer suitable for determining
   * whether we can use an index, based on rules for indexes in
   * WHERE clauses that we support
   *
   * @return preconfigured predicate analyzer for WHERE queries
   */
  private IndexPredicateAnalyzer getIndexPredicateAnalyzer(Index index, Set<Partition> queryPartitions)  {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
    analyzer.addComparisonOp(GenericUDFOPLessThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());

    // only return results for columns in this index
    List<FieldSchema> columnSchemas = index.getSd().getCols();
    for (FieldSchema column : columnSchemas) {
      analyzer.allowColumnName(column.getName());
    }

    // partitioned columns are treated as if they have indexes so that the partitions
    // are used during the index query generation
    partitionCols = new HashSet<String>();
    for (Partition part : queryPartitions) {
      if (part.getSpec().isEmpty()) {
        continue; // empty partitions are from whole tables, so we don't want to add them in
      }
      for (String column : part.getSpec().keySet()) {
        analyzer.allowColumnName(column);
        partitionCols.add(column);
      }
    }

    return analyzer;
  }


  @Override
  public boolean checkQuerySize(long querySize, HiveConf hiveConf) {
    long minSize = hiveConf.getLongVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER_COMPACT_MINSIZE);
    long maxSize = hiveConf.getLongVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER_COMPACT_MAXSIZE);
    if (maxSize < 0) {
      maxSize = Long.MAX_VALUE;
    }
    return (querySize > minSize & querySize < maxSize);
  }

  @Override
  public boolean usesIndexTable() {
    return true;
  }

}
