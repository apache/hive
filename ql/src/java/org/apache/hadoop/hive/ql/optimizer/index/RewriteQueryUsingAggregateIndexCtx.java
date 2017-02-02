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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.FieldNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ColumnPrunerProcFactory;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * RewriteQueryUsingAggregateIndexCtx class stores the
 * context for the {@link RewriteQueryUsingAggregateIndex}
 * used to rewrite operator plan with index table instead of base table.
 */

public final class RewriteQueryUsingAggregateIndexCtx  implements NodeProcessorCtx {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteQueryUsingAggregateIndexCtx.class.getName());
  private RewriteQueryUsingAggregateIndexCtx(ParseContext parseContext, Hive hiveDb,
      RewriteCanApplyCtx canApplyCtx) {
    this.parseContext = parseContext;
    this.hiveDb = hiveDb;
    this.canApplyCtx = canApplyCtx;
    this.indexTableName = canApplyCtx.getIndexTableName();
    this.alias = canApplyCtx.getAlias();
    this.aggregateFunction = canApplyCtx.getAggFunction();
    this.indexKey = canApplyCtx.getIndexKey();
  }

  public static RewriteQueryUsingAggregateIndexCtx getInstance(ParseContext parseContext,
      Hive hiveDb, RewriteCanApplyCtx canApplyCtx) {
    return new RewriteQueryUsingAggregateIndexCtx(
        parseContext, hiveDb, canApplyCtx);
  }

  // Assumes one instance of this + single-threaded compilation for each query.
  private final Hive hiveDb;
  private final ParseContext parseContext;
  private final RewriteCanApplyCtx canApplyCtx;
  //We need the GenericUDAFEvaluator for GenericUDAF function "sum"
  private GenericUDAFEvaluator eval = null;
  private final String indexTableName;
  private final String alias;
  private final String aggregateFunction;
  private ExprNodeColumnDesc aggrExprNode = null;
  private String indexKey;

  public  ParseContext getParseContext() {
    return parseContext;
  }

  public Hive getHiveDb() {
    return hiveDb;
  }

  public String getIndexName() {
     return indexTableName;
  }

  public GenericUDAFEvaluator getEval() {
    return eval;
  }

  public void setEval(GenericUDAFEvaluator eval) {
    this.eval = eval;
  }

  public void setAggrExprNode(ExprNodeColumnDesc aggrExprNode) {
    this.aggrExprNode = aggrExprNode;
  }

  public ExprNodeColumnDesc getAggrExprNode() {
    return aggrExprNode;
  }

  public String getAlias() {
    return alias;
  }

  public String getAggregateFunction() {
    return aggregateFunction;
  }

  public String getIndexKey() {
    return indexKey;
  }

  public void setIndexKey(String indexKey) {
    this.indexKey = indexKey;
  }

  public void invokeRewriteQueryProc() throws SemanticException {
    this.replaceTableScanProcess(canApplyCtx.getTableScanOperator());
    //We need aggrExprNode. Thus, replaceGroupByOperatorProcess should come before replaceSelectOperatorProcess
    for (int index = 0; index < canApplyCtx.getGroupByOperators().size(); index++) {
      this.replaceGroupByOperatorProcess(canApplyCtx.getGroupByOperators().get(index), index);
    }
    for (SelectOperator selectperator : canApplyCtx.getSelectOperators()) {
      this.replaceSelectOperatorProcess(selectperator);
    }
  }

  /**
   * This method replaces the original TableScanOperator with the new
   * TableScanOperator and metadata that scans over the index table rather than
   * scanning over the original table.
   *
   */
  private void replaceTableScanProcess(TableScanOperator scanOperator) throws SemanticException {
    RewriteQueryUsingAggregateIndexCtx rewriteQueryCtx = this;
    String alias = rewriteQueryCtx.getAlias();

    // Need to remove the original TableScanOperators from these data structures
    // and add new ones
    HashMap<String, TableScanOperator> topOps = rewriteQueryCtx.getParseContext()
        .getTopOps();

    // remove original TableScanOperator
    topOps.remove(alias);

    String indexTableName = rewriteQueryCtx.getIndexName();
    Table indexTableHandle = null;
    try {
      indexTableHandle = rewriteQueryCtx.getHiveDb().getTable(indexTableName);
    } catch (HiveException e) {
      LOG.error("Error while getting the table handle for index table.");
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }

    // construct a new descriptor for the index table scan
    TableScanDesc indexTableScanDesc = new TableScanDesc(indexTableHandle);
    indexTableScanDesc.setGatherStats(false);

    String k = MetaStoreUtils.encodeTableName(indexTableName) + Path.SEPARATOR;
    indexTableScanDesc.setStatsAggPrefix(k);
    scanOperator.setConf(indexTableScanDesc);

    // Construct the new RowResolver for the new TableScanOperator
    ArrayList<ColumnInfo> sigRS = new ArrayList<ColumnInfo>();
    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector) indexTableHandle
          .getDeserializer().getObjectInspector();
      StructField field = rowObjectInspector.getStructFieldRef(rewriteQueryCtx.getIndexKey());
      sigRS.add(new ColumnInfo(field.getFieldName(), TypeInfoUtils.getTypeInfoFromObjectInspector(
              field.getFieldObjectInspector()), indexTableName, false));
    } catch (SerDeException e) {
      LOG.error("Error while creating the RowResolver for new TableScanOperator.");
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }
    RowSchema rs = new RowSchema(sigRS);

    // Set row resolver for new table
    String newAlias = indexTableName;
    int index = alias.lastIndexOf(":");
    if (index >= 0) {
      newAlias = alias.substring(0, index) + ":" + indexTableName;
    }

    // Scan operator now points to other table
    scanOperator.getConf().setAlias(newAlias);
    scanOperator.setAlias(indexTableName);
    topOps.put(newAlias, scanOperator);
    rewriteQueryCtx.getParseContext().setTopOps(topOps);

    ColumnPrunerProcFactory.setupNeededColumns(scanOperator, rs,
        Arrays.asList(new FieldNode(rewriteQueryCtx.getIndexKey())));
  }

  /**
   * This method replaces the original SelectOperator with the new
   * SelectOperator with a new column indexed_key_column.
   */
  private void replaceSelectOperatorProcess(SelectOperator operator) throws SemanticException {
    RewriteQueryUsingAggregateIndexCtx rewriteQueryCtx = this;
    // we need to set the colList, outputColumnNames, colExprMap,
    // rowSchema for only that SelectOperator which precedes the GroupByOperator
    // count(indexed_key_column) needs to be replaced by
    // sum(`_count_of_indexed_key_column`)
    List<ExprNodeDesc> selColList = operator.getConf().getColList();
    selColList.add(rewriteQueryCtx.getAggrExprNode());

    List<String> selOutputColNames = operator.getConf().getOutputColumnNames();
    selOutputColNames.add(rewriteQueryCtx.getAggrExprNode().getColumn());

    operator.getColumnExprMap().put(rewriteQueryCtx.getAggrExprNode().getColumn(),
        rewriteQueryCtx.getAggrExprNode());

    RowSchema selRS = operator.getSchema();
    List<ColumnInfo> selRSSignature = selRS.getSignature();
    // Need to create a new type for Column[_count_of_indexed_key_column] node
    PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo("bigint");
    pti.setTypeName("bigint");
    ColumnInfo newCI = new ColumnInfo(rewriteQueryCtx.getAggregateFunction(), pti, "", false);
    selRSSignature.add(newCI);
    selRS.setSignature((ArrayList<ColumnInfo>) selRSSignature);
    operator.setSchema(selRS);
  }

  /**
   * We need to replace the count(indexed_column_key) GenericUDAF aggregation
   * function for group-by construct to "sum" GenericUDAF. This method creates a
   * new operator tree for a sample query that creates a GroupByOperator with
   * sum aggregation function and uses that GroupByOperator information to
   * replace the original GroupByOperator aggregation information. It replaces
   * the AggregationDesc (aggregation descriptor) of the old GroupByOperator
   * with the new Aggregation Desc of the new GroupByOperator.
   * @return
   */
  private void replaceGroupByOperatorProcess(GroupByOperator operator, int index)
      throws SemanticException {
    RewriteQueryUsingAggregateIndexCtx rewriteQueryCtx = this;

    // We need to replace the GroupByOperator which is before RS
    if (index == 0) {
      // the query contains the sum aggregation GenericUDAF
      String selReplacementCommand = "select sum(`" + rewriteQueryCtx.getAggregateFunction() + "`)"
          + " from `" + rewriteQueryCtx.getIndexName() + "` group by "
          + rewriteQueryCtx.getIndexKey() + " ";
      // retrieve the operator tree for the query, and the required GroupByOperator from it
      Operator<?> newOperatorTree = RewriteParseContextGenerator.generateOperatorTree(
              rewriteQueryCtx.getParseContext().getQueryState(),
              selReplacementCommand);

      // we get our new GroupByOperator here
      GroupByOperator newGbyOperator = OperatorUtils.findLastOperatorUpstream(
            newOperatorTree, GroupByOperator.class);
      if (newGbyOperator == null) {
        throw new SemanticException("Error replacing GroupBy operator.");
      }

      // we need this information to set the correct colList, outputColumnNames
      // in SelectOperator
      ExprNodeColumnDesc aggrExprNode = null;

      // Construct the new AggregationDesc to get rid of the current
      // internal names and replace them with new internal names
      // as required by the operator tree
      GroupByDesc newConf = newGbyOperator.getConf();
      List<AggregationDesc> newAggrList = newConf.getAggregators();
      if (newAggrList != null && newAggrList.size() > 0) {
        for (AggregationDesc aggregationDesc : newAggrList) {
          rewriteQueryCtx.setEval(aggregationDesc.getGenericUDAFEvaluator());
          aggrExprNode = (ExprNodeColumnDesc) aggregationDesc.getParameters().get(0);
          rewriteQueryCtx.setAggrExprNode(aggrExprNode);
        }
      }

      // Now the GroupByOperator has the new AggregationList;
      // sum(`_count_of_indexed_key`)
      // instead of count(indexed_key)
      GroupByDesc oldConf = operator.getConf();
      oldConf.setAggregators((ArrayList<AggregationDesc>) newAggrList);
      operator.setConf(oldConf);

    } else {
      // we just need to reset the GenericUDAFEvaluator and its name for this
      // GroupByOperator whose parent is the ReduceSinkOperator
      GroupByDesc childConf = operator.getConf();
      List<AggregationDesc> childAggrList = childConf.getAggregators();
      if (childAggrList != null && childAggrList.size() > 0) {
        for (AggregationDesc aggregationDesc : childAggrList) {
          List<ExprNodeDesc> paraList = aggregationDesc.getParameters();
          List<ObjectInspector> parametersOIList = new ArrayList<ObjectInspector>();
          for (ExprNodeDesc expr : paraList) {
            parametersOIList.add(expr.getWritableObjectInspector());
          }
          GenericUDAFEvaluator evaluator = FunctionRegistry.getGenericUDAFEvaluator("sum",
              parametersOIList, false, false);
          aggregationDesc.setGenericUDAFEvaluator(evaluator);
          aggregationDesc.setGenericUDAFName("sum");
        }
      }
    }
  }
}
