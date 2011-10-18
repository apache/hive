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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * This class defines a procedure factory used to rewrite the operator plan
 * Each method replaces the necessary base table data structures with
 * the index table data structures for each operator.
 */
public final class RewriteQueryUsingAggregateIndex {
  private static final Log LOG = LogFactory.getLog(RewriteQueryUsingAggregateIndex.class.getName());
  private static RewriteQueryUsingAggregateIndexCtx rewriteQueryCtx = null;

  private RewriteQueryUsingAggregateIndex() {
    //this prevents the class from getting instantiated
  }

  private static class NewQuerySelectSchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator operator = (SelectOperator)nd;
      rewriteQueryCtx = (RewriteQueryUsingAggregateIndexCtx)ctx;
      List<Operator<? extends Serializable>> childOps = operator.getChildOperators();
      Operator<? extends Serializable> childOp = childOps.iterator().next();

      //we need to set the colList, outputColumnNames, colExprMap,
      // rowSchema for only that SelectOperator which precedes the GroupByOperator
      // count(indexed_key_column) needs to be replaced by sum(`_count_of_indexed_key_column`)
      if (childOp instanceof GroupByOperator){
        List<ExprNodeDesc> selColList =
          operator.getConf().getColList();
        selColList.add(rewriteQueryCtx.getAggrExprNode());

        List<String> selOutputColNames =
          operator.getConf().getOutputColumnNames();
        selOutputColNames.add(rewriteQueryCtx.getAggrExprNode().getColumn());

        RowSchema selRS = operator.getSchema();
        List<ColumnInfo> selRSSignature =
          selRS.getSignature();
        //Need to create a new type for Column[_count_of_indexed_key_column] node
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("bigint");
        pti.setTypeName("bigint");
        ColumnInfo newCI = new ColumnInfo(rewriteQueryCtx.getAggregateFunction(), pti, "", false);
        selRSSignature.add(newCI);
        selRS.setSignature((ArrayList<ColumnInfo>) selRSSignature);
        operator.setSchema(selRS);
      }
      return null;
    }
  }

    public static NewQuerySelectSchemaProc getNewQuerySelectSchemaProc(){
      return new NewQuerySelectSchemaProc();
  }


  /**
   * This processor replaces the original TableScanOperator with
   * the new TableScanOperator and metadata that scans over the
   * index table rather than scanning over the orginal table.
   *
   */
  private static class ReplaceTableScanOpProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator scanOperator = (TableScanOperator)nd;
      rewriteQueryCtx = (RewriteQueryUsingAggregateIndexCtx)ctx;
      String baseTableName = rewriteQueryCtx.getBaseTableName();
      String alias = null;
      if(baseTableName.contains(":")){
        alias = (baseTableName.split(":"))[0];
      }

      //Need to remove the original TableScanOperators from these data structures
      // and add new ones
      Map<TableScanOperator, Table>  topToTable =
        rewriteQueryCtx.getParseContext().getTopToTable();
      Map<String, Operator<? extends Serializable>>  topOps =
        rewriteQueryCtx.getParseContext().getTopOps();
      Map<Operator<? extends Serializable>, OpParseContext>  opParseContext =
        rewriteQueryCtx.getParseContext().getOpParseCtx();

      //need this to set rowResolver for new scanOperator
      OpParseContext operatorContext = opParseContext.get(scanOperator);

      //remove original TableScanOperator
      topToTable.remove(scanOperator);
      topOps.remove(baseTableName);
      opParseContext.remove(scanOperator);

      //construct a new descriptor for the index table scan
      TableScanDesc indexTableScanDesc = new TableScanDesc();
      indexTableScanDesc.setGatherStats(false);

      String indexTableName = rewriteQueryCtx.getIndexName();
      Table indexTableHandle = null;
      try {
        indexTableHandle = rewriteQueryCtx.getHiveDb().getTable(indexTableName);
      } catch (HiveException e) {
        LOG.error("Error while getting the table handle for index table.");
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }

      String k = indexTableName + Path.SEPARATOR;
      indexTableScanDesc.setStatsAggPrefix(k);
      scanOperator.setConf(indexTableScanDesc);

      //Construct the new RowResolver for the new TableScanOperator
      RowResolver rr = new RowResolver();
      try {
        StructObjectInspector rowObjectInspector =
          (StructObjectInspector) indexTableHandle.getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
        .getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          rr.put(indexTableName, fields.get(i).getFieldName(), new ColumnInfo(fields
              .get(i).getFieldName(), TypeInfoUtils
              .getTypeInfoFromObjectInspector(fields.get(i)
                  .getFieldObjectInspector()), indexTableName, false));
        }
      } catch (SerDeException e) {
        LOG.error("Error while creating the RowResolver for new TableScanOperator.");
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }

      //Set row resolver for new table
      operatorContext.setRowResolver(rr);
      String tabNameWithAlias = null;
      if(alias != null){
        tabNameWithAlias = alias + ":" + indexTableName;
       }else{
         tabNameWithAlias = indexTableName;
       }

      //Scan operator now points to other table
      topToTable.put(scanOperator, indexTableHandle);
      scanOperator.getConf().setAlias(tabNameWithAlias);
      scanOperator.setAlias(indexTableName);
      topOps.put(tabNameWithAlias, scanOperator);
      opParseContext.put(scanOperator, operatorContext);
      rewriteQueryCtx.getParseContext().setTopToTable(
          (HashMap<TableScanOperator, Table>) topToTable);
      rewriteQueryCtx.getParseContext().setTopOps(
          (HashMap<String, Operator<? extends Serializable>>) topOps);
      rewriteQueryCtx.getParseContext().setOpParseCtx(
          (LinkedHashMap<Operator<? extends Serializable>, OpParseContext>) opParseContext);

      return null;
    }
  }

  public static ReplaceTableScanOpProc getReplaceTableScanProc(){
    return new ReplaceTableScanOpProc();
  }

  /**
   * We need to replace the count(indexed_column_key) GenericUDAF aggregation function for
   * group-by construct to "sum" GenericUDAF.
   * This processor creates a new operator tree for a sample query that creates a GroupByOperator
   * with sum aggregation function and uses that GroupByOperator information to replace
   * the original GroupByOperator aggregation information.
   * It replaces the AggregationDesc (aggregation descriptor) of the old GroupByOperator with the
   * new Aggregation Desc of the new GroupByOperator.
   */
  private static class NewQueryGroupbySchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator operator = (GroupByOperator)nd;
      rewriteQueryCtx = (RewriteQueryUsingAggregateIndexCtx)ctx;

      //We need to replace the GroupByOperator which is in
      //groupOpToInputTables map with the new GroupByOperator
      if(rewriteQueryCtx.getParseContext().getGroupOpToInputTables().containsKey(operator)){
        List<ExprNodeDesc> gbyKeyList = operator.getConf().getKeys();
        String gbyKeys = null;
        Iterator<ExprNodeDesc> gbyKeyListItr = gbyKeyList.iterator();
        while(gbyKeyListItr.hasNext()){
          ExprNodeDesc expr = gbyKeyListItr.next().clone();
          if(expr instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc colExpr = (ExprNodeColumnDesc)expr;
            gbyKeys = colExpr.getColumn();
            if(gbyKeyListItr.hasNext()){
              gbyKeys = gbyKeys + ",";
            }
          }
        }


          //the query contains the sum aggregation GenericUDAF
        String selReplacementCommand = "select sum(`"
          + rewriteQueryCtx.getAggregateFunction() + "`)"
          + " from " + rewriteQueryCtx.getIndexName()
          + " group by " + gbyKeys + " ";
        //create a new ParseContext for the query to retrieve its operator tree,
        //and the required GroupByOperator from it
        ParseContext newDAGContext = RewriteParseContextGenerator.generateOperatorTree(
            rewriteQueryCtx.getParseContext().getConf(),
            selReplacementCommand);

        //we get our new GroupByOperator here
        Map<GroupByOperator, Set<String>> newGbyOpMap = newDAGContext.getGroupOpToInputTables();
        GroupByOperator newGbyOperator = newGbyOpMap.keySet().iterator().next();
        GroupByDesc oldConf = operator.getConf();

        //we need this information to set the correct colList, outputColumnNames in SelectOperator
        ExprNodeColumnDesc aggrExprNode = null;

        //Construct the new AggregationDesc to get rid of the current
        //internal names and replace them with new internal names
        //as required by the operator tree
        GroupByDesc newConf = newGbyOperator.getConf();
        List<AggregationDesc> newAggrList = newConf.getAggregators();
        if(newAggrList != null && newAggrList.size() > 0){
          for (AggregationDesc aggregationDesc : newAggrList) {
            rewriteQueryCtx.setEval(aggregationDesc.getGenericUDAFEvaluator());
            aggrExprNode = (ExprNodeColumnDesc)aggregationDesc.getParameters().get(0);
            rewriteQueryCtx.setAggrExprNode(aggrExprNode);
          }
        }

        //Now the GroupByOperator has the new AggregationList; sum(`_count_of_indexed_key`)
        //instead of count(indexed_key)
        OpParseContext gbyOPC = rewriteQueryCtx.getOpc().get(operator);
        RowResolver gbyRR = newDAGContext.getOpParseCtx().get(newGbyOperator).getRowResolver();
        gbyOPC.setRowResolver(gbyRR);
        rewriteQueryCtx.getOpc().put(operator, gbyOPC);

        oldConf.setAggregators((ArrayList<AggregationDesc>) newAggrList);
        operator.setConf(oldConf);


      }else{
        //we just need to reset the GenericUDAFEvaluator and its name for this
        //GroupByOperator whose parent is the ReduceSinkOperator
        GroupByDesc childConf = (GroupByDesc) operator.getConf();
        List<AggregationDesc> childAggrList = childConf.getAggregators();
        if(childAggrList != null && childAggrList.size() > 0){
          for (AggregationDesc aggregationDesc : childAggrList) {
            List<ExprNodeDesc> paraList = aggregationDesc.getParameters();
            List<ObjectInspector> parametersOIList = new ArrayList<ObjectInspector>();
            for (ExprNodeDesc expr : paraList) {
              parametersOIList.add(expr.getWritableObjectInspector());
            }
            GenericUDAFEvaluator evaluator = FunctionRegistry.getGenericUDAFEvaluator(
                "sum", parametersOIList, false, false);
            aggregationDesc.setGenericUDAFEvaluator(evaluator);
            aggregationDesc.setGenericUDAFName("sum");
          }
        }

      }

      return null;
    }
  }

  public static NewQueryGroupbySchemaProc getNewQueryGroupbySchemaProc(){
    return new NewQueryGroupbySchemaProc();
  }
}
