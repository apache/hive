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
package org.apache.hadoop.hive.ql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorDay;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorHour;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMinute;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMonth;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorSecond;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorWeek;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Introduces a RS before FS to partition data by configuration specified
 * time granularity.
 */
public class SortedDynPartitionTimeGranularityOptimizer extends Transform {

  @Override
  public ParseContext transform(ParseContext pCtx) throws SemanticException {
    // create a walker which walks the tree in a DFS manner while maintaining the
    // operator stack. The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    String FS = FileSinkOperator.getOperatorName() + "%";

    opRules.put(new RuleRegExp("Sorted Dynamic Partition Time Granularity", FS), getSortDynPartProc(pCtx));

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }

  private NodeProcessor getSortDynPartProc(ParseContext pCtx) {
    return new SortedDynamicPartitionProc(pCtx);
  }

  class SortedDynamicPartitionProc implements NodeProcessor {

    private final Logger LOG = LoggerFactory.getLogger(SortedDynPartitionTimeGranularityOptimizer.class);
    protected ParseContext parseCtx;

    public SortedDynamicPartitionProc(ParseContext pCtx) {
      this.parseCtx = pCtx;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // introduce RS and EX before FS
      FileSinkOperator fsOp = (FileSinkOperator) nd;
      final String sh = fsOp.getConf().getTableInfo().getOutputFileFormatClassName();
      if (parseCtx.getQueryProperties().isQuery() || sh == null || !sh
              .equals(Constants.DRUID_HIVE_OUTPUT_FORMAT)) {
        // Bail out, nothing to do
        return null;
      }
      String segmentGranularity = null;
      final Table table = fsOp.getConf().getTable();
      if (table != null) {
        // case the statement is an INSERT
        segmentGranularity = table.getParameters().get(Constants.DRUID_SEGMENT_GRANULARITY);
      } else {
        // case the statement is a CREATE TABLE AS
       segmentGranularity = parseCtx.getCreateTable().getTblProps()
                .get(Constants.DRUID_SEGMENT_GRANULARITY);
      }
      segmentGranularity = !Strings.isNullOrEmpty(segmentGranularity)
              ? segmentGranularity
              : HiveConf.getVar(parseCtx.getConf(),
                      HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY
              );
      LOG.info("Sorted dynamic partitioning on time granularity optimization kicked in...");

      // unlink connection between FS and its parent
      Operator<? extends OperatorDesc> fsParent = fsOp.getParentOperators().get(0);
      fsParent = fsOp.getParentOperators().get(0);
      fsParent.getChildOperators().clear();

      // Create SelectOp with granularity column
      Operator<? extends OperatorDesc> granularitySelOp = getGranularitySelOp(fsParent,
              segmentGranularity
      );

      // Create ReduceSinkOp operator
      ArrayList<ColumnInfo> parentCols = Lists.newArrayList(granularitySelOp.getSchema().getSignature());
      ArrayList<ExprNodeDesc> allRSCols = Lists.newArrayList();
      for (ColumnInfo ci : parentCols) {
        allRSCols.add(new ExprNodeColumnDesc(ci));
      }
      // Get the key positions
      List<Integer> keyPositions = new ArrayList<>();
      keyPositions.add(allRSCols.size() - 1);
      List<Integer> sortOrder = new ArrayList<Integer>(1);
      sortOrder.add(1); // asc
      List<Integer> sortNullOrder = new ArrayList<Integer>(1);
      sortNullOrder.add(0); // nulls first
      ReduceSinkOperator rsOp = getReduceSinkOp(keyPositions, sortOrder,
          sortNullOrder, allRSCols, granularitySelOp);

      // Create backtrack SelectOp
      List<ExprNodeDesc> descs = new ArrayList<ExprNodeDesc>(allRSCols.size());
      List<String> colNames = new ArrayList<String>();
      String colName;
      for (int i = 0; i < allRSCols.size(); i++) {
        ExprNodeDesc col = allRSCols.get(i);
        colName = col.getExprString();
        colNames.add(colName);
        if (keyPositions.contains(i)) {
          descs.add(new ExprNodeColumnDesc(col.getTypeInfo(), ReduceField.KEY.toString()+"."+colName, null, false));
        } else {
          descs.add(new ExprNodeColumnDesc(col.getTypeInfo(), ReduceField.VALUE.toString()+"."+colName, null, false));
        }
      }
      RowSchema selRS = new RowSchema(granularitySelOp.getSchema());
      SelectDesc selConf = new SelectDesc(descs, colNames);
      SelectOperator backtrackSelOp = (SelectOperator) OperatorFactory.getAndMakeChild(
          selConf, selRS, rsOp);

      // Link backtrack SelectOp to FileSinkOp
      fsOp.getParentOperators().clear();
      fsOp.getParentOperators().add(backtrackSelOp);
      backtrackSelOp.getChildOperators().add(fsOp);

      // Update file sink descriptor
      fsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_SORTED);
      fsOp.getConf().setPartitionCols(rsOp.getConf().getPartitionCols());
      ColumnInfo ci = new ColumnInfo(granularitySelOp.getSchema().getSignature().get(
              granularitySelOp.getSchema().getSignature().size() - 1)); // granularity column
      fsOp.getSchema().getSignature().add(ci);

      LOG.info("Inserted " + granularitySelOp.getOperatorId() + ", " + rsOp.getOperatorId() + " and "
          + backtrackSelOp.getOperatorId() + " as parent of " + fsOp.getOperatorId()
          + " and child of " + fsParent.getOperatorId());

      parseCtx.setReduceSinkAddedBySortedDynPartition(true);
      return null;
    }

    private Operator<? extends OperatorDesc> getGranularitySelOp(
            Operator<? extends OperatorDesc> fsParent, String segmentGranularity
    ) throws SemanticException {
      ArrayList<ColumnInfo> parentCols = Lists.newArrayList(fsParent.getSchema().getSignature());
      ArrayList<ExprNodeDesc> descs = Lists.newArrayList();
      List<String> colNames = Lists.newArrayList();
      int timestampPos = -1;
      for (int i = 0; i < parentCols.size(); i++) {
        ColumnInfo ci = parentCols.get(i);
        ExprNodeColumnDesc columnDesc = new ExprNodeColumnDesc(ci);
        descs.add(columnDesc);
        colNames.add(columnDesc.getExprString());
        if (columnDesc.getTypeInfo().getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveTypeInfo) columnDesc.getTypeInfo()).getPrimitiveCategory() == PrimitiveCategory.TIMESTAMP) {
          if (timestampPos != -1) {
            throw new SemanticException("Multiple columns with timestamp type on query result; "
                    + "could not resolve which one is the timestamp column");
          }
          timestampPos = i;
        }
      }
      if (timestampPos == -1) {
        throw new SemanticException("No column with timestamp type on query result; "
                + "one column should be of timestamp type");
      }
      RowSchema selRS = new RowSchema(fsParent.getSchema());
      // Granularity (partition) column
      String udfName;

      Class<? extends UDF> udfClass;
      switch (segmentGranularity) {
        case "YEAR":
          udfName = "floor_year";
          udfClass = UDFDateFloorYear.class;
          break;
        case "MONTH":
          udfName = "floor_month";
          udfClass = UDFDateFloorMonth.class;
          break;
        case "WEEK":
          udfName = "floor_week";
          udfClass = UDFDateFloorWeek.class;
          break;
        case "DAY":
          udfName = "floor_day";
          udfClass = UDFDateFloorDay.class;
          break;
        case "HOUR":
          udfName = "floor_hour";
          udfClass = UDFDateFloorHour.class;
          break;
        case "MINUTE":
          udfName = "floor_minute";
          udfClass = UDFDateFloorMinute.class;
          break;
        case "SECOND":
          udfName = "floor_second";
          udfClass = UDFDateFloorSecond.class;
          break;
        default:
          throw new SemanticException("Granularity for Druid segment not recognized");
      }
      ExprNodeDesc expr = new ExprNodeColumnDesc(parentCols.get(timestampPos));
      descs.add(new ExprNodeGenericFuncDesc(
              TypeInfoFactory.timestampTypeInfo,
              new GenericUDFBridge(udfName, false, udfClass.getName()),
              Lists.newArrayList(expr)));
      colNames.add(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME);
      // Add granularity to the row schema
      ColumnInfo ci = new ColumnInfo(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME, TypeInfoFactory.timestampTypeInfo,
              selRS.getSignature().get(0).getTabAlias(), false, false);
      selRS.getSignature().add(ci);

      // Create SelectDesc
      SelectDesc selConf = new SelectDesc(descs, colNames);

      // Create Select Operator
      SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(
              selConf, selRS, fsParent);

      return selOp;
    }

    private ReduceSinkOperator getReduceSinkOp(List<Integer> keyPositions, List<Integer> sortOrder,
        List<Integer> sortNullOrder, ArrayList<ExprNodeDesc> allCols, Operator<? extends OperatorDesc> parent
    ) throws SemanticException {

      ArrayList<ExprNodeDesc> keyCols = Lists.newArrayList();
      // we will clone here as RS will update bucket column key with its
      // corresponding with bucket number and hence their OIs
      for (Integer idx : keyPositions) {
        keyCols.add(allCols.get(idx).clone());
      }

      ArrayList<ExprNodeDesc> valCols = Lists.newArrayList();
      for (int i = 0; i < allCols.size(); i++) {
        if (!keyPositions.contains(i)) {
          valCols.add(allCols.get(i).clone());
        }
      }

      ArrayList<ExprNodeDesc> partCols = Lists.newArrayList();
      for (Integer idx : keyPositions) {
        partCols.add(allCols.get(idx).clone());
      }

      // map _col0 to KEY._col0, etc
      Map<String, ExprNodeDesc> colExprMap = Maps.newHashMap();
      Map<String, String> nameMapping = new HashMap<>();
      ArrayList<String> keyColNames = Lists.newArrayList();
      for (ExprNodeDesc keyCol : keyCols) {
        String keyColName = keyCol.getExprString();
        keyColNames.add(keyColName);
        colExprMap.put(Utilities.ReduceField.KEY + "." +keyColName, keyCol);
        nameMapping.put(keyColName, Utilities.ReduceField.KEY + "." + keyColName);
      }
      ArrayList<String> valColNames = Lists.newArrayList();
      for (ExprNodeDesc valCol : valCols) {
        String colName = valCol.getExprString();
        valColNames.add(colName);
        colExprMap.put(Utilities.ReduceField.VALUE + "." + colName, valCol);
        nameMapping.put(colName, Utilities.ReduceField.VALUE + "." + colName);
      }

      // order and null order
      String orderStr = StringUtils.repeat("+", sortOrder.size());
      String nullOrderStr = StringUtils.repeat("a", sortNullOrder.size());

      // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
      // the reduce operator will initialize Extract operator with information
      // from Key and Value TableDesc
      List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(keyCols,
          keyColNames, 0, "");
      TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, orderStr, nullOrderStr);
      List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(valCols,
          valColNames, 0, "");
      TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);
      List<List<Integer>> distinctColumnIndices = Lists.newArrayList();

      // Number of reducers is set to default (-1)
      ReduceSinkDesc rsConf = new ReduceSinkDesc(keyCols, keyCols.size(), valCols,
          keyColNames, distinctColumnIndices, valColNames, -1, partCols, -1, keyTable,
          valueTable);

      ArrayList<ColumnInfo> signature = new ArrayList<>();
      for (int index = 0; index < parent.getSchema().getSignature().size(); index++) {
        ColumnInfo colInfo = new ColumnInfo(parent.getSchema().getSignature().get(index));
        colInfo.setInternalName(nameMapping.get(colInfo.getInternalName()));
        signature.add(colInfo);
      }
      ReduceSinkOperator op = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
          rsConf, new RowSchema(signature), parent);
      op.setColumnExprMap(colExprMap);
      return op;
    }

  }

}
