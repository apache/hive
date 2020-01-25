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
package org.apache.hadoop.hive.ql.optimizer;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
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
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEpochMilli;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFloor;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDivide;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMod;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Introduces a RS before FS to partition data by configuration specified
 * time granularity.
 */
public class SortedDynPartitionTimeGranularityOptimizer extends Transform {

  @Override
  public ParseContext transform(ParseContext pCtx) throws SemanticException {
    // create a walker which walks the tree in a DFS manner while maintaining the
    // operator stack. The dispatcher generates the plan from the operator tree
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();

    String FS = FileSinkOperator.getOperatorName() + "%";

    opRules.put(new RuleRegExp("Sorted Dynamic Partition Time Granularity", FS), getSortDynPartProc(pCtx));

    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }

  private SemanticNodeProcessor getSortDynPartProc(ParseContext pCtx) {
    return new SortedDynamicPartitionProc(pCtx);
  }

  class SortedDynamicPartitionProc implements SemanticNodeProcessor {

    private final Logger LOG = LoggerFactory.getLogger(SortedDynPartitionTimeGranularityOptimizer.class);
    protected ParseContext parseCtx;
    private int targetShardsPerGranularity = 0;
    private int granularityKeyPos = -1;
    private int partitionKeyPos = -1;

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
      String segmentGranularity;
      final String targetShardsProperty;
      final Table table = fsOp.getConf().getTable();
      if (table != null) {
        // case the statement is an INSERT
        segmentGranularity = table.getParameters().get(Constants.DRUID_SEGMENT_GRANULARITY);
        targetShardsProperty =
            table.getParameters().getOrDefault(Constants.DRUID_TARGET_SHARDS_PER_GRANULARITY, "0");

      } else if (parseCtx.getCreateViewDesc() != null) {
        // case the statement is a CREATE MATERIALIZED VIEW AS
        segmentGranularity = parseCtx.getCreateViewDesc().getTblProps()
                .get(Constants.DRUID_SEGMENT_GRANULARITY);
        targetShardsProperty = parseCtx.getCreateViewDesc().getTblProps()
            .getOrDefault(Constants.DRUID_TARGET_SHARDS_PER_GRANULARITY, "0");
      } else if (parseCtx.getCreateTable() != null) {
        // case the statement is a CREATE TABLE AS
        segmentGranularity = parseCtx.getCreateTable().getTblProps()
                .get(Constants.DRUID_SEGMENT_GRANULARITY);
        targetShardsProperty = parseCtx.getCreateTable().getTblProps()
            .getOrDefault(Constants.DRUID_TARGET_SHARDS_PER_GRANULARITY, "0");
      } else {
        throw new SemanticException("Druid storage handler used but not an INSERT, "
                + "CMVAS or CTAS statement");
      }
      segmentGranularity = Strings.isNullOrEmpty(segmentGranularity) ? HiveConf
          .getVar(parseCtx.getConf(),
              HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY
          ) : segmentGranularity;
      targetShardsPerGranularity = Integer.parseInt(targetShardsProperty);

      LOG.info("Sorted dynamic partitioning on time granularity optimization kicked in...");

      // unlink connection between FS and its parent
      final Operator<? extends OperatorDesc> fsParent = fsOp.getParentOperators().get(0);
      fsParent.getChildOperators().clear();

      if (targetShardsPerGranularity > 0) {
        partitionKeyPos = fsParent.getSchema().getSignature().size() + 1;
      }
      granularityKeyPos = fsParent.getSchema().getSignature().size();
      // Create SelectOp with granularity column
      final Operator<? extends OperatorDesc> granularitySelOp = getGranularitySelOp(fsParent,
              segmentGranularity
      );

      // Create ReduceSinkOp operator
      final ArrayList<ColumnInfo> parentCols =
          Lists.newArrayList(granularitySelOp.getSchema().getSignature());
      final ArrayList<ExprNodeDesc> allRSCols = Lists.newArrayList();
      for (ColumnInfo ci : parentCols) {
        allRSCols.add(new ExprNodeColumnDesc(ci));
      }
      // Get the key positions
      final List<Integer> keyPositions;
      final List<Integer> sortOrder;
      final List<Integer> sortNullOrder;
      //Order matters, assuming later that __time_granularity comes first then __druidPartitionKey
      if (targetShardsPerGranularity > 0) {
        keyPositions = Lists.newArrayList(granularityKeyPos, partitionKeyPos);
        sortOrder = Lists.newArrayList(1, 1); // asc
        sortNullOrder = Lists.newArrayList(0, 0); // nulls first
      } else {
        keyPositions = Lists.newArrayList(granularityKeyPos);
        sortOrder = Lists.newArrayList(1); // asc
        sortNullOrder = Lists.newArrayList(0); // nulls first
      }
      ReduceSinkOperator rsOp = getReduceSinkOp(keyPositions, sortOrder,
          sortNullOrder, allRSCols, granularitySelOp, fsOp.getConf().getWriteType());

      // Create backtrack SelectOp
      final List<ExprNodeDesc> descs = new ArrayList<>(allRSCols.size());
      final List<String> colNames = new ArrayList<>();
      for (int i = 0; i < allRSCols.size(); i++) {
        ExprNodeDesc col = allRSCols.get(i);
        final String colName = col.getExprString();
        colNames.add(colName);
        if (keyPositions.contains(i)) {
          descs.add(
              new ExprNodeColumnDesc(col.getTypeInfo(), ReduceField.KEY.toString() + "." + colName,
                  null, false
              ));
        } else {
          descs.add(new ExprNodeColumnDesc(col.getTypeInfo(),
              ReduceField.VALUE.toString() + "." + colName, null, false
          ));
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
      final ColumnInfo granularityColumnInfo =
          new ColumnInfo(granularitySelOp.getSchema().getSignature().get(granularityKeyPos));
      fsOp.getSchema().getSignature().add(granularityColumnInfo);
      if (targetShardsPerGranularity > 0) {
        final ColumnInfo partitionKeyColumnInfo =
            new ColumnInfo(granularitySelOp.getSchema().getSignature().get(partitionKeyPos));
        fsOp.getSchema().getSignature().add(partitionKeyColumnInfo);
      }

      LOG.info("Inserted " + granularitySelOp.getOperatorId() + ", " + rsOp.getOperatorId() + " and "
          + backtrackSelOp.getOperatorId() + " as parent of " + fsOp.getOperatorId()
          + " and child of " + fsParent.getOperatorId());
      parseCtx.setReduceSinkAddedBySortedDynPartition(true);
      return null;
    }

    private Operator<? extends OperatorDesc> getGranularitySelOp(
        Operator<? extends OperatorDesc> fsParent,
        String segmentGranularity
    ) throws SemanticException {
      final ArrayList<ColumnInfo> parentCols =
          Lists.newArrayList(fsParent.getSchema().getSignature());
      final ArrayList<ExprNodeDesc> descs = Lists.newArrayList();
      final List<String> colNames = Lists.newArrayList();
      PrimitiveCategory timestampType = null;
      int timestampPos = -1;
      for (int i = 0; i < parentCols.size(); i++) {
        ColumnInfo ci = parentCols.get(i);
        ExprNodeColumnDesc columnDesc = new ExprNodeColumnDesc(ci);
        descs.add(columnDesc);
        colNames.add(columnDesc.getExprString());
        if (columnDesc.getTypeInfo().getCategory() == ObjectInspector.Category.PRIMITIVE
                && (((PrimitiveTypeInfo) columnDesc.getTypeInfo()).getPrimitiveCategory() == PrimitiveCategory.TIMESTAMP ||
            ((PrimitiveTypeInfo) columnDesc.getTypeInfo()).getPrimitiveCategory() == PrimitiveCategory.TIMESTAMPLOCALTZ)) {
          if (timestampPos != -1) {
            throw new SemanticException("Multiple columns with timestamp/timestamp with local time-zone type on query result; "
                    + "could not resolve which one is the right column");
          }
          timestampType = ((PrimitiveTypeInfo) columnDesc.getTypeInfo()).getPrimitiveCategory();
          timestampPos = i;
        }
      }
      if (timestampPos == -1) {
        throw new SemanticException("No column with timestamp with local time-zone type on query result; "
                + "one column should be of timestamp with local time-zone type");
      }
      final RowSchema selRS = new RowSchema(fsParent.getSchema());
      // Granularity (partition) column
      final String udfName;
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
          throw new SemanticException(String.format(Locale.ENGLISH,
              "Unknown Druid Granularity [%s], Accepted values are [YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND]",
              segmentGranularity
          ));
      }


      // Timestamp column type in Druid is either timestamp or timestamp with local time-zone, i.e.,
      // a specific instant in time. Thus, for the latest, we have this value and we need to extract the
      // granularity to split the data when we are storing it in Druid. However, Druid stores
      // the data in UTC. Thus, we need to apply the following logic on the data to extract
      // the granularity correctly:
      // 1) Read the timestamp with local time-zone value.
      // 2) Extract UTC epoch (millis) from timestamp with local time-zone.
      // 3) Cast the long to a timestamp.
      // 4) Apply the granularity function on the timestamp value.
      // That way, '2010-01-01 00:00:00 UTC' and '2009-12-31 16:00:00 PST' (same instant)
      // will end up in the same Druid segment.

      // #1 - Read the column value
      ExprNodeDesc expr = new ExprNodeColumnDesc(parentCols.get(timestampPos));
      if (timestampType == PrimitiveCategory.TIMESTAMPLOCALTZ) {
        // #2 - UTC epoch for instant
        expr = new ExprNodeGenericFuncDesc(
            TypeInfoFactory.longTypeInfo, new GenericUDFEpochMilli(), Lists.newArrayList(expr));
        // #3 - Cast to timestamp
        expr = new ExprNodeGenericFuncDesc(
            TypeInfoFactory.timestampTypeInfo, new GenericUDFTimestamp(), Lists.newArrayList(expr));
      }
      // #4 - We apply the granularity function
      expr = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.timestampTypeInfo,
          new GenericUDFBridge(udfName, false, udfClass.getName()),
          Lists.newArrayList(expr));
      descs.add(expr);
      colNames.add(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME);
      // Add granularity to the row schema
      final ColumnInfo ci = new ColumnInfo(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME, TypeInfoFactory.timestampTypeInfo,
              selRS.getSignature().get(0).getTabAlias(), false, false);
      selRS.getSignature().add(ci);
      if (targetShardsPerGranularity > 0 ) {
        // add another partitioning key based on floor(1/rand) % targetShardsPerGranularity
        final ColumnInfo partitionKeyCi =
            new ColumnInfo(Constants.DRUID_SHARD_KEY_COL_NAME, TypeInfoFactory.longTypeInfo,
                selRS.getSignature().get(0).getTabAlias(), false, false
            );
        final ExprNodeDesc targetNumShardDescNode =
            new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, targetShardsPerGranularity);
        final ExprNodeGenericFuncDesc randomFn = ExprNodeGenericFuncDesc
            .newInstance(new GenericUDFBridge("rand", false, UDFRand.class.getName()),
                Lists.newArrayList()
            );

        final ExprNodeGenericFuncDesc random = ExprNodeGenericFuncDesc.newInstance(
            new GenericUDFFloor(), Lists.newArrayList(ExprNodeGenericFuncDesc
                .newInstance(new GenericUDFOPDivide(),
                    Lists.newArrayList(new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 1.0), randomFn)
                )));
        final ExprNodeGenericFuncDesc randModMax = ExprNodeGenericFuncDesc
            .newInstance(new GenericUDFOPMod(),
                Lists.newArrayList(random, targetNumShardDescNode)
            );
        descs.add(randModMax);
        colNames.add(Constants.DRUID_SHARD_KEY_COL_NAME);
        selRS.getSignature().add(partitionKeyCi);
      }
      // Create SelectDesc
      final SelectDesc selConf = new SelectDesc(descs, colNames);
      // Create Select Operator
      final SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(
              selConf, selRS, fsParent);

      return selOp;
    }

    private ReduceSinkOperator getReduceSinkOp(List<Integer> keyPositions, List<Integer> sortOrder,
        List<Integer> sortNullOrder, ArrayList<ExprNodeDesc> allCols, Operator<? extends OperatorDesc> parent,
        AcidUtils.Operation writeType) {
      // we will clone here as RS will update bucket column key with its
      // corresponding with bucket number and hence their OIs
      final ArrayList<ExprNodeDesc> keyCols = keyPositions.stream()
          .map(id -> allCols.get(id).clone())
          .collect(Collectors.toCollection(ArrayList::new));
      ArrayList<ExprNodeDesc> valCols = Lists.newArrayList();
      for (int i = 0; i < allCols.size(); i++) {
        if (i != granularityKeyPos && i != partitionKeyPos) {
          valCols.add(allCols.get(i).clone());
        }
      }

      final ArrayList<ExprNodeDesc> partCols =
          keyPositions.stream().map(id -> allCols.get(id).clone())
              .collect(Collectors.toCollection(ArrayList::new));

      // map _col0 to KEY._col0, etc
      Map<String, ExprNodeDesc> colExprMap = Maps.newHashMap();
      Map<String, String> nameMapping = new HashMap<>();
      final ArrayList<String> keyColNames = Lists.newArrayList();
      final ArrayList<String> valColNames = Lists.newArrayList();
      keyCols.stream().forEach(exprNodeDesc -> {
        keyColNames.add(exprNodeDesc.getExprString());
        colExprMap
            .put(Utilities.ReduceField.KEY + "." + exprNodeDesc.getExprString(), exprNodeDesc);
        nameMapping.put(exprNodeDesc.getExprString(),
            Utilities.ReduceField.KEY + "." + exprNodeDesc.getName()
        );
      });
      valCols.stream().forEach(exprNodeDesc -> {
        valColNames.add(exprNodeDesc.getExprString());
        colExprMap
            .put(Utilities.ReduceField.VALUE + "." + exprNodeDesc.getExprString(), exprNodeDesc);
        nameMapping.put(exprNodeDesc.getExprString(),
            Utilities.ReduceField.VALUE + "." + exprNodeDesc.getName()
        );
      });


      // order and null order
      final String orderStr = StringUtils.repeat("+", sortOrder.size());
      final String nullOrderStr = StringUtils.repeat("a", sortNullOrder.size());

      // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
      // the reduce operator will initialize Extract operator with information
      // from Key and Value TableDesc
      final List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(keyCols,
          keyColNames, 0, "");
      final TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, orderStr, nullOrderStr);
      List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(valCols,
          valColNames, 0, "");
      final TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);
      List<List<Integer>> distinctColumnIndices = Lists.newArrayList();

      // Number of reducers is set to default (-1)
      final ReduceSinkDesc rsConf = new ReduceSinkDesc(keyCols, keyCols.size(), valCols,
          keyColNames, distinctColumnIndices, valColNames, -1, partCols, -1, keyTable,
          valueTable, writeType);

      final ArrayList<ColumnInfo> signature =
          parent.getSchema().getSignature()
              .stream()
              .map(e -> new ColumnInfo(e))
              .map(columnInfo ->
                {
                  columnInfo.setInternalName(nameMapping.get(columnInfo.getInternalName()));
                  return columnInfo;
               })
              .collect(Collectors.toCollection(ArrayList::new));
      final ReduceSinkOperator op = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
          rsConf, new RowSchema(signature), parent);
      op.setColumnExprMap(colExprMap);
      return op;
    }

  }

}
