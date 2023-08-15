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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenBucketingSortingDest {
  protected static final Logger LOG = LoggerFactory.getLogger(GenBucketingSortingDest.class.getName());

  private Operator<? extends OperatorDesc> newOperator;

  private Map<Operator<? extends OperatorDesc>, OpParseContext> newOperatorMap = new HashMap<>();

  private final List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting;

  public GenBucketingSortingDest(String dest, Operator input, QB qb,
      TableDesc table_desc, Table dest_tab, GenFileSinkPlan.SortBucketRSCtx ctx,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      ReadOnlySemanticAnalyzer sa
      ) throws SemanticException {

    ImmutableList.Builder<ReduceSinkOperator> reduceSinkOpBuilder = ImmutableList.builder();
    newOperator = input;
    // If the table is bucketed, and bucketing is enforced, do the following:
    // If the number of buckets is smaller than the number of maximum reducers,
    // create those many reducers.
    // If not, create a multiFileSink instead of FileSink - the multiFileSink will
    // spray the data into multiple buckets. That way, we can support a very large
    // number of buckets without needing a very large number of reducers.
    boolean enforceBucketing = false;
    List<ExprNodeDesc> partnCols = new ArrayList<>();
    List<ExprNodeDesc> sortCols = new ArrayList<>();
    boolean multiFileSpray = false;
    int numFiles = 1;
    int totalFiles = 1;
    boolean isCompaction = false;
    if (dest_tab != null && dest_tab.getParameters() != null) {
      isCompaction = AcidUtils.isCompactionTable(dest_tab.getParameters());
    }

    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    if (dest_tab.getNumBuckets() > 0 && !dest_tab.getBucketCols().isEmpty()) {
      enforceBucketing = true;
      if (SemanticAnalyzer.updating(dest) || SemanticAnalyzer.deleting(dest)) {
        partnCols = getPartitionColsFromBucketColsForUpdateDelete(input, true, operatorMap);
        sortCols = getPartitionColsFromBucketColsForUpdateDelete(input, false, operatorMap);
        createSortOrderForUpdateDelete(sortCols, order, nullOrder, sa.getConf());
      } else {
        partnCols = getPartitionColsFromBucketCols(dest, qb, dest_tab, table_desc, input, false,
            operatorMap, sa.getConf());
      }
    } else {
      // Non-native acid tables should handle their own bucketing for updates/deletes
      if ((SemanticAnalyzer.updating(dest) || SemanticAnalyzer.deleting(dest)) && !AcidUtils.isNonNativeAcidTable(dest_tab, true)) {
        partnCols = getPartitionColsFromBucketColsForUpdateDelete(input, true, operatorMap);
        sortCols = getPartitionColsFromBucketColsForUpdateDelete(input, false, operatorMap);
        createSortOrderForUpdateDelete(sortCols, order, nullOrder, sa.getConf());
        enforceBucketing = true;
      }
    }

    if ((dest_tab.getSortCols() != null) &&
        (dest_tab.getSortCols().size() > 0)) {
      sortCols = getSortCols(dest, qb, dest_tab, table_desc, input, operatorMap, sa.getConf());
      getSortOrders(dest_tab, order, nullOrder);
      if (!enforceBucketing) {
        throw new SemanticException(ErrorMsg.TBL_SORTED_NOT_BUCKETED.getErrorCodedMsg(dest_tab.getCompleteName()));
      }
    } else if (HiveConf.getBoolVar(sa.getConf(), HiveConf.ConfVars.HIVE_SORT_WHEN_BUCKETING) &&
        enforceBucketing && !SemanticAnalyzer.updating(dest) && !SemanticAnalyzer.deleting(dest)) {
      sortCols = new ArrayList<>();
      for (ExprNodeDesc expr : partnCols) {
        sortCols.add(expr.clone());
        order.append(DirectionUtils.codeToSign(DirectionUtils.ASCENDING_CODE));
        nullOrder.append(NullOrdering.NULLS_FIRST.getSign());
      }
    }

    if (enforceBucketing) {
      Operation acidOp = AcidUtils.isFullAcidTable(dest_tab) ? SemanticAnalyzer.getAcidType(table_desc.getOutputFileFormatClass(),
              dest, AcidUtils.isInsertOnlyTable(dest_tab), sa.getTxnMgr()) : Operation.NOT_ACID;
      int maxReducers = sa.getConf().getIntVar(HiveConf.ConfVars.MAXREDUCERS);
      if (sa.getConf().getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS) > 0) {
        maxReducers = sa.getConf().getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);
      }
      int numBuckets = dest_tab.getNumBuckets();
      if (numBuckets > maxReducers) {
        LOG.debug("numBuckets is {} and maxReducers is {}", numBuckets, maxReducers);
        multiFileSpray = true;
        totalFiles = numBuckets;
        if (totalFiles % maxReducers == 0) {
          numFiles = totalFiles / maxReducers;
        }
        else {
          // find the number of reducers such that it is a divisor of totalFiles
          maxReducers = getReducersBucketing(totalFiles, maxReducers);
          numFiles = totalFiles / maxReducers;
        }
      }
      else {
        maxReducers = numBuckets;
      }

      GenReduceSinkPlan genReduceSinkPlan = new GenReduceSinkPlan(input, partnCols, sortCols, order.toString(),
          nullOrder.toString(), maxReducers, acidOp, false, isCompaction, sa, operatorMap);
      newOperator = genReduceSinkPlan.getOperator();
      newOperatorMap.putAll(genReduceSinkPlan.getOperatorMap());
      reduceSinkOpBuilder.add((ReduceSinkOperator)newOperator.getParentOperators().get(0));
      ctx.setMultiFileSpray(multiFileSpray);
      ctx.setNumFiles(numFiles);
      ctx.setTotalFiles(totalFiles);
    }
    reduceSinkOperatorsAddedByEnforceBucketingSorting = reduceSinkOpBuilder.build();
  }

  public static List<ExprNodeDesc> getPartitionColsFromBucketCols(String dest, QB qb, Table tab, TableDesc table_desc,
      Operator input, boolean convert,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      HiveConf conf)
      throws SemanticException {
    List<String> tabBucketCols = tab.getBucketCols();
    List<FieldSchema> tabCols = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();

    for (String bucketCol : tabBucketCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (bucketCol.equals(tabCol.getName())) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, table_desc, input, posns, convert, operatorMap, conf);
  }

  // We have to set up the bucketing columns differently for update and deletes,
  // as it is always using the ROW__ID column.
  public static List<ExprNodeDesc> getPartitionColsFromBucketColsForUpdateDelete(
      Operator input, boolean convert,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
      ) throws SemanticException {
    //return genConvertCol(dest, qb, tab, table_desc, input, Arrays.asList(0), convert);
    // In the case of update and delete the bucketing column is always the first column,
    // and it isn't in the table info.  So rather than asking the table for it,
    // we'll construct it ourself and send it back.  This is based on the work done in
    // genConvertCol below.
    ColumnInfo rowField = operatorMap.get(input).getRowResolver().getColumnInfos().get(0);
    TypeInfo rowFieldTypeInfo = rowField.getType();
    ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo, rowField.getInternalName(),
        rowField.getTabAlias(), true);
    if (convert) {
      column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .createConversionCast(column, TypeInfoFactory.intTypeInfo);
    }
    return Collections.singletonList(column);
  }

  // SORT BY ROW__ID ASC
  private void createSortOrderForUpdateDelete(List<ExprNodeDesc> sortCols,
                                              StringBuilder sortOrder, StringBuilder nullSortOrder,
                                              HiveConf conf) {
    NullOrdering defaultNullOrder = NullOrdering.defaultNullOrder(conf);
    for (int i = 0; i < sortCols.size(); i++) {
      sortOrder.append(DirectionUtils.codeToSign(DirectionUtils.ASCENDING_CODE));
      nullSortOrder.append(defaultNullOrder.getSign());
    }
  }

  public static List<ExprNodeDesc> genConvertCol(String dest, QB qb, TableDesc tableDesc, Operator input,
      List<Integer> posns, boolean convert,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      HiveConf conf
      ) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      AbstractSerDe deserializer = tableDesc.getSerDeClass()
          .newInstance();
      deserializer.initialize(conf, tableDesc.getProperties(), null);
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    List<ColumnInfo> rowFields = operatorMap.get(input).getRowResolver().getColumnInfos();

    // Check column type
    int columnNumber = posns.size();
    List<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(columnNumber);
    for (Integer posn : posns) {
      ObjectInspector tableFieldOI = tableFields.get(posn).getFieldObjectInspector();
      TypeInfo tableFieldTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(tableFieldOI);
      TypeInfo rowFieldTypeInfo = rowFields.get(posn).getType();
      ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
          rowFields.get(posn).getInternalName(), rowFields.get(posn).getTabAlias(),
          rowFields.get(posn).getIsVirtualCol());

      if (convert && !tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
        // need to do some conversions here
        if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
          // cannot convert to complex types
          column = null;
        } else {
          column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
              .createConversionCast(column, (PrimitiveTypeInfo)tableFieldTypeInfo);
        }
        if (column == null) {
          String reason = "Cannot convert column " + posn + " from "
              + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
              qb.getParseInfo().getDestForClause(dest), reason));
        }
      }
      expressions.add(column);
    }

    return expressions;
  }

  private List<ExprNodeDesc> getSortCols(String dest, QB qb, Table tab, TableDesc tableDesc,
                                              Operator input,
                                              Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
                                              HiveConf conf
                                              )
      throws SemanticException {
    List<Order> tabSortCols = tab.getSortCols();
    List<FieldSchema> tabCols = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();
    for (Order sortCol : tabSortCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, tableDesc, input, posns, false, operatorMap, conf);
  }

  private void getSortOrders(Table tab, StringBuilder order, StringBuilder nullOrder) {
    List<Order> tabSortCols = tab.getSortCols();
    List<FieldSchema> tabCols = tab.getCols();

    for (Order sortCol : tabSortCols) {
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          order.append(DirectionUtils.codeToSign(sortCol.getOrder()));
          nullOrder.append(sortCol.getOrder() == DirectionUtils.ASCENDING_CODE ? 'a' : 'z');
          break;
        }
      }
    }
  }

  private int getReducersBucketing(int totalFiles, int maxReducers) {
    int numFiles = (int)Math.ceil((double)totalFiles / (double)maxReducers);
    while (true) {
      if (totalFiles % numFiles == 0) {
        return totalFiles / numFiles;
      }
      numFiles++;
    }
  }

  public Operator<? extends OperatorDesc> getOperator() {
    return newOperator;
  }

  public Map<Operator<? extends OperatorDesc>, OpParseContext> getOperatorMap() {
    return newOperatorMap;
  }

  public List<ReduceSinkOperator> getReduceSinkOperatorsAddedByEnforceBucketingSorting() {
    return reduceSinkOperatorsAddedByEnforceBucketingSorting;
  }
}
