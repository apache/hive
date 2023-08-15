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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.DYNAMICPARTITIONCONVERT;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenConversionSelectOperator {
  protected static final Logger LOG = LoggerFactory.getLogger(GenConversionSelectOperator.class.getName());
 
  private AtomicBoolean converted = new AtomicBoolean(false);
  private Operator<? extends OperatorDesc> conversionSelectOperator;
  private RowResolver rowResolver;

  public GenConversionSelectOperator(
      String dest, QB qb, Operator input,
      Deserializer deserializer, DynamicPartitionCtx dpCtx, List<FieldSchema> parts, Table table,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      HiveConf conf
      ) throws SemanticException {
    conversionSelectOperator = input;
    StructObjectInspector oi = null;
    try {
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING);
    List<ColumnInfo> rowFields = operatorMap.get(input).getRowResolver().getColumnInfos();
    int inColumnCnt = rowFields.size();
    int outColumnCnt = tableFields.size();

    // if target table is always unpartitioned, then the output object inspector will already contain the partition cols
    // too, therefore we shouldn't add the partition col num to the output col num
    boolean alreadyContainsPartCols = Optional.ofNullable(table)
        .map(Table::getStorageHandler)
        .map(HiveStorageHandler::alwaysUnpartitioned)
        .orElse(Boolean.FALSE);

    if (dynPart && dpCtx != null && !alreadyContainsPartCols) {
      outColumnCnt += dpCtx.getNumDPCols();
    }

    // The numbers of input columns and output columns should match for regular query
    if (!SemanticAnalyzer.updating(dest) && !SemanticAnalyzer.deleting(dest) && inColumnCnt != outColumnCnt) {
      String reason = "Table " + dest + " has " + outColumnCnt
          + " columns, but query has " + inColumnCnt + " columns.";
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
          qb.getParseInfo().getDestForClause(dest), reason));
    }

    // Check column types
    int columnNumber = tableFields.size();
    List<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(columnNumber);

    // MetadataTypedColumnsetSerDe does not need type conversions because it
    // does the conversion to String by itself.
    if (!(deserializer instanceof MetadataTypedColumnsetSerDe) && !SemanticAnalyzer.deleting(dest)) {

      // If we're updating, add the required virtual columns.
      int virtualColumnSize = SemanticAnalyzer.updating(dest) ? AcidUtils.getAcidVirtualColumns(table).size() : 0;
      for (int i = 0; i < virtualColumnSize; i++) {
        expressions.add(new ExprNodeColumnDesc(rowFields.get(i).getType(),
            rowFields.get(i).getInternalName(), "", true));
      }

      // here only deals with non-partition columns. We deal with partition columns next
      int rowFieldsOffset = expressions.size();
      for (int i = 0; i < columnNumber; i++) {
        ExprNodeDesc column = handleConversion(tableFields.get(i), rowFields.get(rowFieldsOffset + i), converted, dest, i, qb);
        expressions.add(column);
      }

      // For Non-Native ACID tables we should convert the new values as well
      rowFieldsOffset = expressions.size();
      if (SemanticAnalyzer.updating(dest) && AcidUtils.isNonNativeAcidTable(table, true)) {
        for (int i = 0; i < columnNumber; i++) {
          ExprNodeDesc column = handleConversion(tableFields.get(i), rowFields.get(rowFieldsOffset + i), converted, dest, i, qb);
          expressions.add(column);
        }
      }

      // deal with dynamic partition columns
      rowFieldsOffset = expressions.size();
      if (dynPart && dpCtx != null && dpCtx.getNumDPCols() > 0) {
        // rowFields contains non-partitioned columns (tableFields) followed by DP columns
        for (int dpColIdx = 0; dpColIdx < rowFields.size() - rowFieldsOffset; ++dpColIdx) {

          // create ExprNodeDesc
          ColumnInfo inputColumn = rowFields.get(dpColIdx + rowFieldsOffset);
          TypeInfo inputTypeInfo = inputColumn.getType();
          ExprNodeDesc column =
              new ExprNodeColumnDesc(inputTypeInfo, inputColumn.getInternalName(), "", true);

          // Cast input column to destination column type if necessary.
          if (conf.getBoolVar(DYNAMICPARTITIONCONVERT)) {
            if (parts != null && !parts.isEmpty()) {
              String destPartitionName = dpCtx.getDPColNames().get(dpColIdx);
              FieldSchema destPartitionFieldSchema = parts.stream()
                  .filter(dynamicPartition -> dynamicPartition.getName().equals(destPartitionName))
                  .findFirst().orElse(null);
              if (destPartitionFieldSchema == null) {
                throw new IllegalStateException("Partition schema for dynamic partition " +
                    destPartitionName + " not found in DynamicPartitionCtx.");
              }
              String partitionType = destPartitionFieldSchema.getType();
              if (partitionType == null) {
                throw new IllegalStateException("Couldn't get FieldSchema for partition" +
                    destPartitionFieldSchema.getName());
              }
              PrimitiveTypeInfo partitionTypeInfo =
                  TypeInfoFactory.getPrimitiveTypeInfo(partitionType);
              if (!partitionTypeInfo.equals(inputTypeInfo)) {
                column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
                    .createConversionCast(column, partitionTypeInfo);
                converted.set(true);
              }
            } else {
              LOG.warn("Partition schema for dynamic partition " + inputColumn.getAlias() + " ("
                  + inputColumn.getInternalName() + ") not found in DynamicPartitionCtx. "
                  + "This is expected with a CTAS.");
            }
          }
          expressions.add(column);
        }
      }
    }

    if (converted.get()) {
      // add the select operator
      rowResolver = new RowResolver();
      List<String> colNames = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      for (int i = 0; i < expressions.size(); i++) {
        String name = HiveConf.getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i)
            .getTypeInfo(), "", false));
        colNames.add(name);
        colExprMap.put(name, expressions.get(i));
      }
      conversionSelectOperator = OperatorUtils.createOperator(
          new SelectDesc(expressions, colNames), new RowSchema(rowResolver
              .getColumnInfos()), input);
      conversionSelectOperator.setColumnExprMap(colExprMap);
    }
  }

  public boolean createdOperator() {
    return converted.get();
  }

  public Operator<? extends OperatorDesc> getOperator() {
    return conversionSelectOperator;
  }

  public RowResolver getRowResolver() {
    return rowResolver;
  }

  /**
   * Creates an expression for converting from a table column to a row column. For example:
   * The table column is int but the query provides a string in the row, then we need to cast automatically.
   * @param tableField The target table column
   * @param rowField The source row column
   * @param conversion The value of this boolean is set to true if we detect that a conversion is needed. This is a
   *                   hidden return value hidden here, to notify the caller that a cast was needed.
   * @param dest The destination table for the error message
   * @param columnNum The destination column id for the error message
   * @return The Expression describing the selected column. Note that `conversion` can be considered as a return value
   *         as well
   * @throws SemanticException If conversion were needed, but automatic conversion is not available
   */
  private static ExprNodeDesc handleConversion(StructField tableField, ColumnInfo rowField, AtomicBoolean conversion, String dest,
      int columnNum, QB qb)
      throws SemanticException {
    ObjectInspector tableFieldOI = tableField
        .getFieldObjectInspector();
    TypeInfo tableFieldTypeInfo = TypeInfoUtils
        .getTypeInfoFromObjectInspector(tableFieldOI);
    TypeInfo rowFieldTypeInfo = rowField.getType();
    ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
        rowField.getInternalName(), "", false,
        rowField.isSkewedCol());
    // LazySimpleSerDe can convert any types to String type using
    // JSON-format. However, we may add more operators.
    // Thus, we still keep the conversion.
    if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
      // need to do some conversions here
      conversion.set(true);
      if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
        // cannot convert to complex types
        column = null;
      } else {
        column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
            .createConversionCast(column, (PrimitiveTypeInfo)tableFieldTypeInfo);
      }
      if (column == null) {
        String reason = "Cannot convert column " + columnNum + " from "
            + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
            qb.getParseInfo().getDestForClause(dest), reason));
      }
    }
    return column;
  }
}
