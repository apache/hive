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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;

import junit.framework.Assert;

import org.junit.Test;

public class TestVectorIfStatement {

  @Test
  public void testBoolean() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "boolean");
  }

  @Test
  public void testInt() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "int");
  }

  @Test
  public void testBigInt() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "bigint");
  }

  @Test
  public void testString() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "string");
  }

  @Test
  public void testTimestamp() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "timestamp");
  }

  @Test
  public void testDate() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "date");
  }

  @Test
  public void testIntervalDayTime() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "interval_day_time");
  }

  @Test
  public void testIntervalYearMonth() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "interval_year_month");
  }

  @Test
  public void testDouble() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "double");
  }

  @Test
  public void testChar() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "char(10)");
  }

  @Test
  public void testVarchar() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "varchar(15)");
  }

  @Test
  public void testBinary() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "binary");
  }

  @Test
  public void testDecimalLarge() throws Exception {
    Random random = new Random(9300);

    doIfTests(random, "decimal(20,8)");
  }

  @Test
  public void testDecimalSmall() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "decimal(10,4)");
  }

  public enum IfStmtTestMode {
    ROW_MODE,
    ADAPTOR_WHEN,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public enum ColumnScalarMode {
    COLUMN_COLUMN,
    COLUMN_SCALAR,
    SCALAR_COLUMN,
    SCALAR_SCALAR;

    static final int count = values().length;
  }

  private void doIfTests(Random random, String typeName)
      throws Exception {
    doIfTests(random, typeName, DataTypePhysicalVariation.NONE);
  }

  private void doIfTests(Random random, String typeName,
      DataTypePhysicalVariation dataTypePhysicalVariation)
          throws Exception {
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.COLUMN_COLUMN, dataTypePhysicalVariation, false, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.COLUMN_SCALAR, dataTypePhysicalVariation, false, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.COLUMN_SCALAR, dataTypePhysicalVariation, false, true);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.SCALAR_COLUMN, dataTypePhysicalVariation, false, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.SCALAR_COLUMN, dataTypePhysicalVariation, true, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.SCALAR_SCALAR, dataTypePhysicalVariation, false, false);
  }

  private void doIfTestsWithDiffColumnScalar(Random random, String typeName,
      ColumnScalarMode columnScalarMode, DataTypePhysicalVariation dataTypePhysicalVariation,
      boolean isNullScalar1, boolean isNullScalar2)
          throws Exception {

    /*
    System.out.println("*DEBUG* typeName " + typeName +
        " columnScalarMode " + columnScalarMode +
        " isNullScalar1 " + isNullScalar1 +
        " isNullScalar2 " + isNullScalar2);
    */

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

    boolean isDecimal64 = (dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64);
    final int decimal64Scale =
        (isDecimal64 ? ((DecimalTypeInfo) typeInfo).getScale() : 0);

    List<String> explicitTypeNameList = new ArrayList<String>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList = new ArrayList<DataTypePhysicalVariation>();
    explicitTypeNameList.add("boolean");
    explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);
    if (columnScalarMode != ColumnScalarMode.SCALAR_SCALAR) {
      explicitTypeNameList.add(typeName);
      explicitDataTypePhysicalVariationList.add(dataTypePhysicalVariation);
      if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN) {
        explicitTypeNameList.add(typeName);
        explicitDataTypePhysicalVariationList.add(dataTypePhysicalVariation);
      }
    }

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initExplicitSchema(
        random, explicitTypeNameList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");    // The boolean predicate.

    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Boolean.class, "col0", "table", false);
    int columnNum = 1;
    ExprNodeDesc col2Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      String columnName = "col" + (columnNum++);
      col2Expr = new ExprNodeColumnDesc(typeInfo, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar1Object;
      if (isNullScalar1) {
        scalar1Object = null;
      } else {
        scalar1Object =
            VectorRandomRowSource.randomPrimitiveObject(
                random, (PrimitiveTypeInfo) typeInfo);
      }
      col2Expr = new ExprNodeConstantDesc(typeInfo, scalar1Object);
    }
    ExprNodeDesc col3Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      String columnName = "col" + (columnNum++);
      col3Expr = new ExprNodeColumnDesc(typeInfo, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar2Object;
      if (isNullScalar2) {
        scalar2Object = null;
      } else {
        scalar2Object =
            VectorRandomRowSource.randomPrimitiveObject(
                random, (PrimitiveTypeInfo) typeInfo);
      }
      col3Expr = new ExprNodeConstantDesc(typeInfo, scalar2Object);
    }

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);
    children.add(col3Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    String[] outputScratchTypeNames = new String[] { typeName };
    DataTypePhysicalVariation[] outputDataTypePhysicalVariations =
        new DataTypePhysicalVariation[] { dataTypePhysicalVariation };

    VectorizedRowBatchCtx batchContext =
        new VectorizedRowBatchCtx(
            columnNames,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            /* virtualColumnCount */ 0,
            /* neededVirtualColumns */ null,
            outputScratchTypeNames,
            outputDataTypePhysicalVariations);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[IfStmtTestMode.count][];
    for (int i = 0; i < IfStmtTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      IfStmtTestMode ifStmtTestMode = IfStmtTestMode.values()[i];
      switch (ifStmtTestMode) {
      case ROW_MODE:
        doRowIfTest(
            typeInfo, columns, children, randomRows, rowSource.rowStructObjectInspector(),
            resultObjects);
        break;
      case ADAPTOR_WHEN:
      case VECTOR_EXPRESSION:
        doVectorIfTest(
            typeInfo,
            columns,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            children,
            ifStmtTestMode,
            columnScalarMode,
            batchSource,
            batchContext,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + ifStmtTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < IfStmtTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i + " " + IfStmtTestMode.values()[v] +
                " " + columnScalarMode +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null));
          }
        } else {

          if (isDecimal64 && expectedResult instanceof LongWritable) {

            HiveDecimalWritable expectedHiveDecimalWritable = new HiveDecimalWritable(0);
            expectedHiveDecimalWritable.deserialize64(
                ((LongWritable) expectedResult).get(), decimal64Scale);
            expectedResult = expectedHiveDecimalWritable;
          }

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i + " " + IfStmtTestMode.values()[v] +
                " " + columnScalarMode +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")");
          }
        }
      }
    }
  }

  private void doRowIfTest(TypeInfo typeInfo, List<String> columns, List<ExprNodeDesc> children,
      Object[][] randomRows, ObjectInspector rowInspector, Object[] resultObjects) throws Exception {

    GenericUDF udf = new GenericUDFIf();

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(typeInfo, udf, children);
    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    evaluator.initialize(rowInspector);

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      resultObjects[i] = result;
    }
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow,
      TypeInfo targetTypeInfo, Object[] resultObjects) {

    ObjectInspector objectInspector = TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(targetTypeInfo);

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logicalIndex = 0; logicalIndex < batch.size; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);
      resultVectorExtractRow.extractRow(batch, batchIndex, scrqtchRow);

      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              scrqtchRow[0], objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[rowIndex++] = copyResult;
    }
  }

  private void doVectorIfTest(TypeInfo typeInfo,
      List<String> columns,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      IfStmtTestMode ifStmtTestMode, ColumnScalarMode columnScalarMode,
      VectorRandomBatchSource batchSource, VectorizedRowBatchCtx batchContext,
      Object[] resultObjects)
          throws Exception {

    GenericUDF udf;
    switch (ifStmtTestMode) {
    case VECTOR_EXPRESSION:
      udf = new GenericUDFIf();
      break;
    case ADAPTOR_WHEN:
      udf = new GenericUDFWhen();
      break;
    default:
      throw new RuntimeException("Unexpected IF statement test mode " + ifStmtTestMode);
    }

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(typeInfo, udf, children);

    String ifExprMode = (ifStmtTestMode != IfStmtTestMode.VECTOR_EXPRESSION ? "adaptor" : "good");
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_VECTORIZED_IF_EXPR_MODE, ifExprMode);

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression = vectorizationContext.getVectorExpression(exprDesc);

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(new TypeInfo[] { typeInfo }, new int[] { columns.size() });
    Object[] scrqtchRow = new Object[1];

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " ifStmtTestMode " + ifStmtTestMode +
        " columnScalarMode " + columnScalarMode +
        " vectorExpression " + vectorExpression.getClass().getSimpleName());
    */

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
          typeInfo, resultObjects);
      rowIndex += batch.size;
    }
  }
}
