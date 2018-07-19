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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateAdd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateDiff;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateSub;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorDateDiff {

  @Test
  public void testDateDate() throws Exception {
    Random random = new Random(7743);

    doDateDiffTests(random, "date", "date");
  }

  @Test
  public void testDateTimestamp() throws Exception {
    Random random = new Random(7743);

    doDateDiffTests(random, "date", "timestamp");
  }

  @Test
  public void testDateString() throws Exception {
    Random random = new Random(7743);

    doDateDiffTests(random, "date", "string");
  }

  @Test
  public void testTimestampDate() throws Exception {
    Random random = new Random(82);

    doDateDiffTests(random, "timestamp", "date");
  }

  @Test
  public void testTimestampTimestamp() throws Exception {
    Random random = new Random(82);

    doDateDiffTests(random, "timestamp", "timestamp");
  }

  @Test
  public void testTimestampString() throws Exception {
    Random random = new Random(82);

    doDateDiffTests(random, "timestamp", "string");
  }

  @Test
  public void testStringFamily() throws Exception {
    Random random = new Random(12882);

    doDateDiffTests(random, "char(20)", "date");
    doDateDiffTests(random, "char(20)", "timestamp");
    doDateDiffTests(random, "char(20)", "string");

    doDateDiffTests(random, "varchar(20)", "date");
    doDateDiffTests(random, "varchar(20)", "timestamp");
    doDateDiffTests(random, "varchar(20)", "string");
  }

  public enum DateDiffTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public enum ColumnScalarMode {
    COLUMN_COLUMN,
    COLUMN_SCALAR,
    SCALAR_COLUMN;

    static final int count = values().length;
  }

  private void doDateDiffTests(Random random, String dateTimeStringTypeName,
      String integerTypeName)
          throws Exception {
    for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
      doDateDiffTestsWithDiffColumnScalar(
          random, dateTimeStringTypeName, integerTypeName, columnScalarMode);
    }
  }

  private void doDateDiffTestsWithDiffColumnScalar(Random random, String dateTimeStringTypeName1,
      String dateTimeStringTypeName2, ColumnScalarMode columnScalarMode)
          throws Exception {

    TypeInfo dateTimeStringTypeInfo1 =
        TypeInfoUtils.getTypeInfoFromTypeString(dateTimeStringTypeName1);
    PrimitiveCategory dateTimeStringPrimitiveCategory1 =
        ((PrimitiveTypeInfo) dateTimeStringTypeInfo1).getPrimitiveCategory();
    boolean isStringFamily1 =
        (dateTimeStringPrimitiveCategory1 == PrimitiveCategory.STRING ||
         dateTimeStringPrimitiveCategory1 == PrimitiveCategory.CHAR ||
         dateTimeStringPrimitiveCategory1 == PrimitiveCategory.VARCHAR);

    TypeInfo dateTimeStringTypeInfo2 =
        TypeInfoUtils.getTypeInfoFromTypeString(dateTimeStringTypeName2);
    PrimitiveCategory dateTimeStringPrimitiveCategory2 =
        ((PrimitiveTypeInfo) dateTimeStringTypeInfo2).getPrimitiveCategory();
    boolean isStringFamily2 =
        (dateTimeStringPrimitiveCategory2 == PrimitiveCategory.STRING ||
         dateTimeStringPrimitiveCategory2 == PrimitiveCategory.CHAR ||
         dateTimeStringPrimitiveCategory2 == PrimitiveCategory.VARCHAR);

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 0;
    ExprNodeDesc col1Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      if (!isStringFamily1) {
        generationSpecList.add(
            GenerationSpec.createSameType(dateTimeStringTypeInfo1));
      } else {
        generationSpecList.add(
            GenerationSpec.createStringFamilyOtherTypeValue(
                dateTimeStringTypeInfo1, TypeInfoFactory.dateTypeInfo));
      }
      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col1Expr = new ExprNodeColumnDesc(dateTimeStringTypeInfo1, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar1Object;
      if (!isStringFamily1) {
        scalar1Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) dateTimeStringTypeInfo1);
      } else {
        scalar1Object =
            VectorRandomRowSource.randomStringFamilyOtherTypeValue(
                random, dateTimeStringTypeInfo1, TypeInfoFactory.dateTypeInfo, false);
      }
      col1Expr = new ExprNodeConstantDesc(dateTimeStringTypeInfo1, scalar1Object);
    }
    ExprNodeDesc col2Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      if (!isStringFamily2) {
        generationSpecList.add(
            GenerationSpec.createSameType(dateTimeStringTypeInfo2));
      } else {
        generationSpecList.add(
            GenerationSpec.createStringFamilyOtherTypeValue(
                dateTimeStringTypeInfo2, TypeInfoFactory.dateTypeInfo));
      }

      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col2Expr = new ExprNodeColumnDesc(dateTimeStringTypeInfo2, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar2Object;
      if (!isStringFamily2) {
        scalar2Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) dateTimeStringTypeInfo2);
      } else {
        scalar2Object =
            VectorRandomRowSource.randomStringFamilyOtherTypeValue(
                random, dateTimeStringTypeInfo2, TypeInfoFactory.dateTypeInfo, false);
      }
      col2Expr = new ExprNodeConstantDesc(dateTimeStringTypeInfo2, scalar2Object);
    }

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    String[] outputScratchTypeNames = new String[] { "date" };

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
            null);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[DateDiffTestMode.count][];
    for (int i = 0; i < DateDiffTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;


      GenericUDF udf = new GenericUDFDateDiff();

      ExprNodeGenericFuncDesc exprDesc =
          new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, udf, children);

      DateDiffTestMode dateDiffTestMode = DateDiffTestMode.values()[i];
      switch (dateDiffTestMode) {
      case ROW_MODE:
        doRowDateAddSubTest(
            dateTimeStringTypeInfo1,
            dateTimeStringTypeInfo2,
            columns,
            children,
            exprDesc,
            randomRows,
            columnScalarMode,
            rowSource.rowStructObjectInspector(),
            resultObjects);
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        doVectorDateAddSubTest(
            dateTimeStringTypeInfo1,
            dateTimeStringTypeInfo2,
            columns,
            rowSource.typeInfos(),
            children,
            exprDesc,
            dateDiffTestMode,
            columnScalarMode,
            batchSource,
            batchContext,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + dateDiffTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < DateDiffTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i + " " + DateDiffTestMode.values()[v] +
                " " + columnScalarMode +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null) +
                " row values " + Arrays.toString(randomRows[i]));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i + " " + DateDiffTestMode.values()[v] +
                " " + columnScalarMode +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " row values " + Arrays.toString(randomRows[i]));
          }
        }
      }
    }
  }

  private void doRowDateAddSubTest(TypeInfo dateTimeStringTypeInfo1,
      TypeInfo dateTimeStringTypeInfo2,
      List<String> columns, List<ExprNodeDesc> children,
      ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows, ColumnScalarMode columnScalarMode,
      ObjectInspector rowInspector, Object[] resultObjects) throws Exception {

    /*
    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo1.toString() +
        " dateTimeStringTypeInfo2 " + dateTimeStringTypeInfo2 +
        " dateDiffTestMode ROW_MODE" +
        " columnScalarMode " + columnScalarMode +
        " exprDesc " + exprDesc.toString());
    */

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    evaluator.initialize(rowInspector);

    ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            TypeInfoFactory.intTypeInfo);

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              result, objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[i] = copyResult;
    }
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow, Object[] resultObjects) {

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logicalIndex = 0; logicalIndex < batch.size; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);
      resultVectorExtractRow.extractRow(batch, batchIndex, scrqtchRow);

      // UNDONE: Need to copy the object?
      resultObjects[rowIndex++] = scrqtchRow[0];
    }
  }

  private void doVectorDateAddSubTest(TypeInfo dateTimeStringTypeInfo1,
      TypeInfo dateTimeStringTypeInfo2,
      List<String> columns,
      TypeInfo[] typeInfos,
      List<ExprNodeDesc> children,
      ExprNodeGenericFuncDesc exprDesc,
      DateDiffTestMode dateDiffTestMode, ColumnScalarMode columnScalarMode,
      VectorRandomBatchSource batchSource, VectorizedRowBatchCtx batchContext,
      Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (dateDiffTestMode == DateDiffTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    DataTypePhysicalVariation[] dataTypePhysicalVariations = new DataTypePhysicalVariation[2];
    Arrays.fill(dataTypePhysicalVariations, DataTypePhysicalVariation.NONE);

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression = vectorizationContext.getVectorExpression(exprDesc);
    vectorExpression.transientInit();

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(new TypeInfo[] { TypeInfoFactory.intTypeInfo }, new int[] { columns.size() });
    Object[] scrqtchRow = new Object[1];

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo1 " + dateTimeStringTypeInfo1.toString() +
        " dateTimeStringTypeInfo2 " + dateTimeStringTypeInfo2.toString() +
        " dateDiffTestMode " + dateDiffTestMode +
        " columnScalarMode " + columnScalarMode +
        " vectorExpression " + vectorExpression.toString());
    */

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow, resultObjects);
      rowIndex += batch.size;
    }
  }
}
