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
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateAdd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateSub;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;

import junit.framework.Assert;

import org.junit.Test;

public class TestVectorDateAddSub {

  @Test
  public void testDate() throws Exception {
    Random random = new Random(12882);

    doDateAddSubTests(random, "date", "smallint", true);
    doDateAddSubTests(random, "date", "smallint", false);
    doDateAddSubTests(random, "date", "int", true);
    doDateAddSubTests(random, "date", "int", false);
  }

  @Test
  public void testTimestamp() throws Exception {
    Random random = new Random(12882);

    doDateAddSubTests(random, "timestamp", "smallint", true);
    doDateAddSubTests(random, "timestamp", "smallint", false);
    doDateAddSubTests(random, "timestamp", "int", true);
    doDateAddSubTests(random, "timestamp", "int", false);
  }

  @Test
  public void testStringFamily() throws Exception {
    Random random = new Random(12882);

    doDateAddSubTests(random, "string", "smallint", true);
    doDateAddSubTests(random, "string", "smallint", false);
    doDateAddSubTests(random, "string", "int", true);
    doDateAddSubTests(random, "string", "int", false);

    doDateAddSubTests(random, "char(20)", "int", true);
    doDateAddSubTests(random, "char(20)", "int", false);

    doDateAddSubTests(random, "varchar(20)", "int", true);
    doDateAddSubTests(random, "varchar(20)", "int", false);
  }

  public enum DateAddSubTestMode {
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

  private void doDateAddSubTests(Random random, String dateTimeStringTypeName,
      String integerTypeName, boolean isAdd)
          throws Exception {
    for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
      doDateAddSubTestsWithDiffColumnScalar(
          random, dateTimeStringTypeName, integerTypeName, columnScalarMode, isAdd);
    }
  }

  private Object smallerRange(Random random,
      PrimitiveCategory integerPrimitiveCategory, boolean wantWritable) {

    switch (integerPrimitiveCategory) {
    case SHORT:
      {
        short newRandomShort = (short) random.nextInt(20000);
        if (wantWritable) {
          return new ShortWritable(newRandomShort);
        } else {
          return newRandomShort;
        }
      }
    case INT:
      {
        int newRandomInt = random.nextInt(40000);
        if (wantWritable) {
          return new IntWritable(newRandomInt);
        } else {
          return newRandomInt;
        }
      }
    default:
      throw new RuntimeException("Unsupported integer category " + integerPrimitiveCategory);
    }
  }

  private void doDateAddSubTestsWithDiffColumnScalar(Random random, String dateTimeStringTypeName,
      String integerTypeName, ColumnScalarMode columnScalarMode, boolean isAdd)
          throws Exception {

    TypeInfo dateTimeStringTypeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString(dateTimeStringTypeName);
    PrimitiveCategory dateTimeStringPrimitiveCategory =
        ((PrimitiveTypeInfo) dateTimeStringTypeInfo).getPrimitiveCategory();
    boolean isStringFamily =
        (dateTimeStringPrimitiveCategory == PrimitiveCategory.STRING ||
         dateTimeStringPrimitiveCategory == PrimitiveCategory.CHAR ||
         dateTimeStringPrimitiveCategory == PrimitiveCategory.VARCHAR);

    TypeInfo integerTypeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString(integerTypeName);
    PrimitiveCategory integerPrimitiveCategory =
        ((PrimitiveTypeInfo) integerTypeInfo).getPrimitiveCategory();

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 1;
    ExprNodeDesc col1Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      if (!isStringFamily) {
        generationSpecList.add(
            GenerationSpec.createSameType(dateTimeStringTypeInfo));
      } else {
        generationSpecList.add(
            GenerationSpec.createStringFamilyOtherTypeValue(
                dateTimeStringTypeInfo, TypeInfoFactory.dateTypeInfo));
      }
      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col1Expr = new ExprNodeColumnDesc(dateTimeStringTypeInfo, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar1Object;
      if (!isStringFamily) {
        scalar1Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) dateTimeStringTypeInfo);
      } else {
        scalar1Object =
            VectorRandomRowSource.randomStringFamilyOtherTypeValue(
                random, dateTimeStringTypeInfo, TypeInfoFactory.dateTypeInfo, false);
      }
      col1Expr = new ExprNodeConstantDesc(dateTimeStringTypeInfo, scalar1Object);
    }
    ExprNodeDesc col2Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      generationSpecList.add(
          GenerationSpec.createSameType(integerTypeInfo));
      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col2Expr = new ExprNodeColumnDesc(integerTypeInfo, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar2Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) integerTypeInfo);
      scalar2Object =
          smallerRange(
              random, integerPrimitiveCategory, /* wantWritable */ false);
      col2Expr = new ExprNodeConstantDesc(integerTypeInfo, scalar2Object);
    }

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ true, /* isUnicodeOk */ true,
        explicitDataTypePhysicalVariationList);

    Object[][] randomRows = rowSource.randomRows(100000);

    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {

      // Fixup numbers to limit the range to 0 ... N-1.
      for (int i = 0; i < randomRows.length; i++) {
        Object[] row = randomRows[i];
        if (row[columnNum - 2] != null) {
          row[columnNum - 2] =
              smallerRange(
                  random, integerPrimitiveCategory, /* wantWritable */ true);
        }
      }
    }

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
    Object[][] resultObjectsArray = new Object[DateAddSubTestMode.count][];
    for (int i = 0; i < DateAddSubTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      GenericUDF udf =
          (isAdd ? new GenericUDFDateAdd() : new GenericUDFDateSub());

      ExprNodeGenericFuncDesc exprDesc =
          new ExprNodeGenericFuncDesc(TypeInfoFactory.dateTypeInfo, udf, children);

      DateAddSubTestMode dateAddSubTestMode = DateAddSubTestMode.values()[i];
      switch (dateAddSubTestMode) {
      case ROW_MODE:
        doRowDateAddSubTest(
            dateTimeStringTypeInfo,
            integerTypeInfo,
            columns,
            children,
            isAdd,
            exprDesc,
            randomRows,
            columnScalarMode,
            rowSource.rowStructObjectInspector(),
            resultObjects);
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        doVectorDateAddSubTest(
            dateTimeStringTypeInfo,
            integerTypeInfo,
            columns,
            rowSource.typeInfos(),
            children,
            isAdd,
            exprDesc,
            dateAddSubTestMode,
            columnScalarMode,
            batchSource,
            batchContext,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + dateAddSubTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < DateAddSubTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i + " " + DateAddSubTestMode.values()[v] +
                " isAdd " + isAdd +
                " " + columnScalarMode +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null) +
                " row values " + Arrays.toString(randomRows[i]));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i + " " + DateAddSubTestMode.values()[v] +
                " isAdd " + isAdd +
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

  private void doRowDateAddSubTest(TypeInfo dateTimeStringTypeInfo, TypeInfo integerTypeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      boolean isAdd, ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows, ColumnScalarMode columnScalarMode,
      ObjectInspector rowInspector, Object[] resultObjects) throws Exception {

    /*
    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
        " integerTypeInfo " + integerTypeInfo +
        " isAdd " + isAdd +
        " dateAddSubTestMode ROW_MODE" +
        " columnScalarMode " + columnScalarMode +
        " exprDesc " + exprDesc.toString());
    */

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    evaluator.initialize(rowInspector);

    ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            TypeInfoFactory.dateTypeInfo);

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

  private void doVectorDateAddSubTest(TypeInfo dateTimeStringTypeInfo, TypeInfo integerTypeInfo,
      List<String> columns,
      TypeInfo[] typeInfos,
      List<ExprNodeDesc> children,
      boolean isAdd, ExprNodeGenericFuncDesc exprDesc,
      DateAddSubTestMode dateAddSubTestMode, ColumnScalarMode columnScalarMode,
      VectorRandomBatchSource batchSource, VectorizedRowBatchCtx batchContext,
      Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (dateAddSubTestMode == DateAddSubTestMode.ADAPTOR) {
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

    if (dateAddSubTestMode == DateAddSubTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
          " dateAddSubTestMode " + dateAddSubTestMode +
          " columnScalarMode " + columnScalarMode +
          " vectorExpression " + vectorExpression.toString());
    }

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(new TypeInfo[] { TypeInfoFactory.dateTypeInfo }, new int[] { columns.size() });
    Object[] scrqtchRow = new Object[1];

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
        " integerTypeInfo " + integerTypeInfo +
        " isAdd " + isAdd +
        " dateAddSubTestMode " + dateAddSubTestMode +
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
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
          TypeInfoFactory.dateTypeInfo, resultObjects);
      rowIndex += batch.size;
    }
  }
}
