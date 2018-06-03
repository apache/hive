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

  private static final boolean corruptDateStrings = false;

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

  private static final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private Object randomDateStringFamily(
      Random random, TypeInfo dateTimeStringTypeInfo, boolean wantWritable) {

    String randomDateString = VectorRandomRowSource.randomPrimitiveDateStringObject(random);
    if (corruptDateStrings && random.nextInt(40) == 39) {

      // Randomly corrupt.
      int index = random.nextInt(randomDateString.length());
      char[] chars = randomDateString.toCharArray();
      chars[index] = alphabet.charAt(random.nextInt(alphabet.length()));
      randomDateString = String.valueOf(chars);
    }

    PrimitiveCategory dateTimeStringPrimitiveCategory =
        ((PrimitiveTypeInfo) dateTimeStringTypeInfo).getPrimitiveCategory();
    switch (dateTimeStringPrimitiveCategory) {
    case STRING:
      return randomDateString;
    case CHAR:
      {
        HiveChar hiveChar =
            new HiveChar(randomDateString, ((CharTypeInfo) dateTimeStringTypeInfo).getLength());
        if (wantWritable) {
          return new HiveCharWritable(hiveChar);
        } else {
          return hiveChar;
        }
      }
    case VARCHAR:
      {
        HiveVarchar hiveVarchar =
            new HiveVarchar(
                randomDateString, ((VarcharTypeInfo) dateTimeStringTypeInfo).getLength());
        if (wantWritable) {
          return new HiveVarcharWritable(hiveVarchar);
        } else {
          return hiveVarchar;
        }
      }
    default:
      throw new RuntimeException("Unexpected string family category " + dateTimeStringPrimitiveCategory);
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

    List<String> explicitTypeNameList = new ArrayList<String>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 0;
    ExprNodeDesc col1Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      explicitTypeNameList.add(dateTimeStringTypeName1);
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
            randomDateStringFamily(
                random, dateTimeStringTypeInfo1, /* wantWritable */ false);
      }
      col1Expr = new ExprNodeConstantDesc(dateTimeStringTypeInfo1, scalar1Object);
    }
    ExprNodeDesc col2Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      explicitTypeNameList.add(dateTimeStringTypeName2);
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
            randomDateStringFamily(
                random, dateTimeStringTypeInfo2, /* wantWritable */ false);
      }
      col2Expr = new ExprNodeConstantDesc(dateTimeStringTypeInfo2, scalar2Object);
    }

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initExplicitSchema(
        random, explicitTypeNameList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    Object[][] randomRows = rowSource.randomRows(100000);

    if (isStringFamily1) {
      if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
          columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
        for (int i = 0; i < randomRows.length; i++) {
          Object[] row = randomRows[i];
          Object object = row[columnNum - 1];
          if (row[0] != null) {
            row[0] =
                randomDateStringFamily(
                    random, dateTimeStringTypeInfo1, /* wantWritable */ true);
          }
        }
      }
    }

    if (isStringFamily2) {
      if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
          columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
        for (int i = 0; i < randomRows.length; i++) {
          Object[] row = randomRows[i];
          Object object = row[columnNum - 1];
          if (row[columnNum - 1] != null) {
            row[columnNum - 1] =
                randomDateStringFamily(
                    random, dateTimeStringTypeInfo2, /* wantWritable */ true);
          }
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

    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo1.toString() +
        " dateTimeStringTypeInfo2 " + dateTimeStringTypeInfo2 +
        " dateDiffTestMode ROW_MODE" +
        " columnScalarMode " + columnScalarMode +
        " exprDesc " + exprDesc.toString());

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

    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo1 " + dateTimeStringTypeInfo1.toString() +
        " dateTimeStringTypeInfo2 " + dateTimeStringTypeInfo2.toString() +
        " dateDiffTestMode " + dateDiffTestMode +
        " columnScalarMode " + columnScalarMode +
        " vectorExpression " + vectorExpression.toString());

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
