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

import junit.framework.Assert;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDayOfWeek;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TestVectorTimestampExtract {

  @Test
  public void testTimestamp() throws Exception {
    Random random = new Random(7436);

    doTimestampExtractTests(random, "timestamp");
  }

  @Test
  public void testDate() throws Exception {
    Random random = new Random(83992);

    doTimestampExtractTests(random, "date");
  }

  @Test
  public void testString() throws Exception {
    Random random = new Random(378);

    doTimestampExtractTests(random, "string");
  }

  private void doTimestampExtractTests(Random random, String typeName)
      throws Exception {

    doIfTestOneTimestampExtract(random, typeName, "day");
    doIfTestOneTimestampExtract(random, typeName, "dayofweek");
    doIfTestOneTimestampExtract(random, typeName, "hour");
    doIfTestOneTimestampExtract(random, typeName, "minute");
    doIfTestOneTimestampExtract(random, typeName, "month");
    doIfTestOneTimestampExtract(random, typeName, "second");
    doIfTestOneTimestampExtract(random, typeName, "yearweek");
    doIfTestOneTimestampExtract(random, typeName, "year");
  }

  private void doIfTestOneTimestampExtract(Random random, String dateTimeStringTypeName,
      String extractFunctionName)
          throws Exception {

    TypeInfo dateTimeStringTypeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString(dateTimeStringTypeName);
    PrimitiveCategory dateTimeStringPrimitiveCategory =
        ((PrimitiveTypeInfo) dateTimeStringTypeInfo).getPrimitiveCategory();
    boolean isStringFamily =
        (dateTimeStringPrimitiveCategory == PrimitiveCategory.STRING ||
         dateTimeStringPrimitiveCategory == PrimitiveCategory.CHAR ||
         dateTimeStringPrimitiveCategory == PrimitiveCategory.VARCHAR);

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 1;
    ExprNodeDesc col1Expr;
    if (!isStringFamily) {
      generationSpecList.add(
          GenerationSpec.createSameType(dateTimeStringTypeInfo));
    } else {
      generationSpecList.add(
          GenerationSpec.createStringFamilyOtherTypeValue(
              dateTimeStringTypeInfo, TypeInfoFactory.timestampTypeInfo));
    }
    explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

    String columnName = "col" + (columnNum++);
    col1Expr = new ExprNodeColumnDesc(dateTimeStringTypeInfo, columnName, "table", false);
    columns.add(columnName);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ true, /* isUnicodeOk */ true,
        explicitDataTypePhysicalVariationList);

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    if (dateTimeStringPrimitiveCategory == PrimitiveCategory.DATE &&
        (extractFunctionName.equals("hour") ||
            extractFunctionName.equals("minute") ||
            extractFunctionName.equals("second"))) {
      return;
    }

    final GenericUDF udf;
    switch (extractFunctionName) {
    case "day":
      udf = new UDFDayOfMonth();
      break;
    case "dayofweek":
      GenericUDFBridge dayOfWeekUDFBridge = new GenericUDFBridge();
      dayOfWeekUDFBridge.setUdfClassName(UDFDayOfWeek.class.getName());
      udf = dayOfWeekUDFBridge;
      break;
    case "hour":
      udf = new UDFHour();
      break;
    case "minute":
      udf = new UDFMinute();
      break;
    case "month":
      udf = new UDFMonth();
      break;
    case "second":
      udf = new UDFSecond();
      break;
    case "yearweek":
      GenericUDFBridge weekOfYearUDFBridge = new GenericUDFBridge();
      weekOfYearUDFBridge.setUdfClassName(UDFWeekOfYear.class.getName());
      udf = weekOfYearUDFBridge;
      break;
    case "year":
      udf = new UDFYear();
      break;
    default:
      throw new RuntimeException("Unexpected extract function name " + extractFunctionName);
    }

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, udf, children);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[TimestampExtractTestMode.count][];
    for (int i = 0; i < TimestampExtractTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      TimestampExtractTestMode timestampExtractTestMode = TimestampExtractTestMode.values()[i];
      switch (timestampExtractTestMode) {
      case ROW_MODE:
        if (!doRowCastTest(
              dateTimeStringTypeInfo,
              columns,
              children,
              exprDesc,
              randomRows,
              rowSource.rowStructObjectInspector(),
              resultObjects)) {
          return;
        }
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        if (!doVectorCastTest(
              dateTimeStringTypeInfo,
              columns,
              columnNames,
              rowSource.typeInfos(),
              rowSource.dataTypePhysicalVariations(),
              children,
              exprDesc,
              timestampExtractTestMode,
              batchSource,
              resultObjects)) {
          return;
        }
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + timestampExtractTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < TimestampExtractTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " dateTimeStringTypeName " + dateTimeStringTypeName +
                " extractFunctionName " + extractFunctionName +
                " " + TimestampExtractTestMode.values()[v] +
                " result is NULL " + (vectorResult == null ? "YES" : "NO result " + vectorResult.toString()) +
                " does not match row-mode expected result is NULL " +
                (expectedResult == null ? "YES" : "NO result " + expectedResult.toString()) +
                " row values " + Arrays.toString(randomRows[i]));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " dateTimeStringTypeName " + dateTimeStringTypeName +
                " extractFunctionName " + extractFunctionName +
                " " + TimestampExtractTestMode.values()[v] +
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

  private boolean doRowCastTest(TypeInfo dateTimeStringTypeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows, ObjectInspector rowInspector, Object[] resultObjects)
          throws Exception {

    /*
    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
        " timestampExtractTestMode ROW_MODE" +
        " exprDesc " + exprDesc.toString());
    */

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    try {
        evaluator.initialize(rowInspector);
    } catch (HiveException e) {
      return false;
    }

    ObjectInspector objectInspector = TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(
            TypeInfoFactory.intTypeInfo);

    PrimitiveCategory dateTimeStringPrimitiveCategory =
        ((PrimitiveTypeInfo) dateTimeStringTypeInfo).getPrimitiveCategory();

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object object = row[0];

      Object result;
      switch (dateTimeStringPrimitiveCategory) {
      case TIMESTAMP:
        result = evaluator.evaluate((TimestampWritableV2) object);
        break;
      case DATE:
        result = evaluator.evaluate((DateWritableV2) object);
        break;
      case STRING:
        {
          Text text;
          if (object == null) {
            text = null;
          } else if (object instanceof String) {
            text = new Text();
            text.set((String) object);
          } else {
            text = (Text) object;
          }
          result = evaluator.evaluate(text);
        }
        break;
      default:
        throw new RuntimeException(
            "Unexpected date timestamp string primitive category " +
            dateTimeStringPrimitiveCategory);
      }

      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              result, objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[i] = copyResult;
    }

    return true;
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

  private boolean doVectorCastTest(TypeInfo dateTimeStringTypeInfo,
      List<String> columns, String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      ExprNodeGenericFuncDesc exprDesc,
      TimestampExtractTestMode timestampExtractTestMode,
      VectorRandomBatchSource batchSource,
      Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (timestampExtractTestMode == TimestampExtractTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression = vectorizationContext.getVectorExpression(exprDesc);
    vectorExpression.transientInit();

    if (timestampExtractTestMode == TimestampExtractTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
          " timestampExtractTestMode " + timestampExtractTestMode +
          " vectorExpression " + vectorExpression.toString());
    }

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
        " timestampExtractTestMode " + timestampExtractTestMode +
        " vectorExpression " + vectorExpression.getClass().getSimpleName());
    */

    VectorRandomRowSource rowSource = batchSource.getRowSource();
    VectorizedRowBatchCtx batchContext =
        new VectorizedRowBatchCtx(
            columnNames,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            /* virtualColumnCount */ 0,
            /* neededVirtualColumns */ null,
            vectorizationContext.getScratchColumnTypeNames(),
            vectorizationContext.getScratchDataTypePhysicalVariations());

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();

    resultVectorExtractRow.init(
        new TypeInfo[] { TypeInfoFactory.intTypeInfo }, new int[] { vectorExpression.getOutputColumnNum() });
    Object[] scrqtchRow = new Object[1];

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
          TypeInfoFactory.intTypeInfo, resultObjects);
      rowIndex += batch.size;
    }

    return true;
  }

  public enum TimestampExtractTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }
}
