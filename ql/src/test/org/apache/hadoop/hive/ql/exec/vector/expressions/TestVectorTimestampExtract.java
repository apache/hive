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
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.TestVectorDateAddSub.ColumnScalarMode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateAdd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateSub;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorTimestampExtract {

  private static final boolean corruptTimestampStrings = false;

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

  public enum TimestampExtractTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
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

  private static final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private Object randomTimestampStringFamily(
      Random random, TypeInfo dateTimeStringTypeInfo, boolean wantWritable) {

    String randomTimestampString =
        VectorRandomRowSource.randomPrimitiveTimestampStringObject(random);
    if (corruptTimestampStrings && random.nextInt(40) == 39) {

      // Randomly corrupt.
      int index = random.nextInt(randomTimestampString.length());
      char[] chars = randomTimestampString.toCharArray();
      chars[index] = alphabet.charAt(random.nextInt(alphabet.length()));
      randomTimestampString = String.valueOf(chars);
    }

    PrimitiveCategory dateTimeStringPrimitiveCategory =
        ((PrimitiveTypeInfo) dateTimeStringTypeInfo).getPrimitiveCategory();
    switch (dateTimeStringPrimitiveCategory) {
    case STRING:
      return randomTimestampString;
    case CHAR:
      {
        HiveChar hiveChar =
            new HiveChar(randomTimestampString, ((CharTypeInfo) dateTimeStringTypeInfo).getLength());
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
                randomTimestampString, ((VarcharTypeInfo) dateTimeStringTypeInfo).getLength());
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

    List<String> explicitTypeNameList = new ArrayList<String>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 0;
    ExprNodeDesc col1Expr;
    explicitTypeNameList.add(dateTimeStringTypeName);
    explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

    String columnName = "col" + (columnNum++);
    col1Expr = new ExprNodeColumnDesc(dateTimeStringTypeInfo, columnName, "table", false);
    columns.add(columnName);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initExplicitSchema(
        random, explicitTypeNameList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    Object[][] randomRows = rowSource.randomRows(100000);

    if (isStringFamily) {
      for (int i = 0; i < randomRows.length; i++) {
        Object[] row = randomRows[i];
        Object object = row[columnNum - 1];
        if (row[0] != null) {
          row[0] =
              randomTimestampStringFamily(
                  random, dateTimeStringTypeInfo, /* wantWritable */ true);
        }
      }
    }

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

    final UDF udf;
    switch (extractFunctionName) {
    case "day":
      udf = new UDFDayOfMonth();
      break;
    case "dayofweek":
      udf = new UDFDayOfWeek();
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
      udf = new UDFWeekOfYear();
      break;
    case "year":
      udf = new UDFYear();
      break;
    default:
      throw new RuntimeException("Unexpected extract function name " + extractFunctionName);
    }

    GenericUDFBridge genericUDFBridge = new GenericUDFBridge();
    genericUDFBridge.setUdfClassName(udf.getClass().getName());

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, genericUDFBridge, children);

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

    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
        " timestampExtractTestMode ROW_MODE" +
        " exprDesc " + exprDesc.toString());

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
        result = evaluator.evaluate((TimestampWritable) object);
        break;
      case DATE:
        result = evaluator.evaluate((DateWritable) object);
        break;
      case STRING:
        {
          Text text;
          if (object == null) {
            text = null;
          } else {
            text = new Text();
            text.set((String) object);
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

    System.out.println(
        "*DEBUG* dateTimeStringTypeInfo " + dateTimeStringTypeInfo.toString() +
        " timestampExtractTestMode " + timestampExtractTestMode +
        " vectorExpression " + vectorExpression.getClass().getSimpleName());

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
}
