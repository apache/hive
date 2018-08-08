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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.SupportedTypes;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorIndex {

  @Test
  public void testListIndex() throws Exception {
    Random random = new Random(241);

    doIndex(random, /* isList */ true, null, /* isFullElementTypeGamut */ true);
  }

  private static TypeInfo[] decimalTypeInfos = new TypeInfo[] {
    new DecimalTypeInfo(38, 18),
    new DecimalTypeInfo(25, 2),
    new DecimalTypeInfo(19, 4),
    new DecimalTypeInfo(18, 10),
    new DecimalTypeInfo(17, 3),
    new DecimalTypeInfo(12, 2),
    new DecimalTypeInfo(7, 1)
  };

  @Test
  public void testMapIndex() throws Exception {
    Random random = new Random(233);

    doIndex(random, /* isList */ false, "int", /* isFullElementTypeGamut */ true);
    doIndex(random, /* isList */ false, "bigint", /* isFullElementTypeGamut */ false);
    doIndex(random, /* isList */ false, "double", /* isFullElementTypeGamut */ false);
    doIndex(random, /* isList */ false, "string", /* isFullElementTypeGamut */ false);
    for (TypeInfo typeInfo : decimalTypeInfos) {
      doIndex(
          random, /* isList */ false, typeInfo.getTypeName(), /* isFullElementTypeGamut */ false);
    }
  }

  public enum IndexTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  private void doIndex(Random random, boolean isList, String keyTypeName,
      boolean isFullElementTypeGamut)
      throws Exception {

    String oneElementRootTypeName = "bigint";
    doIndexOnRandomDataType(random, isList, keyTypeName, oneElementRootTypeName,
        /* allowNulls */ true, /* isScalarIndex */ false);

    doIndexOnRandomDataType(random, isList, keyTypeName, oneElementRootTypeName,
        /* allowNulls */ true, /* isScalarIndex */ true);

    doIndexOnRandomDataType(random, isList, keyTypeName, oneElementRootTypeName,
        /* allowNulls */ false, /* isScalarIndex */ false);
    doIndexOnRandomDataType(random, isList, keyTypeName, oneElementRootTypeName,
        /* allowNulls */ false, /* isScalarIndex */ true);

    if (!isFullElementTypeGamut) {
      return;
    }

    List<String> elementRootTypeNameList = new ArrayList<String>();
    elementRootTypeNameList.add("int");
    elementRootTypeNameList.add("bigint");
    elementRootTypeNameList.add("double");
    elementRootTypeNameList.add("string");
    elementRootTypeNameList.add("char");
    elementRootTypeNameList.add("varchar");
    elementRootTypeNameList.add("date");
    elementRootTypeNameList.add("timestamp");
    elementRootTypeNameList.add("binary");
    elementRootTypeNameList.add("decimal");
    elementRootTypeNameList.add("interval_day_time");

    for (String elementRootTypeName : elementRootTypeNameList) {
      doIndexOnRandomDataType(random, isList, keyTypeName, elementRootTypeName,
          /* allowNulls */ true, /* isScalarIndex */ false);
    }
  }

  private boolean doIndexOnRandomDataType(Random random,
      boolean isList, String keyTypeName, String elementRootTypeName,
      boolean allowNulls, boolean isScalarIndex)
          throws Exception {

    String elementTypeName =
        VectorRandomRowSource.getDecoratedTypeName(
            random, elementRootTypeName, SupportedTypes.ALL, /* allowedTypeNameSet */ null,
            /* depth */ 0, /* maxDepth */ 3);

    TypeInfo elementTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(elementTypeName);

    ObjectInspector elementObjectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            elementTypeInfo);

    //----------------------------------------------------------------------------------------------

    final TypeInfo keyTypeInfo;
    if (isList) {
      keyTypeInfo = TypeInfoFactory.intTypeInfo;
    } else {
      keyTypeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(keyTypeName);
    }
    final ObjectInspector keyObjectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            keyTypeInfo);

    Object exampleObject =
        (isList ?
            ((WritableIntObjectInspector) keyObjectInspector).create(0) :
            VectorRandomRowSource.randomWritable(
                random, keyTypeInfo, keyObjectInspector, DataTypePhysicalVariation.NONE,
                /* allowNull */ false));
    WritableComparator writableComparator =
        WritableComparator.get((Class<? extends WritableComparable>) exampleObject.getClass());

    final int allKeyCount = 10 + random.nextInt(10);
    final int keyCount = 5 + random.nextInt(allKeyCount / 2);
    List<Object> allKeyList = new ArrayList<Object>(allKeyCount);

    Set<Object> allKeyTreeSet = new TreeSet<Object>(writableComparator);

    int fillAllKeyCount = 0;
    while (fillAllKeyCount < allKeyCount) {
      Object object;
      if (isList) {
        WritableIntObjectInspector writableOI = (WritableIntObjectInspector) keyObjectInspector;
        int index = random.nextInt(keyCount);
        object = writableOI.create(index);
        while (allKeyTreeSet.contains(object)) {
          index =
              (random.nextBoolean() ?
                  random.nextInt() :
                  (random.nextBoolean() ? -1 : keyCount));
          object = writableOI.create(index);
        }
      } else {
        do {
          object =
              VectorRandomRowSource.randomWritable(
                  random, keyTypeInfo, keyObjectInspector, DataTypePhysicalVariation.NONE,
                  /* allowNull */ false);
        } while (allKeyTreeSet.contains(object));
      }
      allKeyList.add(object);
      allKeyTreeSet.add(object);
      fillAllKeyCount++;
    }

    List<Object> keyList = new ArrayList<Object>();

    Set<Object> keyTreeSet = new TreeSet<Object>(writableComparator);

    int fillKeyCount = 0;
    while (fillKeyCount < keyCount) {
      Object newKey = allKeyList.get(random.nextInt(allKeyCount));
      if (keyTreeSet.contains(newKey)) {
        continue;
      }
      keyList.add(newKey);
      keyTreeSet.add(newKey);
      fillKeyCount++;
    }

    //----------------------------------------------------------------------------------------------

    final TypeInfo typeInfo;
    if (isList) {
      ListTypeInfo listTypeInfo = new ListTypeInfo();
      listTypeInfo.setListElementTypeInfo(elementTypeInfo);
      typeInfo = listTypeInfo;
    } else {
      MapTypeInfo mapTypeInfo = new MapTypeInfo();
      mapTypeInfo.setMapKeyTypeInfo(keyTypeInfo);
      mapTypeInfo.setMapValueTypeInfo(elementTypeInfo);
      typeInfo = mapTypeInfo;
    }

    final String typeName = typeInfo.getTypeName();

    final ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            typeInfo);

    //----------------------------------------------------------------------------------------------

    GenerationSpec generationSpec = GenerationSpec.createSameType(typeInfo);

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();
    List<String> columns = new ArrayList<String>();
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();

    int columnNum = 1;

    ExprNodeDesc keyColExpr;

    if (!isScalarIndex) {
      generationSpecList.add(
          GenerationSpec.createValueList(keyTypeInfo, keyList));
      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);
      String columnName = "col" + columnNum++;
      columns.add(columnName);
      keyColExpr = new ExprNodeColumnDesc(keyTypeInfo, columnName, "table", false);
    } else {
      Object scalarWritable = keyList.get(random.nextInt(keyCount));
      final Object scalarObject =
            VectorRandomRowSource.getNonWritableObject(
                scalarWritable, keyTypeInfo, keyObjectInspector);
      keyColExpr = new ExprNodeConstantDesc(keyTypeInfo, scalarObject);
    }

    /*
    System.out.println("*DEBUG* typeName " + typeName);
    System.out.println("*DEBUG* keyColExpr " + keyColExpr.toString());
    System.out.println("*DEBUG* keyList " + keyList.toString());
    System.out.println("*DEBUG* allKeyList " + allKeyList.toString());
    */

    generationSpecList.add(
        GenerationSpec.createValueList(typeInfo, keyList));
    explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);
    String columnName = "col" + columnNum++;
    columns.add(columnName);

    ExprNodeDesc listOrMapColExpr;
    listOrMapColExpr = new ExprNodeColumnDesc(typeInfo, columnName, "table", false);

    children.add(listOrMapColExpr);
    children.add(keyColExpr);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ allowNulls,  /* isUnicodeOk */ true,
        explicitDataTypePhysicalVariationList);

    String[] columnNames = columns.toArray(new String[0]);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    final GenericUDF udf = new GenericUDFIndex();

    ObjectInspector[] argumentOIs = new ObjectInspector[2];
    argumentOIs[0] = objectInspector;
    argumentOIs[1] = keyObjectInspector;

    final ObjectInspector outputObjectInspector = udf.initialize(argumentOIs);

    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(elementTypeInfo, udf, children);

    System.out.println("here");

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[IndexTestMode.count][];
    for (int i = 0; i < IndexTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      IndexTestMode indexTestMode = IndexTestMode.values()[i];
      switch (indexTestMode) {
      case ROW_MODE:
        if (!doRowCastTest(
              typeInfo,
              columns,
              children,
              udf, exprDesc,
              randomRows,
              rowSource.rowStructObjectInspector(),
              elementObjectInspector,
              outputTypeInfo,
              resultObjects)) {
          return false;
        }
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        if (!doVectorCastTest(
              typeInfo,
              columns,
              columnNames,
              rowSource.typeInfos(),
              rowSource.dataTypePhysicalVariations(),
              children,
              udf, exprDesc,
              indexTestMode,
              batchSource,
              exprDesc.getWritableObjectInspector(),
              outputTypeInfo,
              resultObjects)) {
          return false;
        }
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + indexTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < IndexTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        IndexTestMode indexTestMode = IndexTestMode.values()[v];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " " + indexTestMode +
                " result is NULL " + (vectorResult == null ? "YES" : "NO result " + vectorResult.toString()) +
                " does not match row-mode expected result is NULL " +
                (expectedResult == null ? "YES" : "NO result " + expectedResult.toString()) +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " " + indexTestMode +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        }
      }
    }

    return true;
  }

  private boolean doRowCastTest(TypeInfo typeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows,
      ObjectInspector rowInspector,
      ObjectInspector elementObjectInspector,
      TypeInfo outputTypeInfo,
      Object[] resultObjects)
          throws Exception {

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " outputTypeInfo " + outputTypeInfo.toString() +
        " indexTestMode ROW_MODE" +
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

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult;
      try {
        copyResult =
            ObjectInspectorUtils.copyToStandardObject(
                result, elementObjectInspector,
                ObjectInspectorCopyOption.WRITABLE);
      } catch (Exception e) {
        System.out.println("here");
        throw e;
      }
      resultObjects[i] = copyResult;
    }

    return true;
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow,
      ObjectInspector objectInspector, Object[] resultObjects) {

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

  private boolean doVectorCastTest(TypeInfo typeInfo,
      List<String> columns, String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      IndexTestMode indexTestMode,
      VectorRandomBatchSource batchSource,
      ObjectInspector objectInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (indexTestMode == IndexTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression =
        vectorizationContext.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
    vectorExpression.transientInit();

    if (indexTestMode == IndexTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo " + typeInfo.toString() +
          " indexTestMode " + indexTestMode +
          " vectorExpression " + vectorExpression.toString());
    }

    System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " indexTestMode " + indexTestMode +
        " vectorExpression " + vectorExpression.toString());
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
        new TypeInfo[] { outputTypeInfo }, new int[] { vectorExpression.getOutputColumnNum() });
    Object[] scrqtchRow = new Object[1];

    /*
    System.out.println(
        "*DEBUG* typeInfo1 " + typeInfo1.toString() +
        " typeInfo2 " + typeInfo2.toString() +
        " arithmeticTestMode " + arithmeticTestMode +
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
          objectInspector, resultObjects);
      rowIndex += batch.size;
    }

    return true;
  }
}
