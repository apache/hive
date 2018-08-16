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

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.SupportedTypes;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.TestVectorArithmetic.ColumnScalarMode;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorBetweenIn {

  @Test
  public void testTinyInt() throws Exception {
    Random random = new Random(5371);

    doBetweenIn(random, "tinyint");
  }

  @Test
  public void testSmallInt() throws Exception {
    Random random = new Random(2772);

    doBetweenIn(random, "smallint");
  }

  @Test
  public void testInt() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "int");
  }

  @Test
  public void testBigInt() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "bigint");
  }

  @Test
  public void testString() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "string");
  }

  @Test
  public void testTimestamp() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "timestamp");
  }

  @Test
  public void testDate() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "date");
  }

  @Test
  public void testFloat() throws Exception {
    Random random = new Random(7322);

    doBetweenIn(random, "float");
  }

  @Test
  public void testDouble() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "double");
  }

  @Test
  public void testChar() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "char(10)");
  }

  @Test
  public void testVarchar() throws Exception {
    Random random = new Random(12882);

    doBetweenIn(random, "varchar(15)");
  }

  @Test
  public void testDecimal() throws Exception {
    Random random = new Random(9300);

    doDecimalTests(random, /* tryDecimal64 */ false);
  }

  @Test
  public void testDecimal64() throws Exception {
    Random random = new Random(9300);

    doDecimalTests(random, /* tryDecimal64 */ true);
  }

  @Test
  public void testStruct() throws Exception {
    Random random = new Random(9300);

    doStructTests(random);
  }

  public enum BetweenInTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public enum BetweenInVariation {
    FILTER_BETWEEN,
    FILTER_NOT_BETWEEN,
    PROJECTION_BETWEEN,
    PROJECTION_NOT_BETWEEN,
    FILTER_IN,
    PROJECTION_IN;

    static final int count = values().length;

    final boolean isFilter;
    BetweenInVariation() {
      isFilter = name().startsWith("FILTER");
    }
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

  private void doDecimalTests(Random random, boolean tryDecimal64)
      throws Exception {
    for (TypeInfo typeInfo : decimalTypeInfos) {
      doBetweenIn(
          random, typeInfo.getTypeName(), tryDecimal64);
    }
  }

  private void doBetweenIn(Random random, String typeName)
      throws Exception {
    doBetweenIn(random, typeName, /* tryDecimal64 */ false);
  }

  private static final BetweenInVariation[] structInVarations =
      new BetweenInVariation[] { BetweenInVariation.FILTER_IN, BetweenInVariation.PROJECTION_IN };

  private void doStructTests(Random random) throws Exception {

    String typeName = "struct";

    // These are the only type supported for STRUCT IN by the VectorizationContext class.
    Set<String> allowedTypeNameSet = new HashSet<String>();
    allowedTypeNameSet.add("int");
    allowedTypeNameSet.add("bigint");
    allowedTypeNameSet.add("double");
    allowedTypeNameSet.add("string");

    // Only STRUCT type IN currently supported.
    for (BetweenInVariation betweenInVariation : structInVarations) {

      for (int i = 0; i < 4; i++) {
        typeName =
            VectorRandomRowSource.getDecoratedTypeName(
                random, typeName, SupportedTypes.ALL, allowedTypeNameSet,
                /* depth */ 0, /* maxDepth */ 1);

         doBetweenStructInVariation(
              random, typeName, betweenInVariation);
      }
    }
  }

  private void doBetweenIn(Random random, String typeName, boolean tryDecimal64)
          throws Exception {

    int subVariation;
    for (BetweenInVariation betweenInVariation : BetweenInVariation.values()) {
      subVariation = 0;
      while (true) {
        if (!doBetweenInVariation(
            random, typeName, tryDecimal64, betweenInVariation, subVariation)) {
          break;
        }
        subVariation++;
      }
    }
  }

  private boolean checkDecimal64(boolean tryDecimal64, TypeInfo typeInfo) {
    if (!tryDecimal64 || !(typeInfo instanceof DecimalTypeInfo)) {
      return false;
    }
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
    boolean result = HiveDecimalWritable.isPrecisionDecimal64(decimalTypeInfo.getPrecision());
    return result;
  }

  private void removeValue(List<Object> valueList, Object value) {
    valueList.remove(value);
  }

  private boolean needsValidDataTypeData(TypeInfo typeInfo) {
    if (!(typeInfo instanceof PrimitiveTypeInfo)) {
      return false;
    }
    PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    if (primitiveCategory == PrimitiveCategory.STRING ||
        primitiveCategory == PrimitiveCategory.CHAR ||
        primitiveCategory == PrimitiveCategory.VARCHAR ||
        primitiveCategory == PrimitiveCategory.BINARY) {
      return false;
    }
    return true;
  }

  private boolean doBetweenInVariation(Random random, String typeName,
      boolean tryDecimal64, BetweenInVariation betweenInVariation, int subVariation)
          throws Exception {

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

    boolean isDecimal64 = checkDecimal64(tryDecimal64, typeInfo);
    DataTypePhysicalVariation dataTypePhysicalVariation =
        (isDecimal64 ? DataTypePhysicalVariation.DECIMAL_64 : DataTypePhysicalVariation.NONE);
    final int decimal64Scale =
        (isDecimal64 ? ((DecimalTypeInfo) typeInfo).getScale() : 0);

    //----------------------------------------------------------------------------------------------

    ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            typeInfo);

    final int valueCount = 10 + random.nextInt(10);
    List<Object> valueList = new ArrayList<Object>(valueCount);
    for (int i = 0; i < valueCount; i++) {
      valueList.add(
          VectorRandomRowSource.randomWritable(
              random, typeInfo, objectInspector, dataTypePhysicalVariation, /* allowNull */ false));
    }

    final boolean isBetween =
        (betweenInVariation == BetweenInVariation.FILTER_BETWEEN ||
        betweenInVariation == BetweenInVariation.FILTER_NOT_BETWEEN ||
        betweenInVariation == BetweenInVariation.PROJECTION_BETWEEN ||
        betweenInVariation == BetweenInVariation.PROJECTION_NOT_BETWEEN);

    List<Object> compareList = new ArrayList<Object>();

    List<Object> sortedList = new ArrayList<Object>(valueCount);
    sortedList.addAll(valueList);

    Object exampleObject = valueList.get(0);
    WritableComparator writableComparator =
        WritableComparator.get((Class<? extends WritableComparable>) exampleObject.getClass());
    sortedList.sort(writableComparator);

    final boolean isInvert;
    if (isBetween) {

      // FILTER_BETWEEN
      // FILTER_NOT_BETWEEN
      // PROJECTION_BETWEEN
      // PROJECTION_NOT_BETWEEN
      isInvert =
          (betweenInVariation == BetweenInVariation.FILTER_NOT_BETWEEN ||
           betweenInVariation == BetweenInVariation.PROJECTION_NOT_BETWEEN);
      switch (subVariation) {
      case 0:
        // Range covers all values exactly.
        compareList.add(sortedList.get(0));
        compareList.add(sortedList.get(valueCount - 1));
        break;
      case 1:
        // Exclude the first and last sorted.
        compareList.add(sortedList.get(1));
        compareList.add(sortedList.get(valueCount - 2));
        break;
      case 2:
        // Only last 2 sorted.
        compareList.add(sortedList.get(valueCount - 2));
        compareList.add(sortedList.get(valueCount - 1));
        break;
      case 3:
      case 4:
      case 5:
      case 6:
        {
          // Choose 2 adjacent in the middle.
          Object min = sortedList.get(5);
          Object max = sortedList.get(6);
          compareList.add(min);
          compareList.add(max);
          if (subVariation == 4) {
            removeValue(valueList, min);
          } else if (subVariation == 5) {
            removeValue(valueList, max);
          } else if (subVariation == 6) {
            removeValue(valueList, min);
            removeValue(valueList, max);
          }
        }
        break;
      default:
        return false;
      }
    } else {

      // FILTER_IN.
      // PROJECTION_IN.
      isInvert = false;
      switch (subVariation) {
      case 0:
        // All values.
        compareList.addAll(valueList);
        break;
      case 1:
        // Don't include the first and last sorted.
        for (int i = 1; i < valueCount - 1; i++) {
          compareList.add(valueList.get(i));
        }
        break;
      case 2:
        // The even ones.
        for (int i = 2; i < valueCount; i += 2) {
          compareList.add(valueList.get(i));
        }
        break;
      case 3:
        {
          // Choose 2 adjacent in the middle.
          Object min = sortedList.get(5);
          Object max = sortedList.get(6);
          compareList.add(min);
          compareList.add(max);
          if (subVariation == 4) {
            removeValue(valueList, min);
          } else if (subVariation == 5) {
            removeValue(valueList, max);
          } else if (subVariation == 6) {
            removeValue(valueList, min);
            removeValue(valueList, max);
          }
        }
        break;
      default:
        return false;
      }
    }

    //----------------------------------------------------------------------------------------------

    GenerationSpec generationSpec = GenerationSpec.createValueList(typeInfo, valueList);

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();
    generationSpecList.add(generationSpec);
    explicitDataTypePhysicalVariationList.add(dataTypePhysicalVariation);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    List<String> columns = new ArrayList<String>();
    String col1Name = rowSource.columnNames().get(0);
    columns.add(col1Name);
    final ExprNodeDesc col1Expr = new ExprNodeColumnDesc(typeInfo, col1Name, "table", false);

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    if (isBetween) {
      children.add(new ExprNodeConstantDesc(new Boolean(isInvert)));
    }
    children.add(col1Expr);
    for (Object compareObject : compareList) {
      ExprNodeConstantDesc constDesc =
          new ExprNodeConstantDesc(
              typeInfo,
              VectorRandomRowSource.getNonWritableObject(
                  compareObject, typeInfo, objectInspector));
      children.add(constDesc);
    }

    String[] columnNames = columns.toArray(new String[0]);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    final GenericUDF udf;
    final ObjectInspector outputObjectInspector;
    if (isBetween) {

      udf = new GenericUDFBetween();

      // First argument is boolean invert. Arguments 1..3 are inspectors for range limits...
      ObjectInspector[] argumentOIs = new ObjectInspector[4];
      argumentOIs[0] = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      argumentOIs[1] = objectInspector;
      argumentOIs[2] = objectInspector;
      argumentOIs[3] = objectInspector;
      outputObjectInspector = udf.initialize(argumentOIs);
    } else {
      final int compareCount = compareList.size();
      udf = new GenericUDFIn();
      ObjectInspector[] argumentOIs = new ObjectInspector[compareCount];
      ConstantObjectInspector constantObjectInspector =
          (ConstantObjectInspector) children.get(1).getWritableObjectInspector();
      for (int i = 0; i < compareCount; i++) {
        argumentOIs[i] = constantObjectInspector;
      }
      outputObjectInspector = udf.initialize(argumentOIs);
    }

    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(
            TypeInfoFactory.booleanTypeInfo, udf, children);

    return executeTestModesAndVerify(
        typeInfo, betweenInVariation, compareList, columns, columnNames, children,
        udf, exprDesc,
        randomRows, rowSource, batchSource, outputTypeInfo,
        /* skipAdaptor */ false);
  }

  private boolean doBetweenStructInVariation(Random random, String structTypeName,
      BetweenInVariation betweenInVariation)
          throws Exception {

    StructTypeInfo structTypeInfo =
        (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(structTypeName);

    ObjectInspector structObjectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            structTypeInfo);

    final int valueCount = 10 + random.nextInt(10);
    List<Object> valueList = new ArrayList<Object>(valueCount);
    for (int i = 0; i < valueCount; i++) {
      valueList.add(
          VectorRandomRowSource.randomWritable(
              random, structTypeInfo, structObjectInspector, DataTypePhysicalVariation.NONE,
              /* allowNull */ false));
    }

    final boolean isInvert = false;
 
    // No convenient WritableComparator / WritableComparable available for STRUCT.
    List<Object> compareList = new ArrayList<Object>();

    Set<Integer> includedSet = new HashSet<Integer>();
    final int chooseLimit = 4 + random.nextInt(valueCount/2);
    int chooseCount = 0;
    while (chooseCount < chooseLimit) {
      final int index = random.nextInt(valueCount);
      if (includedSet.contains(index)) {
        continue;
      }
      includedSet.add(index);
      compareList.add(valueList.get(index));
      chooseCount++;
    }

    //----------------------------------------------------------------------------------------------

    GenerationSpec structGenerationSpec = GenerationSpec.createValueList(structTypeInfo, valueList);

    List<GenerationSpec> structGenerationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> structExplicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();
    structGenerationSpecList.add(structGenerationSpec);
    structExplicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

    VectorRandomRowSource structRowSource = new VectorRandomRowSource();

    structRowSource.initGenerationSpecSchema(
        random, structGenerationSpecList, /* maxComplexDepth */ 0, /* allowNull */ true,
        structExplicitDataTypePhysicalVariationList);

    Object[][] structRandomRows = structRowSource.randomRows(100000);

    // ---------------------------------------------------------------------------------------------

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<TypeInfo> fieldTypeInfoList = structTypeInfo.getAllStructFieldTypeInfos();
    final int fieldCount = fieldTypeInfoList.size();
    for (int i = 0; i < fieldCount; i++) {
      GenerationSpec generationSpec = GenerationSpec.createOmitGeneration(fieldTypeInfoList.get(i));
      generationSpecList.add(generationSpec);
      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);
    }

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    Object[][] randomRows = rowSource.randomRows(100000);

    final int rowCount = randomRows.length;
    for (int r = 0; r < rowCount; r++) {
      List<Object> fieldValueList = (ArrayList) structRandomRows[r][0]; 
      for (int f = 0; f < fieldCount; f++) {
        randomRows[r][f] = fieldValueList.get(f);
      }
    }

    // ---------------------------------------------------------------------------------------------

    // Currently, STRUCT IN vectorization assumes a GenericUDFStruct.

    List<ObjectInspector> structUdfObjectInspectorList = new ArrayList<ObjectInspector>();
    List<ExprNodeDesc> structUdfChildren = new ArrayList<ExprNodeDesc>(fieldCount);
    List<String> rowColumnNameList = rowSource.columnNames();
    for (int i = 0; i < fieldCount; i++) {
      TypeInfo fieldTypeInfo = fieldTypeInfoList.get(i);
      ExprNodeColumnDesc fieldExpr =
          new ExprNodeColumnDesc(
              fieldTypeInfo, rowColumnNameList.get(i), "table", false);
      structUdfChildren.add(fieldExpr);
      ObjectInspector fieldObjectInspector =
          VectorRandomRowSource.getObjectInspector(fieldTypeInfo, DataTypePhysicalVariation.NONE);
      structUdfObjectInspectorList.add(fieldObjectInspector);
    }
    StandardStructObjectInspector structUdfObjectInspector =
        ObjectInspectorFactory.
            getStandardStructObjectInspector(rowColumnNameList, structUdfObjectInspectorList);
    String structUdfTypeName = structUdfObjectInspector.getTypeName();
    TypeInfo structUdfTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(structUdfTypeName);

    String structFuncText = "struct";
    FunctionInfo fi = FunctionRegistry.getFunctionInfo(structFuncText);
    GenericUDF genericUDF = fi.getGenericUDF();
    ExprNodeDesc col1Expr =
        new ExprNodeGenericFuncDesc(
            structUdfObjectInspector, genericUDF, structFuncText, structUdfChildren);

    // ---------------------------------------------------------------------------------------------

    List<String> columns = new ArrayList<String>();

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    for (int i = 0; i < compareList.size(); i++) {
      Object compareObject = compareList.get(i);
      ExprNodeConstantDesc constDesc =
          new ExprNodeConstantDesc(
              structUdfTypeInfo,
              VectorRandomRowSource.getNonWritableObject(
                  compareObject, structUdfTypeInfo, structUdfObjectInspector));
      children.add(constDesc);
    }

    for (int i = 0; i < fieldCount; i++) {
      columns.add(rowColumnNameList.get(i));
    }

    String[] columnNames = columns.toArray(new String[0]);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    // ---------------------------------------------------------------------------------------------

    final GenericUDF udf = new GenericUDFIn();
    final int compareCount = compareList.size();
    ObjectInspector[] argumentOIs = new ObjectInspector[compareCount];
    for (int i = 0; i < compareCount; i++) {
      argumentOIs[i] = structUdfObjectInspector;
    }
    final ObjectInspector outputObjectInspector = udf.initialize(argumentOIs);

    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(
            TypeInfoFactory.booleanTypeInfo, udf, children);

    return executeTestModesAndVerify(
        structUdfTypeInfo, betweenInVariation, compareList, columns, columnNames, children,
        udf, exprDesc,
        randomRows, rowSource, batchSource, outputTypeInfo,
        /* skipAdaptor */ true);
  }

  private boolean executeTestModesAndVerify(TypeInfo typeInfo,
      BetweenInVariation betweenInVariation, List<Object> compareList,
      List<String> columns, String[] columnNames, List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows,
      VectorRandomRowSource rowSource, VectorRandomBatchSource batchSource,
      TypeInfo outputTypeInfo, boolean skipAdaptor)
          throws Exception {

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[BetweenInTestMode.count][];
    for (int i = 0; i < BetweenInTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      BetweenInTestMode betweenInTestMode = BetweenInTestMode.values()[i];
      switch (betweenInTestMode) {
      case ROW_MODE:
        if (!doRowCastTest(
              typeInfo,
              betweenInVariation,
              compareList,
              columns,
              children,
              udf, exprDesc,
              randomRows,
              rowSource.rowStructObjectInspector(),
              resultObjects)) {
          return false;
        }
        break;
      case ADAPTOR:
         if (skipAdaptor) {
           continue;
         }
      case VECTOR_EXPRESSION:
        if (!doVectorCastTest(
              typeInfo,
              betweenInVariation,
              compareList,
              columns,
              columnNames,
              rowSource.typeInfos(),
              rowSource.dataTypePhysicalVariations(),
              children,
              udf, exprDesc,
              betweenInTestMode,
              batchSource,
              exprDesc.getWritableObjectInspector(),
              outputTypeInfo,
              resultObjects)) {
          return false;
        }
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + betweenInTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < BetweenInTestMode.count; v++) {
        BetweenInTestMode betweenInTestMode = BetweenInTestMode.values()[v];
        if (skipAdaptor) {
          continue;
        }
        Object vectorResult = resultObjectsArray[v][i];
        if (betweenInVariation.isFilter &&
            expectedResult == null &&
            vectorResult != null) {
          // This is OK.
          boolean vectorBoolean = ((BooleanWritable) vectorResult).get();
          if (vectorBoolean) {
            Assert.fail(
                "Row " + i +
                " typeName " + typeInfo.getTypeName() +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + betweenInVariation +
                " " + betweenInTestMode +
                " result is NOT NULL and true" +
                " does not match row-mode expected result is NULL which means false here" +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        } else if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeInfo.getTypeName() +
                " " + betweenInVariation +
                " " + betweenInTestMode +
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
                " sourceTypeName " + typeInfo.getTypeName() +
                " " + betweenInVariation +
                " " + betweenInTestMode +
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
      BetweenInVariation betweenInVariation, List<Object> compareList,
      List<String> columns, List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows,
      ObjectInspector rowInspector, Object[] resultObjects)
          throws Exception {

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " targetTypeInfo " + targetTypeInfo +
        " betweenInTestMode ROW_MODE" +
        " exprDesc " + exprDesc.toString());
    */

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);

    evaluator.initialize(rowInspector);

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              result, PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
              ObjectInspectorCopyOption.WRITABLE);
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
      BetweenInVariation betweenInVariation, List<Object> compareList,
      List<String> columns, String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      BetweenInTestMode betweenInTestMode,
      VectorRandomBatchSource batchSource,
      ObjectInspector objectInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (betweenInTestMode == BetweenInTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    final boolean isFilter = betweenInVariation.isFilter;

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression =
        vectorizationContext.getVectorExpression(exprDesc,
            (isFilter ?
                VectorExpressionDescriptor.Mode.FILTER :
                VectorExpressionDescriptor.Mode.PROJECTION));
    vectorExpression.transientInit();

    if (betweenInTestMode == BetweenInTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo " + typeInfo.toString() +
          " betweenInTestMode " + betweenInTestMode +
          " betweenInVariation " + betweenInVariation +
          " vectorExpression " + vectorExpression.toString());
    }

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " betweenInTestMode " + betweenInTestMode +
        " betweenInVariation " + betweenInVariation +
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

    VectorExtractRow resultVectorExtractRow = null;
    Object[] scrqtchRow = null;
    if (!isFilter) {
      resultVectorExtractRow = new VectorExtractRow();
      final int outputColumnNum = vectorExpression.getOutputColumnNum();
      resultVectorExtractRow.init(
          new TypeInfo[] { outputTypeInfo }, new int[] { outputColumnNum });
      scrqtchRow = new Object[1];
    }

    boolean copySelectedInUse = false;
    int[] copySelected = new int[VectorizedRowBatch.DEFAULT_SIZE];

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      final int originalBatchSize = batch.size;
      if (isFilter) {
        copySelectedInUse = batch.selectedInUse;
        if (batch.selectedInUse) {
          System.arraycopy(batch.selected, 0, copySelected, 0, originalBatchSize);
        }
      }

      // In filter mode, the batch size can be made smaller.
      vectorExpression.evaluate(batch);

      if (!isFilter) {
        extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
            objectInspector, resultObjects);
      } else {
        final int currentBatchSize = batch.size;
        if (copySelectedInUse && batch.selectedInUse) {
          int selectIndex = 0;
          for (int i = 0; i < originalBatchSize; i++) {
            final int originalBatchIndex = copySelected[i];
            final boolean booleanResult;
            if (selectIndex < currentBatchSize && batch.selected[selectIndex] == originalBatchIndex) {
              booleanResult = true;
              selectIndex++;
            } else {
              booleanResult = false;
            }
            resultObjects[rowIndex + i] = new BooleanWritable(booleanResult);
          }
        } else if (batch.selectedInUse) {
          int selectIndex = 0;
          for (int i = 0; i < originalBatchSize; i++) {
            final boolean booleanResult;
            if (selectIndex < currentBatchSize && batch.selected[selectIndex] == i) {
              booleanResult = true;
              selectIndex++;
            } else {
              booleanResult = false;
            }
            resultObjects[rowIndex + i] = new BooleanWritable(booleanResult);
          }
        } else if (currentBatchSize == 0) {
          // Whole batch got zapped.
          for (int i = 0; i < originalBatchSize; i++) {
            resultObjects[rowIndex + i] = new BooleanWritable(false);
          }
        } else {
          // Every row kept.
          for (int i = 0; i < originalBatchSize; i++) {
            resultObjects[rowIndex + i] = new BooleanWritable(true);
          }
        }
      }

      rowIndex += originalBatchSize;
    }

    return true;
  }
}
