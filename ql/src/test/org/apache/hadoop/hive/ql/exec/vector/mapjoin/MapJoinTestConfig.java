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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowCollectorTestOperatorBase;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowVectorCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjectsMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinBaseOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOuterFilteredOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VerifyFastRow;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class MapJoinTestConfig {

  public static enum MapJoinTestImplementation {
    ROW_MODE_HASH_MAP,
    ROW_MODE_OPTIMIZED,
    VECTOR_PASS_THROUGH,
    NATIVE_VECTOR_OPTIMIZED,
    NATIVE_VECTOR_FAST
  }

  public static boolean isVectorOutput(MapJoinTestImplementation mapJoinImplementation) {
    return
        (mapJoinImplementation != MapJoinTestImplementation.ROW_MODE_HASH_MAP &&
         mapJoinImplementation != MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
  }

  /*
   * This test collector operator is for MapJoin row-mode.
   */
  public static class TestMultiSetCollectorOperator extends RowCollectorTestOperator {

    private final RowTestObjectsMultiSet testRowMultiSet;

    public TestMultiSetCollectorOperator(
        ObjectInspector[] outputObjectInspectors,
        RowTestObjectsMultiSet testRowMultiSet) {
      super(outputObjectInspectors);
      this.testRowMultiSet = testRowMultiSet;
    }

    public RowTestObjectsMultiSet getTestRowMultiSet() {
      return testRowMultiSet;
    }

    public void nextTestRow(RowTestObjects testRow) {
      testRowMultiSet.add(testRow, RowTestObjectsMultiSet.RowFlag.NONE);
    }

    @Override
    public String getName() {
      return TestMultiSetCollectorOperator.class.getSimpleName();
    }
  }

  public static class TestMultiSetVectorCollectorOperator extends RowVectorCollectorTestOperator {

    private final RowTestObjectsMultiSet testRowMultiSet;

    public RowTestObjectsMultiSet getTestRowMultiSet() {
      return testRowMultiSet;
    }

    public TestMultiSetVectorCollectorOperator(TypeInfo[] outputTypeInfos,
        ObjectInspector[] outputObjectInspectors, RowTestObjectsMultiSet testRowMultiSet)
            throws HiveException {
      super(outputTypeInfos, outputObjectInspectors);
      this.testRowMultiSet = testRowMultiSet;
     }

    public TestMultiSetVectorCollectorOperator(
        int[] outputProjectionColumnNums,
        TypeInfo[] outputTypeInfos,
        ObjectInspector[] outputObjectInspectors,
        RowTestObjectsMultiSet testRowMultiSet) throws HiveException {
      super(outputProjectionColumnNums, outputTypeInfos, outputObjectInspectors);
      this.testRowMultiSet = testRowMultiSet;
    }

    public void nextTestRow(RowTestObjects testRow) {
      testRowMultiSet.add(testRow, RowTestObjectsMultiSet.RowFlag.NONE);
    }

    @Override
    public String getName() {
      return TestMultiSetVectorCollectorOperator.class.getSimpleName();
    }
  }

  public static MapJoinDesc createMapJoinDesc(MapJoinTestDescription testDesc) {
    return createMapJoinDesc(testDesc, false);
  }

  public static MapJoinDesc createMapJoinDesc(MapJoinTestDescription testDesc,
      boolean isFullOuterIntersect) {

    MapJoinDesc mapJoinDesc = new MapJoinDesc();

    mapJoinDesc.setPosBigTable(0);

    List<ExprNodeDesc> bigTableKeyExpr = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < testDesc.bigTableKeyColumnNums.length; i++) {
      bigTableKeyExpr.add(
          new ExprNodeColumnDesc(
              testDesc.bigTableKeyTypeInfos[i],
              testDesc.bigTableKeyColumnNames[i], "B", false));
    }

    Map<Byte, List<ExprNodeDesc>> keyMap = new HashMap<Byte, List<ExprNodeDesc>>();
    keyMap.put((byte) 0, bigTableKeyExpr);

    // Big Table expression includes all columns -- keys and extra (value) columns.
    // UNDONE: Assumes all values retained...
    List<ExprNodeDesc> bigTableExpr = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < testDesc.bigTableColumnNames.length; i++) {
      bigTableExpr.add(
          new ExprNodeColumnDesc(
              testDesc.bigTableTypeInfos[i],
              testDesc.bigTableColumnNames[i], "B", false));
    }

    Map<Byte, List<ExprNodeDesc>> exprMap = new HashMap<Byte, List<ExprNodeDesc>>();
    exprMap.put((byte) 0, bigTableExpr);

    List<ExprNodeDesc> smallTableKeyExpr = new ArrayList<ExprNodeDesc>();

     for (int i = 0; i < testDesc.smallTableKeyTypeInfos.length; i++) {
      ExprNodeColumnDesc exprNodeColumnDesc =
          new ExprNodeColumnDesc(
              testDesc.smallTableKeyTypeInfos[i],
              testDesc.smallTableKeyColumnNames[i], "S", false);
      smallTableKeyExpr.add(exprNodeColumnDesc);
    }

    // Retained Small Table keys and values.
    List<ExprNodeDesc> smallTableExpr = new ArrayList<ExprNodeDesc>();
    final int smallTableRetainKeySize = testDesc.smallTableRetainKeyColumnNums.length;
    for (int i = 0; i < smallTableRetainKeySize; i++) {
      int smallTableKeyColumnNum = testDesc.smallTableRetainKeyColumnNums[i];
      smallTableExpr.add(
          new ExprNodeColumnDesc(
              testDesc.smallTableTypeInfos[smallTableKeyColumnNum],
              testDesc.smallTableColumnNames[smallTableKeyColumnNum], "S", false));
    }

    final int smallTableRetainValueSize = testDesc.smallTableRetainValueColumnNums.length;
    for (int i = 0; i < smallTableRetainValueSize; i++) {
      int smallTableValueColumnNum =
          smallTableRetainKeySize + testDesc.smallTableRetainValueColumnNums[i];
      smallTableExpr.add(
          new ExprNodeColumnDesc(
              testDesc.smallTableTypeInfos[smallTableValueColumnNum],
              testDesc.smallTableColumnNames[smallTableValueColumnNum], "S", false));
    }

    keyMap.put((byte) 1, smallTableKeyExpr);
    exprMap.put((byte) 1, smallTableExpr);

    mapJoinDesc.setKeys(keyMap);
    mapJoinDesc.setExprs(exprMap);

    Byte[] order = new Byte[] {(byte) 0, (byte) 1};
    mapJoinDesc.setTagOrder(order);
    mapJoinDesc.setNoOuterJoin(
        testDesc.vectorMapJoinVariation != VectorMapJoinVariation.OUTER &&
        testDesc.vectorMapJoinVariation != VectorMapJoinVariation.FULL_OUTER);

    Map<Byte, List<ExprNodeDesc>> filterMap = new HashMap<Byte, List<ExprNodeDesc>>();
    filterMap.put((byte) 0, new ArrayList<ExprNodeDesc>());  // None.
    mapJoinDesc.setFilters(filterMap);

    List<Integer> bigTableRetainColumnNumsList = intArrayToList(testDesc.bigTableRetainColumnNums);
    Map<Byte, List<Integer>> retainListMap = new HashMap<Byte, List<Integer>>();
    retainListMap.put((byte) 0, bigTableRetainColumnNumsList);

    // For now, just small table keys/values...
    if (testDesc.smallTableRetainKeyColumnNums.length == 0) {

      // Just the value columns numbers with retain.
      List<Integer> smallTableValueRetainColumnNumsList =
          intArrayToList(testDesc.smallTableRetainValueColumnNums);

      retainListMap.put((byte) 1, smallTableValueRetainColumnNumsList);
    } else {

      // Both the key/value columns numbers.

      // Zero and above numbers indicate a big table key is needed for
      // small table result "area".

      // Negative numbers indicate a column to be (deserialize) read from the small table's
      // LazyBinary value row.

      ArrayList<Integer> smallTableValueIndicesNumsList = new ArrayList<Integer>();;
      for (int i = 0; i < testDesc.smallTableRetainKeyColumnNums.length; i++) {
        smallTableValueIndicesNumsList.add(testDesc.smallTableRetainKeyColumnNums[i]);
      }
      for (int i = 0; i < testDesc.smallTableRetainValueColumnNums.length; i++) {
        smallTableValueIndicesNumsList.add(-testDesc.smallTableRetainValueColumnNums[i] - 1);
      }
      int[] smallTableValueIndicesNums =
          ArrayUtils.toPrimitive(smallTableValueIndicesNumsList.toArray(new Integer[0]));

      Map<Byte, int[]> valueIndicesMap = new HashMap<Byte, int[]>();
      valueIndicesMap.put((byte) 1, smallTableValueIndicesNums);
      mapJoinDesc.setValueIndices(valueIndicesMap);
    }
    mapJoinDesc.setRetainList(retainListMap);

    switch (testDesc.mapJoinPlanVariation) {
    case DYNAMIC_PARTITION_HASH_JOIN:
      // FULL OUTER which behaves differently for dynamic partition hash join.
      mapJoinDesc.setDynamicPartitionHashJoin(true);
      break;
    default:
      throw new RuntimeException(
          "Unexpected map join plan variation " + testDesc.mapJoinPlanVariation);
    }

    int joinDescType;
    switch (testDesc.vectorMapJoinVariation) {
    case INNER:
    case INNER_BIG_ONLY:
      joinDescType = JoinDesc.INNER_JOIN;
      break;
    case LEFT_SEMI:
      joinDescType = JoinDesc.LEFT_SEMI_JOIN;
      break;
    case LEFT_ANTI:
        joinDescType = JoinDesc.ANTI_JOIN;
        break;
    case OUTER:
      joinDescType = JoinDesc.LEFT_OUTER_JOIN;
      break;
    case FULL_OUTER:
      joinDescType = JoinDesc.FULL_OUTER_JOIN;
      break;
    default:
      throw new RuntimeException("unknown operator variation " + testDesc.vectorMapJoinVariation);
    }
    JoinCondDesc[] conds = new JoinCondDesc[1];
    conds[0] = new JoinCondDesc(0, 1, joinDescType);
    mapJoinDesc.setConds(conds);

    TableDesc keyTableDesc = PlanUtils.getMapJoinKeyTableDesc(testDesc.hiveConf, PlanUtils
        .getFieldSchemasFromColumnList(smallTableKeyExpr, ""));
    mapJoinDesc.setKeyTblDesc(keyTableDesc);

    // Small Table expression value columns.
    List<ExprNodeDesc> smallTableValueExpr = new ArrayList<ExprNodeDesc>();

    // All Small Table keys and values.
    for (int i = 0; i < testDesc.smallTableValueColumnNames.length; i++) {
      smallTableValueExpr.add(
          new ExprNodeColumnDesc(
              testDesc.smallTableValueTypeInfos[i],
              testDesc.smallTableValueColumnNames[i], "S", false));
    }

    TableDesc valueTableDesc = PlanUtils.getMapJoinValueTableDesc(
        PlanUtils.getFieldSchemasFromColumnList(smallTableValueExpr, ""));
    ArrayList<TableDesc> valueTableDescsList = new ArrayList<TableDesc>();

    // Big Table entry, then Small Table entry.
    valueTableDescsList.add(null);
    valueTableDescsList.add(valueTableDesc);
    mapJoinDesc.setValueTblDescs(valueTableDescsList);
    mapJoinDesc.setValueFilteredTblDescs(valueTableDescsList);

    mapJoinDesc.setOutputColumnNames(Arrays.asList(testDesc.outputColumnNames));

    return mapJoinDesc;
  }

  public static VectorMapJoinDesc createVectorMapJoinDesc(MapJoinTestDescription testDesc) {
    VectorMapJoinDesc vectorDesc = new VectorMapJoinDesc();
    vectorDesc.setHashTableImplementationType(HashTableImplementationType.FAST);
    HashTableKind hashTableKind;
    switch (testDesc.vectorMapJoinVariation) {
    case INNER:
      hashTableKind = HashTableKind.HASH_MAP;
      break;
    case INNER_BIG_ONLY:
      hashTableKind = HashTableKind.HASH_MULTISET;
      break;
    case LEFT_SEMI:
    case LEFT_ANTI:
      hashTableKind = HashTableKind.HASH_SET;
      break;
    case OUTER:
    case FULL_OUTER:
      hashTableKind = HashTableKind.HASH_MAP;
      break;
    default:
      throw new RuntimeException("unknown operator variation " + testDesc.vectorMapJoinVariation);
    }
    vectorDesc.setHashTableKind(hashTableKind);
    HashTableKeyType hashTableKeyType = HashTableKeyType.MULTI_KEY;   // Assume.
    if (testDesc.bigTableKeyTypeInfos.length == 1) {
      switch (((PrimitiveTypeInfo) testDesc.bigTableKeyTypeInfos[0]).getPrimitiveCategory()) {
      case BOOLEAN:
        hashTableKeyType = HashTableKeyType.BOOLEAN;
        break;
      case BYTE:
        hashTableKeyType = HashTableKeyType.BYTE;
        break;
      case SHORT:
        hashTableKeyType = HashTableKeyType.SHORT;
        break;
      case INT:
        hashTableKeyType = HashTableKeyType.INT;
        break;
      case LONG:
        hashTableKeyType = HashTableKeyType.LONG;
        break;
      case STRING:
        hashTableKeyType = HashTableKeyType.STRING;
        break;
      default:
        // Stay with MULTI_KEY
      }
    }
    vectorDesc.setHashTableKeyType(hashTableKeyType);
    vectorDesc.setVectorMapJoinVariation(testDesc.vectorMapJoinVariation);
    vectorDesc.setMinMaxEnabled(false);

    VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();

    vectorMapJoinInfo.setBigTableKeyColumnMap(testDesc.bigTableKeyColumnNums);
    vectorMapJoinInfo.setBigTableKeyColumnNames(testDesc.bigTableKeyColumnNames);
    vectorMapJoinInfo.setBigTableKeyTypeInfos(testDesc.bigTableKeyTypeInfos);
    vectorMapJoinInfo.setSlimmedBigTableKeyExpressions(null);

    vectorDesc.setAllBigTableKeyExpressions(null);

    vectorMapJoinInfo.setBigTableValueColumnMap(testDesc.bigTableColumnNums);
    vectorMapJoinInfo.setBigTableValueColumnNames(testDesc.bigTableColumnNames);
    vectorMapJoinInfo.setBigTableValueTypeInfos(testDesc.bigTableTypeInfos);
    vectorMapJoinInfo.setSlimmedBigTableValueExpressions(null);

    vectorDesc.setAllBigTableValueExpressions(null);

    vectorMapJoinInfo.setBigTableFilterExpressions(new VectorExpression[0]);


    /*
     * Column mapping.
     */
    VectorColumnOutputMapping bigTableRetainMapping =
        new VectorColumnOutputMapping("Big Table Retain Mapping");

    VectorColumnOutputMapping nonOuterSmallTableKeyMapping =
        new VectorColumnOutputMapping("Non Outer Small Table Key Key Mapping");

    VectorColumnOutputMapping outerSmallTableKeyMapping =
        new VectorColumnOutputMapping("Outer Small Table Key Mapping");

    VectorColumnSourceMapping fullOuterSmallTableKeyMapping =
        new VectorColumnSourceMapping("Full Outer Small Table Key Mapping");

    VectorColumnSourceMapping projectionMapping =
        new VectorColumnSourceMapping("Projection Mapping");

    int nextOutputColumn = 0;

    final int bigTableRetainedSize = testDesc.bigTableRetainColumnNums.length;
    for (int i = 0; i < bigTableRetainedSize; i++) {
      final int batchColumnIndex = testDesc.bigTableRetainColumnNums[i];
      TypeInfo typeInfo = testDesc.bigTableTypeInfos[i];
      projectionMapping.add(
          nextOutputColumn, batchColumnIndex, typeInfo);
      // Collect columns we copy from the big table batch to the overflow batch.
      if (!bigTableRetainMapping.containsOutputColumn(batchColumnIndex)) {

        // Tolerate repeated use of a big table column.
        bigTableRetainMapping.add(batchColumnIndex, batchColumnIndex, typeInfo);
      }
      nextOutputColumn++;
    }

    boolean isOuterJoin =
        (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.OUTER ||
         testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER);

    int emulateScratchColumn = testDesc.bigTableTypeInfos.length;

    VectorColumnOutputMapping smallTableKeyOutputMapping =
        new VectorColumnOutputMapping("Small Table Key Output Mapping");
    final int smallTableKeyRetainSize = testDesc.smallTableRetainKeyColumnNums.length;
    for (int i = 0; i < testDesc.smallTableRetainKeyColumnNums.length; i++) {
      final int smallTableKeyColumnNum = testDesc.smallTableRetainKeyColumnNums[i];
      final int bigTableKeyColumnNum = testDesc.bigTableKeyColumnNums[smallTableKeyColumnNum];
      TypeInfo keyTypeInfo = testDesc.smallTableKeyTypeInfos[smallTableKeyColumnNum];
      if (!isOuterJoin) {
        // Project the big table key into the small table result "area".
        projectionMapping.add(nextOutputColumn, bigTableKeyColumnNum, keyTypeInfo);
        if (!bigTableRetainMapping.containsOutputColumn(bigTableKeyColumnNum)) {
          nonOuterSmallTableKeyMapping.add(bigTableKeyColumnNum, bigTableKeyColumnNum, keyTypeInfo);
        }
      } else {
        outerSmallTableKeyMapping.add(bigTableKeyColumnNum, emulateScratchColumn, keyTypeInfo);
        projectionMapping.add(nextOutputColumn, emulateScratchColumn, keyTypeInfo);

        // For FULL OUTER MapJoin, we need to be able to deserialize a Small Table key
        // into the output result.
        fullOuterSmallTableKeyMapping.add(smallTableKeyColumnNum, emulateScratchColumn, keyTypeInfo);
        emulateScratchColumn++;
      }
      nextOutputColumn++;
    }

    // The order of the fields in the LazyBinary small table value must be used, so
    // we use the source ordering flavor for the mapping.
    VectorColumnSourceMapping smallTableValueMapping =
        new VectorColumnSourceMapping("Small Table Value Mapping");
    for (int i = 0; i < testDesc.smallTableValueTypeInfos.length; i++) {
      smallTableValueMapping.add(i, emulateScratchColumn, testDesc.smallTableValueTypeInfos[i]);
      projectionMapping.add(nextOutputColumn, emulateScratchColumn, testDesc.smallTableValueTypeInfos[i]);
      emulateScratchColumn++;
      nextOutputColumn++;
    }

    // Convert dynamic arrays and maps to simple arrays.

    bigTableRetainMapping.finalize();
    vectorMapJoinInfo.setBigTableRetainColumnMap(bigTableRetainMapping.getOutputColumns());
    vectorMapJoinInfo.setBigTableRetainTypeInfos(bigTableRetainMapping.getTypeInfos());

    nonOuterSmallTableKeyMapping.finalize();
    vectorMapJoinInfo.setNonOuterSmallTableKeyColumnMap(nonOuterSmallTableKeyMapping.getOutputColumns());
    vectorMapJoinInfo.setNonOuterSmallTableKeyTypeInfos(nonOuterSmallTableKeyMapping.getTypeInfos());

    outerSmallTableKeyMapping.finalize();
    fullOuterSmallTableKeyMapping.finalize();

    vectorMapJoinInfo.setOuterSmallTableKeyMapping(outerSmallTableKeyMapping);
    vectorMapJoinInfo.setFullOuterSmallTableKeyMapping(fullOuterSmallTableKeyMapping);

    smallTableValueMapping.finalize();

    vectorMapJoinInfo.setSmallTableValueMapping(smallTableValueMapping);

    projectionMapping.finalize();

    // Verify we added an entry for each output.
    assert projectionMapping.isSourceSequenceGood();

    vectorMapJoinInfo.setProjectionMapping(projectionMapping);

    if (projectionMapping.getCount() != testDesc.outputColumnNames.length) {
      throw new RuntimeException();
    };

    vectorDesc.setVectorMapJoinInfo(vectorMapJoinInfo);

    return vectorDesc;
  }

  public static VectorMapJoinCommonOperator createNativeVectorMapJoinOperator(
      VectorMapJoinVariation VectorMapJoinVariation, MapJoinDesc mapJoinDesc,
      VectorMapJoinDesc vectorDesc, VectorizationContext vContext)
          throws HiveException {
    VectorMapJoinCommonOperator operator;
    switch (vectorDesc.getHashTableKeyType()) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      switch (VectorMapJoinVariation) {
      case INNER:
        operator =
            new VectorMapJoinInnerLongOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case INNER_BIG_ONLY:
        operator =
            new VectorMapJoinInnerBigOnlyLongOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case LEFT_SEMI:
        operator =
            new VectorMapJoinLeftSemiLongOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case OUTER:
        operator =
            new VectorMapJoinOuterLongOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case FULL_OUTER:
        operator =
            new VectorMapJoinFullOuterLongOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      default:
        throw new RuntimeException("unknown operator variation " + VectorMapJoinVariation);
      }
      break;
    case STRING:
      switch (VectorMapJoinVariation) {
      case INNER:
        operator =
            new VectorMapJoinInnerStringOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case INNER_BIG_ONLY:
        operator =
            new VectorMapJoinInnerBigOnlyStringOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case LEFT_SEMI:
        operator =
            new VectorMapJoinLeftSemiStringOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case OUTER:
        operator =
            new VectorMapJoinOuterStringOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
      case FULL_OUTER:
        operator =
            new VectorMapJoinFullOuterStringOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      default:
        throw new RuntimeException("unknown operator variation " + VectorMapJoinVariation);
      }
      break;
    case MULTI_KEY:
      switch (VectorMapJoinVariation) {
      case INNER:
        operator =
            new VectorMapJoinInnerMultiKeyOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case INNER_BIG_ONLY:
        operator =
            new VectorMapJoinInnerBigOnlyMultiKeyOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case LEFT_SEMI:
        operator =
            new VectorMapJoinLeftSemiMultiKeyOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case OUTER:
        operator =
            new VectorMapJoinOuterMultiKeyOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      case FULL_OUTER:
        operator =
            new VectorMapJoinFullOuterMultiKeyOperator(new CompilationOpContext(),
                mapJoinDesc, vContext, vectorDesc);
        break;
      default:
        throw new RuntimeException("unknown operator variation " + VectorMapJoinVariation);
      }
      break;
    default:
      throw new RuntimeException("Unknown hash table key type " + vectorDesc.getHashTableKeyType());
    }
    System.out.println("*BENCHMARK* createNativeVectorMapJoinOperator " +
        operator.getClass().getSimpleName());
    return operator;
  }

  public static VectorizationContext createVectorizationContext(MapJoinTestDescription testDesc)
      throws HiveException {
    VectorizationContext vContext =
        new VectorizationContext("test", testDesc.bigTableColumnNameList);

    boolean isOuterJoin =
        (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.OUTER ||
         testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER);

    if (isOuterJoin) {

      // We need physical columns.
      for (int i = 0; i < testDesc.smallTableRetainKeyColumnNums.length; i++) {
        final int smallTableKeyRetainColumnNum = testDesc.smallTableRetainKeyColumnNums[i];
        vContext.allocateScratchColumn(testDesc.smallTableKeyTypeInfos[smallTableKeyRetainColumnNum]);
      }
    }

    // Create scratch columns to hold small table results.
    for (int i = 0; i < testDesc.smallTableRetainValueColumnNums.length; i++) {
      vContext.allocateScratchColumn(testDesc.smallTableValueTypeInfos[i]);
    }
    return vContext;
  }

  private static boolean hasFilter(MapJoinDesc mapJoinDesc, int alias) {
    int[][] filterMaps = mapJoinDesc.getFilterMap();
    return filterMaps != null && filterMaps[alias] != null;
  }

  public static MapJoinTableContainerSerDe createMapJoinTableContainerSerDe(MapJoinDesc mapJoinDesc)
      throws SerDeException {

    final Byte smallTablePos = 1;

    TableDesc keyTableDesc = mapJoinDesc.getKeyTblDesc();
    AbstractSerDe keySerializer = (AbstractSerDe) ReflectionUtil.newInstance(
        BinarySortableSerDe.class, null);
    keySerializer.initialize(null, keyTableDesc.getProperties(), null);
    MapJoinObjectSerDeContext keyContext = new MapJoinObjectSerDeContext(keySerializer, false);

    final List<TableDesc> valueTableDescList;
    if (mapJoinDesc.getNoOuterJoin()) {
      valueTableDescList = mapJoinDesc.getValueTblDescs();
    } else {
      valueTableDescList = mapJoinDesc.getValueFilteredTblDescs();
    }
    TableDesc valueTableDesc = valueTableDescList.get(smallTablePos);
    AbstractSerDe valueSerDe = (AbstractSerDe) ReflectionUtil.newInstance(
        valueTableDesc.getSerDeClass(), null);
    valueSerDe.initialize(null, valueTableDesc.getProperties(), null);
    MapJoinObjectSerDeContext valueContext =
        new MapJoinObjectSerDeContext(valueSerDe, hasFilter(mapJoinDesc, smallTablePos));
    MapJoinTableContainerSerDe mapJoinTableContainerSerDe =
        new MapJoinTableContainerSerDe(keyContext, valueContext);
    return mapJoinTableContainerSerDe;
  }

  public static void connectOperators(
      Operator<? extends OperatorDesc> operator,
      Operator<? extends OperatorDesc> childOperator) throws HiveException {

    List<Operator<? extends OperatorDesc>> newParentOperators = newOperatorList();
    newParentOperators.addAll(childOperator.getParentOperators());
    newParentOperators.add(operator);
    childOperator.setParentOperators(newParentOperators);

    List<Operator<? extends OperatorDesc>> newChildOperators = newOperatorList();
    newChildOperators.addAll(operator.getChildOperators());
    newChildOperators.add(childOperator);
    operator.setChildOperators(newChildOperators);

  }

  private static List<Integer> intArrayToList(int[] intArray) {
    List<Integer> intList = new ArrayList<Integer>(intArray.length);
    for (int i = 0; i < intArray.length; i++) {
      intList.add(intArray[i]);
    }
    return intList;
  }

  private static void loadTableContainerData(MapJoinTestDescription testDesc, MapJoinTestData testData,
      MapJoinTableContainer mapJoinTableContainer )
          throws IOException, SerDeException, HiveException {

    LazyBinarySerializeWrite valueSerializeWrite = null;
    Output valueOutput = null;
    if (testData.smallTableValues != null) {
      valueSerializeWrite = new LazyBinarySerializeWrite(testDesc.smallTableValueTypeInfos.length);
      valueOutput = new Output();
    }
    BytesWritable valueBytesWritable = new BytesWritable();

    BytesWritable keyBytesWritable = new BytesWritable();
    BinarySortableSerializeWrite keySerializeWrite =
        new BinarySortableSerializeWrite(testDesc.bigTableKeyTypeInfos.length);
    Output keyOutput = new Output();
    int round = 0;
    boolean atLeastOneValueAdded = false;
    while (true) {
      for (Entry<RowTestObjects, Integer> testRowEntry : testData.smallTableKeyHashMap.entrySet()) {
        final int smallTableKeyIndex = testRowEntry.getValue();
        final int valueCount = testData.smallTableValueCounts.get(smallTableKeyIndex);
        boolean addEntry = round + 1 <= valueCount;

        if (addEntry) {
          atLeastOneValueAdded = true;

          RowTestObjects valueRow = null;
          if (testData.smallTableValues != null) {
            ArrayList<RowTestObjects> valueList = testData.smallTableValues.get(smallTableKeyIndex);
            valueRow = valueList.get(round);
          }

          Object[] smallTableKey = testRowEntry.getKey().getRow();
          keyOutput.reset();
          keySerializeWrite.set(keyOutput);

          for (int index = 0; index < testDesc.bigTableKeyTypeInfos.length; index++) {

            Writable keyWritable = (Writable) smallTableKey[index];

            VerifyFastRow.serializeWrite(
                keySerializeWrite, (PrimitiveTypeInfo) testDesc.bigTableKeyTypeInfos[index], keyWritable);
          }

          keyBytesWritable.set(keyOutput.getData(), 0, keyOutput.getLength());

          if (valueRow == null) {
            // Empty value.
            mapJoinTableContainer.putRow(keyBytesWritable, valueBytesWritable);
          } else {
            Object[] smallTableValue = valueRow.getRow();
            valueOutput.reset();
            valueSerializeWrite.set(valueOutput);
            for (int index = 0; index < testDesc.smallTableValueTypeInfos.length; index++) {

              Writable valueWritable = (Writable) smallTableValue[index];

              VerifyFastRow.serializeWrite(
                  valueSerializeWrite, (PrimitiveTypeInfo) testDesc.smallTableValueTypeInfos[index], valueWritable);
            }
            valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());
            mapJoinTableContainer.putRow(keyBytesWritable, valueBytesWritable);
          }
        }
      }
      if (testData.smallTableValues == null || !atLeastOneValueAdded) {
        break;
      }
      round++;
      atLeastOneValueAdded = false;
    }
    mapJoinTableContainer.seal();
  }

  public static class CreateMapJoinResult {
    public final MapJoinOperator mapJoinOperator;
    public final MapJoinTableContainer mapJoinTableContainer;
    public final MapJoinTableContainerSerDe mapJoinTableContainerSerDe;

    public CreateMapJoinResult(
        MapJoinOperator mapJoinOperator,
        MapJoinTableContainer mapJoinTableContainer,
        MapJoinTableContainerSerDe mapJoinTableContainerSerDe) {
      this.mapJoinOperator = mapJoinOperator;
      this.mapJoinTableContainer = mapJoinTableContainer;
      this.mapJoinTableContainerSerDe = mapJoinTableContainerSerDe;
    }
  }
  public static CreateMapJoinResult createMapJoin(
      MapJoinTestDescription testDesc,
      MapJoinTestData testData,
      MapJoinDesc mapJoinDesc, boolean isVectorMapJoin, boolean isOriginalMapJoin,
      MapJoinTableContainer shareMapJoinTableContainer)
          throws SerDeException, IOException, HiveException {

    final Byte bigTablePos = 0;

    MapJoinTableContainerSerDe mapJoinTableContainerSerDe =
        MapJoinTestConfig.createMapJoinTableContainerSerDe(mapJoinDesc);

    MapJoinObjectSerDeContext valCtx = mapJoinTableContainerSerDe.getValueContext();

    MapJoinTableContainer mapJoinTableContainer =
        (isOriginalMapJoin ?
            new HashMapWrapper(
                testDesc.hiveConf, -1) :
            new MapJoinBytesTableContainer(
                testDesc.hiveConf, valCtx, testData.smallTableKeyHashMap.size(), 0));

    mapJoinTableContainer.setSerde(
        mapJoinTableContainerSerDe.getKeyContext(),
        mapJoinTableContainerSerDe.getValueContext());

    loadTableContainerData(testDesc, testData, mapJoinTableContainer);

    MapJoinOperator operator;
    if (!isVectorMapJoin) {
      operator = new MapJoinOperator(new CompilationOpContext());
      operator.setConf(mapJoinDesc);
    } else {
      VectorizationContext vContext =
          new VectorizationContext("test", testDesc.bigTableColumnNameList);

      /*
      // UNDONE: Unclear this belonds in the input VectorizationContext...
      // Create scratch columns to hold small table results.
      for (int i = 0; i < testDesc.smallTableValueTypeInfos.length; i++) {
        vContext.allocateScratchColumn(testDesc.smallTableValueTypeInfos[i]);
      }
      */

      // This is what the Vectorizer class does.
      VectorMapJoinDesc vectorMapJoinDesc = new VectorMapJoinDesc();

      byte posBigTable = (byte) mapJoinDesc.getPosBigTable();
      VectorExpression[] allBigTableKeyExpressions =
          vContext.getVectorExpressions(mapJoinDesc.getKeys().get(posBigTable));
      vectorMapJoinDesc.setAllBigTableKeyExpressions(allBigTableKeyExpressions);

      Map<Byte, List<ExprNodeDesc>> exprs = mapJoinDesc.getExprs();
      VectorExpression[] allBigTableValueExpressions =
          vContext.getVectorExpressions(exprs.get(posBigTable));
      vectorMapJoinDesc.setAllBigTableValueExpressions(allBigTableValueExpressions);

      List<ExprNodeDesc> bigTableFilters = mapJoinDesc.getFilters().get(bigTablePos);
      boolean isOuterAndFiltered = (!mapJoinDesc.isNoOuterJoin() && bigTableFilters.size() > 0);
      if (!isOuterAndFiltered) {
        operator = new VectorMapJoinOperator(
            new CompilationOpContext(), mapJoinDesc,
            vContext, vectorMapJoinDesc);
      } else {
        operator = new VectorMapJoinOuterFilteredOperator(
            new CompilationOpContext(), mapJoinDesc,
            vContext, vectorMapJoinDesc);
      }
    }

    HiveConf.setBoolVar(testDesc.hiveConf,
        HiveConf.ConfVars.HIVE_MAPJOIN_TESTING_NO_HASH_TABLE_LOAD, true);

    return new CreateMapJoinResult(operator, mapJoinTableContainer, mapJoinTableContainerSerDe);
  }

  public static CreateMapJoinResult createNativeVectorMapJoin(
      MapJoinTestDescription testDesc,
      MapJoinTestData testData,
      MapJoinDesc mapJoinDesc, HashTableImplementationType hashTableImplementationType,
      MapJoinTableContainer shareMapJoinTableContainer)
          throws SerDeException, IOException, HiveException {

    VectorMapJoinDesc vectorDesc = MapJoinTestConfig.createVectorMapJoinDesc(testDesc);
    mapJoinDesc.setVectorDesc(vectorDesc);

    vectorDesc.setHashTableImplementationType(hashTableImplementationType);

    VectorMapJoinInfo vectorMapJoinInfo = vectorDesc.getVectorMapJoinInfo();

    MapJoinTableContainer mapJoinTableContainer;
    MapJoinTableContainerSerDe mapJoinTableContainerSerDe = null;
    switch (vectorDesc.getHashTableImplementationType()) {
    case OPTIMIZED:
      mapJoinTableContainer =
        new MapJoinBytesTableContainer(
            testDesc.hiveConf, null, testData.smallTableKeyHashMap.size(), 0);

      mapJoinTableContainerSerDe =
          MapJoinTestConfig.createMapJoinTableContainerSerDe(mapJoinDesc);

      mapJoinTableContainer.setSerde(
          mapJoinTableContainerSerDe.getKeyContext(),
          mapJoinTableContainerSerDe.getValueContext());
      break;
    case FAST:
      mapJoinTableContainer =
          new VectorMapJoinFastTableContainer(
              mapJoinDesc, testDesc.hiveConf, testData.smallTableKeyHashMap.size(), 1);
      break;
    default:
      throw new RuntimeException("Unexpected hash table implementation type " + vectorDesc.getHashTableImplementationType());
    }

//    if (shareMapJoinTableContainer == null) {
      loadTableContainerData(testDesc, testData, mapJoinTableContainer);
//    } else {
//      setTableContainerData(mapJoinTableContainer, shareMapJoinTableContainer);
//    }

    VectorizationContext vContext = MapJoinTestConfig.createVectorizationContext(testDesc);

    byte posBigTable = (byte) mapJoinDesc.getPosBigTable();
    VectorExpression[] slimmedBigTableKeyExpressions =
        vContext.getVectorExpressions(mapJoinDesc.getKeys().get(posBigTable));
    vectorMapJoinInfo.setSlimmedBigTableKeyExpressions(slimmedBigTableKeyExpressions);

    Map<Byte, List<ExprNodeDesc>> exprs = mapJoinDesc.getExprs();
    VectorExpression[] slimmedBigTableValueExpressions =
        vContext.getVectorExpressions(exprs.get(posBigTable));
    vectorMapJoinInfo.setSlimmedBigTableValueExpressions(slimmedBigTableValueExpressions);

    VectorMapJoinCommonOperator operator =
        MapJoinTestConfig.createNativeVectorMapJoinOperator(
            testDesc.vectorMapJoinVariation,
            mapJoinDesc,
            vectorDesc,
            vContext);

    HiveConf.setBoolVar(testDesc.hiveConf,
        HiveConf.ConfVars.HIVE_MAPJOIN_TESTING_NO_HASH_TABLE_LOAD, true);

    return new CreateMapJoinResult(operator, mapJoinTableContainer, mapJoinTableContainerSerDe);
  }

  public static CreateMapJoinResult createMapJoinImplementation(
      MapJoinTestImplementation mapJoinImplementation,
      MapJoinTestDescription testDesc,
      MapJoinTestData testData,
      MapJoinDesc mapJoinDesc)
          throws SerDeException, IOException, HiveException {
    return createMapJoinImplementation(
        mapJoinImplementation, testDesc, testData, mapJoinDesc, null);
  }

  public static CreateMapJoinResult createMapJoinImplementation(
      MapJoinTestImplementation mapJoinImplementation,
      MapJoinTestDescription testDesc,
      MapJoinTestData testData,
      MapJoinDesc mapJoinDesc,
      MapJoinTableContainer shareMapJoinTableContainer)
          throws SerDeException, IOException, HiveException {

    CreateMapJoinResult result;
    switch (mapJoinImplementation) {
    case ROW_MODE_HASH_MAP:

      // MapJoinOperator
      result = MapJoinTestConfig.createMapJoin(
          testDesc, testData, mapJoinDesc, /* isVectorMapJoin */ false,
          /* isOriginalMapJoin */ true,
          shareMapJoinTableContainer);
      break;

    case ROW_MODE_OPTIMIZED:

      // MapJoinOperator
      result = MapJoinTestConfig.createMapJoin(
          testDesc, testData, mapJoinDesc, /* isVectorMapJoin */ false,
          /* isOriginalMapJoin */ false,
          shareMapJoinTableContainer);
      break;

    case VECTOR_PASS_THROUGH:

      // VectorMapJoinOperator
      result = MapJoinTestConfig.createMapJoin(
          testDesc, testData, mapJoinDesc, /* isVectorMapJoin */ true,
          /* n/a */ false,
          shareMapJoinTableContainer);
      break;

    case NATIVE_VECTOR_OPTIMIZED:
      result = MapJoinTestConfig.createNativeVectorMapJoin(
          testDesc, testData, mapJoinDesc,
          HashTableImplementationType.OPTIMIZED,
          shareMapJoinTableContainer);
      break;

    case NATIVE_VECTOR_FAST:
      result = MapJoinTestConfig.createNativeVectorMapJoin(
          testDesc, testData, mapJoinDesc,
          HashTableImplementationType.FAST,
          shareMapJoinTableContainer);
      break;
    default:
      throw new RuntimeException("Unexpected MapJoin Operator Implementation " + mapJoinImplementation);
    }
    return result;
  }

  private static Operator<SelectDesc> makeInterceptSelectOperator(
      MapJoinOperator mapJoinOperator, int bigTableKeySize, int bigTableRetainSize,
      String[] outputColumnNames, TypeInfo[] outputTypeInfos) {

    MapJoinDesc mapJoinDesc = (MapJoinDesc) mapJoinOperator.getConf();

    List<ExprNodeDesc> selectExprList = new ArrayList<ExprNodeDesc>();
    List<String> selectOutputColumnNameList = new ArrayList<String>();
    for (int i = 0; i < bigTableRetainSize; i++) {
      String selectOutputColumnName = HiveConf.getColumnInternalName(i);
      selectOutputColumnNameList.add(selectOutputColumnName);

      TypeInfo outputTypeInfo = outputTypeInfos[i];
      if (i < bigTableKeySize) {

        // Big Table key.
        ExprNodeColumnDesc keyColumnExpr =
            new ExprNodeColumnDesc(
                outputTypeInfo,
                outputColumnNames[i], "test", false);
        selectExprList.add(keyColumnExpr);
      } else {

        // For row-mode, substitute NULL constant for any non-key extra Big Table columns.
        ExprNodeConstantDesc nullExtraColumnExpr =
            new ExprNodeConstantDesc(
                outputTypeInfo,
                null);
        nullExtraColumnExpr.setFoldedFromCol(outputColumnNames[i]);
        selectExprList.add(nullExtraColumnExpr);
      }
    }

    SelectDesc selectDesc = new SelectDesc(selectExprList, selectOutputColumnNameList);
    Operator<SelectDesc> selectOperator =
        OperatorFactory.get(new CompilationOpContext(), selectDesc);

    return selectOperator;
  }

  private static Operator<SelectDesc> vectorizeInterceptSelectOperator(
      MapJoinOperator mapJoinOperator, int bigTableKeySize, int bigTableRetainSize,
      Operator<SelectDesc> selectOperator) throws HiveException{

    MapJoinDesc mapJoinDesc = (MapJoinDesc) mapJoinOperator.getConf();

    VectorizationContext vOutContext =
        ((VectorizationContextRegion) mapJoinOperator).getOutputVectorizationContext();

    SelectDesc selectDesc = (SelectDesc) selectOperator.getConf();
    List<ExprNodeDesc> selectExprs = selectDesc.getColList();

    VectorExpression[] selectVectorExpr = new VectorExpression[bigTableRetainSize];
    for (int i = 0; i < bigTableRetainSize; i++) {

      TypeInfo typeInfo = selectExprs.get(i).getTypeInfo();
      if (i < bigTableKeySize) {

        // Big Table key.
        selectVectorExpr[i] = vOutContext.getVectorExpression(selectExprs.get(i));
      } else {

        // For vector-mode, for test purposes we substitute a NO-OP (we don't want to modify
        // the batch).

        // FULL OUTER INTERCEPT does not look at non-key columns.

        NoOpExpression noOpExpression = new NoOpExpression(i);

        noOpExpression.setInputTypeInfos(typeInfo);
        noOpExpression.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.NONE);

        noOpExpression.setOutputTypeInfo(typeInfo);
        noOpExpression.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

        selectVectorExpr[i] = noOpExpression;
      }
    }

    System.out.println("*BENCHMARK* VectorSelectOperator selectVectorExpr " +
        Arrays.toString(selectVectorExpr));

    int[] projectedColumns =
        ArrayUtils.toPrimitive(
            vOutContext.getProjectedColumns().subList(0, bigTableRetainSize).
                toArray(new Integer[0]));
    System.out.println("*BENCHMARK* VectorSelectOperator projectedColumns " +
        Arrays.toString(projectedColumns));

    VectorSelectDesc vectorSelectDesc = new VectorSelectDesc();
    vectorSelectDesc.setSelectExpressions(selectVectorExpr);
    vectorSelectDesc.setProjectedOutputColumns(projectedColumns);

    Operator<SelectDesc> vectorSelectOperator = OperatorFactory.getVectorOperator(
        selectOperator.getCompilationOpContext(), selectDesc,
        vOutContext, vectorSelectDesc);

    return vectorSelectOperator;
  }

  public static CountCollectorTestOperator addFullOuterIntercept(
      MapJoinTestImplementation mapJoinImplementation,
      MapJoinTestDescription testDesc,
      RowTestObjectsMultiSet outputTestRowMultiSet, MapJoinTestData testData,
      MapJoinOperator mapJoinOperator, MapJoinTableContainer mapJoinTableContainer,
      MapJoinTableContainerSerDe mapJoinTableContainerSerDe)
          throws SerDeException, IOException, HiveException {

    MapJoinDesc mapJoinDesc = (MapJoinDesc) mapJoinOperator.getConf();

    // For FULL OUTER MapJoin, we require all Big Keys to be present in the output result.
    // The first N output columns are the Big Table key columns.
    Map<Byte, List<ExprNodeDesc>> keyMap = mapJoinDesc.getKeys();
    List<ExprNodeDesc> bigTableKeyExprs = keyMap.get((byte) 0);
    final int bigTableKeySize = bigTableKeyExprs.size();

    Map<Byte, List<Integer>> retainMap = mapJoinDesc.getRetainList();
    List<Integer> bigTableRetainList = retainMap.get((byte) 0);
    final int bigTableRetainSize = bigTableRetainList.size();

    List<String> outputColumnNameList = mapJoinDesc.getOutputColumnNames();
    String[] mapJoinOutputColumnNames = outputColumnNameList.toArray(new String[0]);

    // Use a utility method to get the MapJoin output TypeInfo.
    TypeInfo[] mapJoinOutputTypeInfos = VectorMapJoinBaseOperator.getOutputTypeInfos(mapJoinDesc);

    final boolean isVectorOutput = MapJoinTestConfig.isVectorOutput(mapJoinImplementation);

    /*
     * Always create a row-mode SelectOperator.  If we are vector-mode, next we will use its
     * expressions and replace it with a VectorSelectOperator.
     */
    Operator<SelectDesc> selectOperator =
        makeInterceptSelectOperator(
            mapJoinOperator, bigTableKeySize, bigTableRetainSize,
            mapJoinOutputColumnNames, mapJoinOutputTypeInfos);

    List<String> selectOutputColumnNameList =
        ((SelectDesc) selectOperator.getConf()).getOutputColumnNames();
    String[] selectOutputColumnNames =
        selectOutputColumnNameList.toArray(new String[0]);

    if (isVectorOutput) {
      selectOperator =
          vectorizeInterceptSelectOperator(
              mapJoinOperator, bigTableKeySize, bigTableRetainSize, selectOperator);
    }

    /*
     * Create test description just for FULL OUTER INTERCEPT with different
     */
    MapJoinTestDescription interceptTestDesc =
        new MapJoinTestDescription(
            testDesc.hiveConf, testDesc.vectorMapJoinVariation,
            selectOutputColumnNames,
            Arrays.copyOf(mapJoinOutputTypeInfos, bigTableRetainSize),
            testDesc.bigTableKeyColumnNums,
            testDesc.smallTableValueTypeInfos,
            testDesc.smallTableRetainKeyColumnNums,
            testDesc.smallTableGenerationParameters,
            testDesc.mapJoinPlanVariation);

    MapJoinDesc intersectMapJoinDesc =
        createMapJoinDesc(interceptTestDesc, /* isFullOuterIntersect */ true);

    /*
     * Create FULL OUTER INTERSECT MapJoin operator.
     */
    CreateMapJoinResult interceptCreateMapJoinResult =
        createMapJoinImplementation(
            mapJoinImplementation, interceptTestDesc, testData, intersectMapJoinDesc);
    MapJoinOperator intersectMapJoinOperator =
        interceptCreateMapJoinResult.mapJoinOperator;
    MapJoinTableContainer intersectMapJoinTableContainer =
        interceptCreateMapJoinResult.mapJoinTableContainer;
    MapJoinTableContainerSerDe interceptMapJoinTableContainerSerDe =
        interceptCreateMapJoinResult.mapJoinTableContainerSerDe;

    connectOperators(mapJoinOperator, selectOperator);

    connectOperators(selectOperator, intersectMapJoinOperator);

    CountCollectorTestOperator interceptTestCollectorOperator;
    if (!isVectorOutput) {
      interceptTestCollectorOperator =
          new TestMultiSetCollectorOperator(
              interceptTestDesc.outputObjectInspectors, outputTestRowMultiSet);
    } else {
      VectorizationContext vContext =
          ((VectorizationContextRegion) intersectMapJoinOperator).getOutputVectorizationContext();
      int[] intersectProjectionColumns =
          ArrayUtils.toPrimitive(vContext.getProjectedColumns().toArray(new Integer[0]));
      interceptTestCollectorOperator =
          new TestMultiSetVectorCollectorOperator(
              intersectProjectionColumns,
              interceptTestDesc.outputTypeInfos,
              interceptTestDesc.outputObjectInspectors, outputTestRowMultiSet);
    }

    connectOperators(intersectMapJoinOperator, interceptTestCollectorOperator);

    // Setup the FULL OUTER INTERSECT MapJoin's inputObjInspector to include the Small Table, etc.
    intersectMapJoinOperator.setInputObjInspectors(interceptTestDesc.inputObjectInspectors);

    // Now, invoke initializeOp methods from the root MapJoin operator.
    mapJoinOperator.initialize(testDesc.hiveConf, testDesc.inputObjectInspectors);

    // Fixup the mapJoinTables container references to our test data.
    mapJoinOperator.setTestMapJoinTableContainer(
        1, mapJoinTableContainer, mapJoinTableContainerSerDe);
    intersectMapJoinOperator.setTestMapJoinTableContainer(
        1, intersectMapJoinTableContainer, interceptMapJoinTableContainerSerDe);

    return interceptTestCollectorOperator;
  }

  private static List<Operator<? extends OperatorDesc>> newOperatorList() {
    return new ArrayList<Operator<? extends OperatorDesc>>();
  }
}
