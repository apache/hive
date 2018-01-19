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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowCollectorTestOperatorBase;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOuterFilteredOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VerifyFastRow;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
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

  public static MapJoinDesc createMapJoinDesc(MapJoinTestDescription testDesc) {

    MapJoinDesc mapJoinDesc = new MapJoinDesc();
    mapJoinDesc.setPosBigTable(0);
    List<ExprNodeDesc> keyExpr = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < testDesc.bigTableKeyColumnNums.length; i++) {
      keyExpr.add(new ExprNodeColumnDesc(testDesc.bigTableKeyTypeInfos[i], testDesc.bigTableKeyColumnNames[i], "B", false));
    }

    Map<Byte, List<ExprNodeDesc>> keyMap = new HashMap<Byte, List<ExprNodeDesc>>();
    keyMap.put((byte)0, keyExpr);

    List<ExprNodeDesc> smallTableExpr = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < testDesc.smallTableValueColumnNames.length; i++) {
      smallTableExpr.add(new ExprNodeColumnDesc(testDesc.smallTableValueTypeInfos[i], testDesc.smallTableValueColumnNames[i], "S", false));
    }
    keyMap.put((byte)1, smallTableExpr);

    mapJoinDesc.setKeys(keyMap);
    mapJoinDesc.setExprs(keyMap);

    Byte[] order = new Byte[] {(byte) 0, (byte) 1};
    mapJoinDesc.setTagOrder(order);
    mapJoinDesc.setNoOuterJoin(testDesc.vectorMapJoinVariation != VectorMapJoinVariation.OUTER);

    Map<Byte, List<ExprNodeDesc>> filterMap = new HashMap<Byte, List<ExprNodeDesc>>();
    filterMap.put((byte) 0, new ArrayList<ExprNodeDesc>());  // None.
    mapJoinDesc.setFilters(filterMap);

    List<Integer> bigTableRetainColumnNumsList = intArrayToList(testDesc.bigTableRetainColumnNums);

    // For now, just small table values...
    List<Integer> smallTableRetainColumnNumsList = intArrayToList(testDesc.smallTableRetainValueColumnNums);

    Map<Byte, List<Integer>> retainListMap = new HashMap<Byte, List<Integer>>();
    retainListMap.put((byte) 0, bigTableRetainColumnNumsList);
    retainListMap.put((byte) 1, smallTableRetainColumnNumsList);
    mapJoinDesc.setRetainList(retainListMap);

    int joinDescType;
    switch (testDesc.vectorMapJoinVariation) {
    case INNER:
    case INNER_BIG_ONLY:
      joinDescType = JoinDesc.INNER_JOIN;
      break;
    case LEFT_SEMI:
      joinDescType = JoinDesc.LEFT_SEMI_JOIN;
      break;
    case OUTER:
      joinDescType = JoinDesc.LEFT_OUTER_JOIN;
      break;
    default:
      throw new RuntimeException("unknown operator variation " + testDesc.vectorMapJoinVariation);
    }
    JoinCondDesc[] conds = new JoinCondDesc[1];
    conds[0] = new JoinCondDesc(0, 1, joinDescType);
    mapJoinDesc.setConds(conds);

    TableDesc keyTableDesc = PlanUtils.getMapJoinKeyTableDesc(testDesc.hiveConf, PlanUtils
        .getFieldSchemasFromColumnList(keyExpr, ""));
    mapJoinDesc.setKeyTblDesc(keyTableDesc);

    TableDesc valueTableDesc = PlanUtils.getMapJoinValueTableDesc(
        PlanUtils.getFieldSchemasFromColumnList(smallTableExpr, ""));
    ArrayList<TableDesc> valueTableDescsList = new ArrayList<TableDesc>();
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
      hashTableKind = HashTableKind.HASH_SET;
      break;
    case OUTER:
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
      case BYTE:
      case SHORT:
      case INT:
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

    vectorMapJoinInfo.setBigTableValueColumnMap(new int[0]);
    vectorMapJoinInfo.setBigTableValueColumnNames(new String[0]);
    vectorMapJoinInfo.setBigTableValueTypeInfos(new TypeInfo[0]);
    vectorMapJoinInfo.setSlimmedBigTableValueExpressions(null);

    vectorDesc.setAllBigTableValueExpressions(null);

    VectorColumnSourceMapping projectionMapping =
        new VectorColumnSourceMapping("Projection Mapping");


    VectorColumnOutputMapping bigTableRetainedMapping =
        new VectorColumnOutputMapping("Big Table Retained Mapping");
    for (int i = 0; i < testDesc.bigTableTypeInfos.length; i++) {
      bigTableRetainedMapping.add(i, i, testDesc.bigTableTypeInfos[i]);
      projectionMapping.add(i, i, testDesc.bigTableKeyTypeInfos[i]);
    }

    VectorColumnOutputMapping bigTableOuterKeyMapping =
        new VectorColumnOutputMapping("Big Table Outer Key Mapping");

    // The order of the fields in the LazyBinary small table value must be used, so
    // we use the source ordering flavor for the mapping.
    VectorColumnSourceMapping smallTableMapping =
        new VectorColumnSourceMapping("Small Table Mapping");
    int outputColumn = testDesc.bigTableTypeInfos.length;
    for (int i = 0; i < testDesc.smallTableValueTypeInfos.length; i++) {
      smallTableMapping.add(i, outputColumn, testDesc.smallTableValueTypeInfos[i]);
      projectionMapping.add(outputColumn, outputColumn, testDesc.smallTableValueTypeInfos[i]);
      outputColumn++;
    }

    // Convert dynamic arrays and maps to simple arrays.

    bigTableRetainedMapping.finalize();

    bigTableOuterKeyMapping.finalize();

    smallTableMapping.finalize();

    vectorMapJoinInfo.setBigTableRetainedMapping(bigTableRetainedMapping);
    vectorMapJoinInfo.setBigTableOuterKeyMapping(bigTableOuterKeyMapping);
    vectorMapJoinInfo.setSmallTableMapping(smallTableMapping);

    projectionMapping.finalize();

    // Verify we added an entry for each output.
    assert projectionMapping.isSourceSequenceGood();

    vectorMapJoinInfo.setProjectionMapping(projectionMapping);

    assert projectionMapping.getCount() == testDesc.outputColumnNames.length;

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
      default:
        throw new RuntimeException("unknown operator variation " + VectorMapJoinVariation);
      }
      break;
    default:
      throw new RuntimeException("Unknown hash table key type " + vectorDesc.getHashTableKeyType());
    }
    return operator;
  }

  public static VectorizationContext createVectorizationContext(MapJoinTestDescription testDesc)
      throws HiveException {
    VectorizationContext vContext =
        new VectorizationContext("test", testDesc.bigTableColumnNamesList);

    // Create scratch columns to hold small table results.
    for (int i = 0; i < testDesc.smallTableValueTypeInfos.length; i++) {
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

    // UNDONE: Why do we need to specify BinarySortableSerDe explicitly here???
    TableDesc keyTableDesc = mapJoinDesc.getKeyTblDesc();
    AbstractSerDe keySerializer = (AbstractSerDe) ReflectionUtil.newInstance(
        BinarySortableSerDe.class, null);
    SerDeUtils.initializeSerDe(keySerializer, null, keyTableDesc.getProperties(), null);
    MapJoinObjectSerDeContext keyContext = new MapJoinObjectSerDeContext(keySerializer, false);

    TableDesc valueTableDesc;
    if (mapJoinDesc.getNoOuterJoin()) {
      valueTableDesc = mapJoinDesc.getValueTblDescs().get(smallTablePos);
    } else {
      valueTableDesc = mapJoinDesc.getValueFilteredTblDescs().get(smallTablePos);
    }
    AbstractSerDe valueSerDe = (AbstractSerDe) ReflectionUtil.newInstance(
        valueTableDesc.getDeserializerClass(), null);
    SerDeUtils.initializeSerDe(valueSerDe, null, valueTableDesc.getProperties(), null);
    MapJoinObjectSerDeContext valueContext =
        new MapJoinObjectSerDeContext(valueSerDe, hasFilter(mapJoinDesc, smallTablePos));
    MapJoinTableContainerSerDe mapJoinTableContainerSerDe =
        new MapJoinTableContainerSerDe(keyContext, valueContext);
    return mapJoinTableContainerSerDe;
  }

  public static void connectOperators(
      MapJoinTestDescription testDesc,
      Operator<? extends OperatorDesc> operator,
      Operator<? extends OperatorDesc> testCollectorOperator) throws HiveException {
    Operator<? extends OperatorDesc>[] parents = new Operator[] {operator};
    testCollectorOperator.setParentOperators(Arrays.asList(parents));
    Operator<? extends OperatorDesc>[] childOperators = new Operator[] {testCollectorOperator};
    operator.setChildOperators(Arrays.asList(childOperators));
    HiveConf.setBoolVar(testDesc.hiveConf,
        HiveConf.ConfVars.HIVE_MAPJOIN_TESTING_NO_HASH_TABLE_LOAD, true);
    operator.initialize(testDesc.hiveConf, testDesc.inputObjectInspectors);
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

  public static MapJoinOperator createMapJoin(MapJoinTestDescription testDesc,
      Operator<? extends OperatorDesc> collectorOperator, MapJoinTestData testData,
      MapJoinDesc mapJoinDesc, boolean isVectorMapJoin, boolean isOriginalMapJoin)
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
      VectorizationContext vContext = new VectorizationContext("test", testDesc.bigTableColumnNamesList);
      // Create scratch columns to hold small table results.
      for (int i = 0; i < testDesc.smallTableValueTypeInfos.length; i++) {
        vContext.allocateScratchColumn(testDesc.smallTableValueTypeInfos[i]);
      }

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

    MapJoinTestConfig.connectOperators(testDesc, operator, collectorOperator);

    operator.setTestMapJoinTableContainer(1, mapJoinTableContainer, mapJoinTableContainerSerDe);

    return operator;
  }

  public static MapJoinOperator createNativeVectorMapJoin(MapJoinTestDescription testDesc,
      Operator<? extends OperatorDesc> collectorOperator, MapJoinTestData testData,
      MapJoinDesc mapJoinDesc, HashTableImplementationType hashTableImplementationType)
          throws SerDeException, IOException, HiveException {

    VectorMapJoinDesc vectorDesc = MapJoinTestConfig.createVectorMapJoinDesc(testDesc);

    // UNDONE
    mapJoinDesc.setVectorDesc(vectorDesc);

    vectorDesc.setHashTableImplementationType(hashTableImplementationType);

    VectorMapJoinInfo vectorMapJoinInfo = vectorDesc.getVectorMapJoinInfo();

    MapJoinTableContainer mapJoinTableContainer;
    switch (vectorDesc.getHashTableImplementationType()) {
    case OPTIMIZED:
      mapJoinTableContainer =
        new MapJoinBytesTableContainer(
            testDesc.hiveConf, null, testData.smallTableKeyHashMap.size(), 0);

      MapJoinTableContainerSerDe mapJoinTableContainerSerDe =
          MapJoinTestConfig.createMapJoinTableContainerSerDe(mapJoinDesc);

      mapJoinTableContainer.setSerde(
          mapJoinTableContainerSerDe.getKeyContext(),
          mapJoinTableContainerSerDe.getValueContext());
      break;
    case FAST:
      mapJoinTableContainer =
          new VectorMapJoinFastTableContainer(
              mapJoinDesc, testDesc.hiveConf, testData.smallTableKeyHashMap.size());
      break;
    default:
      throw new RuntimeException("Unexpected hash table implementation type " + vectorDesc.getHashTableImplementationType());
    }

    loadTableContainerData(testDesc, testData, mapJoinTableContainer);

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

    MapJoinTestConfig.connectOperators(testDesc, operator, collectorOperator);

    operator.setTestMapJoinTableContainer(1, mapJoinTableContainer, null);

    return operator;
  }

  public static MapJoinOperator createMapJoinImplementation(MapJoinTestImplementation mapJoinImplementation,
      MapJoinTestDescription testDesc,
      Operator<? extends OperatorDesc> testCollectorOperator, MapJoinTestData testData,
      MapJoinDesc mapJoinDesc) throws SerDeException, IOException, HiveException {

    MapJoinOperator operator;
    switch (mapJoinImplementation) {
    case ROW_MODE_HASH_MAP:

      // MapJoinOperator
      operator = MapJoinTestConfig.createMapJoin(
          testDesc, testCollectorOperator, testData, mapJoinDesc, /* isVectorMapJoin */ false,
          /* isOriginalMapJoin */ true);
      break;

    case ROW_MODE_OPTIMIZED:

      // MapJoinOperator
      operator = MapJoinTestConfig.createMapJoin(
          testDesc, testCollectorOperator, testData, mapJoinDesc, /* isVectorMapJoin */ false,
          /* isOriginalMapJoin */ false);
      break;

    case VECTOR_PASS_THROUGH:

      // VectorMapJoinOperator
      operator = MapJoinTestConfig.createMapJoin(
          testDesc, testCollectorOperator, testData, mapJoinDesc, /* isVectorMapJoin */ true,
          /* n/a */ false);
      break;

    case NATIVE_VECTOR_OPTIMIZED:
      operator = MapJoinTestConfig.createNativeVectorMapJoin(
          testDesc, testCollectorOperator, testData, mapJoinDesc, HashTableImplementationType.OPTIMIZED);
      break;

    case NATIVE_VECTOR_FAST:
      operator = MapJoinTestConfig.createNativeVectorMapJoin(
          testDesc, testCollectorOperator, testData, mapJoinDesc, HashTableImplementationType.FAST);
      break;
    default:
      throw new RuntimeException("Unexpected MapJoin Operator Implementation " + mapJoinImplementation);
    }
    return operator;
  }
}