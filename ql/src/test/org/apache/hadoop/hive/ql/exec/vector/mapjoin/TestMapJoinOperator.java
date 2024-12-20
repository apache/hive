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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountVectorCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowCollectorTestOperatorBase;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjectsMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorBatchDebug;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOuterFilteredOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType.GenerateCategory;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.CreateMapJoinResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.TestMultiSetCollectorOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.TestMultiSetVectorCollectorOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.MapJoinPlanVariation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters.ValueOption;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastMultiKeyHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VerifyFastRow;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HashCodeUtil;
import org.apache.hive.common.util.ReflectionUtil;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;

public class TestMapJoinOperator {

  private boolean addLongHiveConfVariation(int hiveConfVariation, HiveConf hiveConf) {

    // Set defaults.
    HiveConf.setBoolVar(
        hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_MINMAX_ENABLED, false);
    HiveConf.setIntVar(
        hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_OVERFLOW_REPEATED_THRESHOLD, -1);

    switch (hiveConfVariation) {
    case 0:
      break;
    case 1:
      HiveConf.setBoolVar(
          hiveConf,
          HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_MINMAX_ENABLED, true);
      break;
    case 2:
      // Force generateHashMapResultLargeMultiValue to be used.
      HiveConf.setIntVar(
          hiveConf,
          HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_OVERFLOW_REPEATED_THRESHOLD, 5);
      break;
    default:
      return false;
    }
    return true;
  }

  private boolean goodTestVariation(MapJoinTestDescription testDesc) {
    final int smallTableValueSize = testDesc.smallTableRetainValueColumnNums.length;

    switch (testDesc.vectorMapJoinVariation) {
    case INNER:
      return (smallTableValueSize > 0);
    case INNER_BIG_ONLY:
    case LEFT_SEMI:
    case LEFT_ANTI:
      return (smallTableValueSize == 0);
    case OUTER:
      return true;
    case FULL_OUTER:
      return true;
    default:
      throw new RuntimeException(
          "Unexpected vectorMapJoinVariation " + testDesc.vectorMapJoinVariation);
    }

  }

  @Ignore
  @Test
  public void testLong0() throws Exception {
    long seed = 234882L;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
          doTestLong0(
              seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
              MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  private boolean doTestLong0(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: long key, no value; Small Table: no key retained, date value
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.longTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {};

    smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.dateTypeInfo};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testLong0");

    return false;
  }

  @Ignore
  @Test
  public void testLong0_NoRegularKeys() throws Exception {
    long seed = 234882L;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
          doTestLong0_NoRegularKeys(
              seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
              MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  private boolean doTestLong0_NoRegularKeys(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();
    smallTableGenerationParameters.setValueOption(ValueOption.NO_REGULAR_SMALL_KEYS);

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: long key, no value; Small Table: no key retained, date value
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.longTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {};

    smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.dateTypeInfo};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "doTestLong0_NoRegularKeys");

    return false;
  }

  @Ignore
  @Test
  public void testLong1() throws Exception {
    long seed = 234882L;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestLong1(
                seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestLong1(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: int key, long value; Small Table: no key retained, string value
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.longTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {};

    smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.stringTypeInfo};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testLong1");

    return false;
  }

  @Test
  public void testLong2() throws Exception {
    long seed = 3553;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestLong2(
                seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestLong2(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: short key, no value; Small Table: key retained, timestamp value
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.shortTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.timestampTypeInfo};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testLong2");

    return false;
  }


  @Test
  public void testLong3() throws Exception {
    long seed = 9934;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestLong3(
                seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestLong3(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: int key, string value; Small Table: key retained, decimal value
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.stringTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos =
        new TypeInfo[] {
            new DecimalTypeInfo(38, 18)};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testLong3");

    return false;
  }

  @Test
  public void testLong3_NoRegularKeys() throws Exception {
    long seed = 9934;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestLong3_NoRegularKeys(
                seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestLong3_NoRegularKeys(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();
    smallTableGenerationParameters.setValueOption(ValueOption.NO_REGULAR_SMALL_KEYS);

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: int key, string value; Small Table: key retained, decimal value
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.stringTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos =
        new TypeInfo[] {
            new DecimalTypeInfo(38, 18)};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "doTestLong3_NoRegularKeys");

    return false;
  }

  @Test
  public void testLong4() throws Exception {
    long seed = 3982;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestLong4(
                seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestLong4(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: int key, no value; Small Table: no key retained, no value
    // (exercise INNER_BIGONLY, LEFT_SEMI)
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.intTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {};

    smallTableValueTypeInfos = new TypeInfo[] {};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testLong4");

    return false;
  }

  @Test
  public void testLong5() throws Exception {
    long seed = 3553;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestLong5(
                seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestLong5(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    // Cause there to be no regular FULL OUTER MapJoin MATCHes so only non-match Small Table
    // results.
    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: long key, no value; Small Table: key retained, no value
    // (exercise INNER_BIGONLY, LEFT_SEMI)
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.longTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos = new TypeInfo[] {};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testLong5");

    return false;
  }

  @Test
  public void testLong6() throws Exception {
    long seed = 9384;
    int rowCount = 10;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestLong6(
                seed, rowCount, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestLong6(long seed, int rowCount, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    HiveConf hiveConf = getHiveConf();

    if (!addLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    // Cause there to be no regular FULL OUTER MapJoin MATCHes so only non-match Small Table
    // results.
    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Big Table: long key, timestamp value; Small Table: key retained, no value
    // (exercise INNER_BIGONLY, LEFT_SEMI)
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.longTypeInfo,
            TypeInfoFactory.timestampTypeInfo};

    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos = new TypeInfo[] {};

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testLong6");

    return false;
  }

  private boolean addNonLongHiveConfVariation(int hiveConfVariation, HiveConf hiveConf) {

    // Set defaults.
    HiveConf.setIntVar(
        hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_OVERFLOW_REPEATED_THRESHOLD, -1);

    switch (hiveConfVariation) {
    case 0:
      break;
    case 1:
      // Force generateHashMapResultLargeMultiValue to be used.
      HiveConf.setIntVar(
         hiveConf,
         HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_OVERFLOW_REPEATED_THRESHOLD, 5);
      break;
    default:
      return false;
    }
    return true;
  }

  @Test
  public void testMultiKey0() throws Exception {
    long seed = 28322;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestMultiKey0(
                seed, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestMultiKey0(long seed, int hiveConfVariation, VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    int rowCount = 10;

    HiveConf hiveConf = getHiveConf();

    if (!addNonLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Three key columns.
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.shortTypeInfo,
            TypeInfoFactory.intTypeInfo};
    bigTableKeyColumnNums = new int[] {0, 1};

    smallTableRetainKeyColumnNums = new int[] {0, 1};

    smallTableValueTypeInfos = new TypeInfo[] {};

    //----------------------------------------------------------------------------------------------

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    // Prepare data.  Good for ANY implementation variation.
    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testMultiKey0");

    return false;
  }

  @Test
  public void testMultiKey1() throws Exception {
    long seed = 87543;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestMultiKey1(
                seed, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestMultiKey1(long seed, int hiveConfVariation, VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    int rowCount = 10;

    HiveConf hiveConf = getHiveConf();

    if (!addNonLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Three key columns.
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.timestampTypeInfo,
            TypeInfoFactory.shortTypeInfo,
            TypeInfoFactory.stringTypeInfo};
    bigTableKeyColumnNums = new int[] {0, 1, 2};

    smallTableRetainKeyColumnNums = new int[] {0, 1, 2};

    smallTableValueTypeInfos =
        new TypeInfo[] {new DecimalTypeInfo(38, 18)};

    //----------------------------------------------------------------------------------------------

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    // Prepare data.  Good for ANY implementation variation.
    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testMultiKey1");

    return false;
  }

  @Test
  public void testMultiKey2() throws Exception {
    long seed = 87543;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestMultiKey2(
                seed, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestMultiKey2(long seed, int hiveConfVariation, VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    int rowCount = 10;

    HiveConf hiveConf = getHiveConf();

    if (!addNonLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Three key columns.
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.longTypeInfo,
            TypeInfoFactory.shortTypeInfo,
            TypeInfoFactory.stringTypeInfo};
    bigTableKeyColumnNums = new int[] {0, 1, 2};

    smallTableRetainKeyColumnNums = new int[] {0, 1, 2};

    smallTableValueTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.stringTypeInfo};

    //----------------------------------------------------------------------------------------------

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    // Prepare data.  Good for ANY implementation variation.
    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testMultiKey0");

    return false;
  }

  @Test
  public void testMultiKey3() throws Exception {
    long seed = 87543;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestMultiKey3(
                seed, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestMultiKey3(long seed, int hiveConfVariation, VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    int rowCount = 10;

    HiveConf hiveConf = getHiveConf();

    if (!addNonLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // Three key columns.
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.dateTypeInfo,
            TypeInfoFactory.byteTypeInfo};
    bigTableKeyColumnNums = new int[] {0, 1};

    smallTableRetainKeyColumnNums = new int[] {0, 1};

    smallTableValueTypeInfos =
        new TypeInfo[] {};

    //----------------------------------------------------------------------------------------------

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    // Prepare data.  Good for ANY implementation variation.
    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testMultiKey3");

    return false;
  }

  @Test
  public void testString0() throws Exception {
    long seed = 87543;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestString0(
                seed, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestString0(long seed, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    int rowCount = 10;

    HiveConf hiveConf = getHiveConf();

    if (!addNonLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // One plain STRING key column.
    bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.stringTypeInfo};
    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.dateTypeInfo, TypeInfoFactory.timestampTypeInfo};

    //----------------------------------------------------------------------------------------------

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    // Prepare data.  Good for ANY implementation variation.
    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testString0");

    return false;
  }

  @Test
  public void testString1() throws Exception {
    long seed = 3422;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestString1(
                seed, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestString1(long seed, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    int rowCount = 10;

    HiveConf hiveConf = getHiveConf();

    if (!addNonLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // One BINARY key column.
    bigTableTypeInfos =
        new TypeInfo[] {
        TypeInfoFactory.binaryTypeInfo};
    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.shortTypeInfo,
            TypeInfoFactory.floatTypeInfo,
            new DecimalTypeInfo(38, 18)};

    smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    //----------------------------------------------------------------------------------------------

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    // Prepare data.  Good for ANY implementation variation.
    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testString1");

    return false;
  }

  @Test
  public void testString2() throws Exception {
    long seed = 7439;

    int hiveConfVariation = 0;
    boolean hiveConfVariationsDone = false;
    do {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        hiveConfVariationsDone =
            doTestString2(
                seed, hiveConfVariation, vectorMapJoinVariation,
                MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
      }
      seed++;
      hiveConfVariation++;
    } while (!hiveConfVariationsDone);
  }

  public boolean doTestString2(long seed, int hiveConfVariation,
      VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinPlanVariation mapJoinPlanVariation) throws Exception {

    int rowCount = 10;

    HiveConf hiveConf = getHiveConf();

    if (!addNonLongHiveConfVariation(hiveConfVariation, hiveConf)) {
      return true;
    }

    TypeInfo[] bigTableTypeInfos = null;

    int[] bigTableKeyColumnNums = null;

    TypeInfo[] smallTableValueTypeInfos = null;

    int[] smallTableRetainKeyColumnNums = null;

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    MapJoinTestDescription testDesc = null;
    MapJoinTestData testData = null;

    // One STRING key column; Small Table value: NONE (tests INNER_BIG_ONLY, LEFT_SEMI).
    bigTableTypeInfos =
        new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo};
    bigTableKeyColumnNums = new int[] {0};

    smallTableRetainKeyColumnNums = new int[] {0};

    smallTableValueTypeInfos = new TypeInfo[] {};

    smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    //----------------------------------------------------------------------------------------------

    testDesc =
        new MapJoinTestDescription(
            hiveConf, vectorMapJoinVariation,
            bigTableTypeInfos,
            bigTableKeyColumnNums,
            smallTableValueTypeInfos,
            smallTableRetainKeyColumnNums,
            smallTableGenerationParameters,
            mapJoinPlanVariation);

    if (!goodTestVariation(testDesc)) {
      return false;
    }

    // Prepare data.  Good for ANY implementation variation.
    testData =
        new MapJoinTestData(rowCount, testDesc, seed);

    executeTest(testDesc, testData, "testString2");

    return false;
  }

  private void addBigTableRetained(MapJoinTestDescription testDesc, Object[] bigTableRowObjects,
      Object[] outputObjects) {
    final int bigTableRetainColumnNumsLength = testDesc.bigTableRetainColumnNums.length;
    for (int o = 0; o < bigTableRetainColumnNumsLength; o++) {
      outputObjects[o] = bigTableRowObjects[testDesc.bigTableRetainColumnNums[o]];
    }
  }

  private void addToOutput(MapJoinTestDescription testDesc,
      RowTestObjectsMultiSet expectedTestRowMultiSet, Object[] outputObjects,
      RowTestObjectsMultiSet.RowFlag rowFlag) {
    for (int c = 0; c < outputObjects.length; c++) {
      PrimitiveObjectInspector primitiveObjInsp =
          ((PrimitiveObjectInspector) testDesc.outputObjectInspectors[c]);
      Object outputObject = outputObjects[c];
      outputObjects[c] = primitiveObjInsp.copyObject(outputObject);
    }
    expectedTestRowMultiSet.add(new RowTestObjects(outputObjects), rowFlag);
  }

  private String rowToCsvString(Object[] rowObjects) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < rowObjects.length; i++) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      Object obj = rowObjects[i];
      if (obj == null) {
        sb.append("\\N");
      } else {
        sb.append(obj);
      }
    }
    return sb.toString();
  }

  /*
   * Simulate the join by driving the test big table data by our test small table HashMap and
   * create the expected output as a multi-set of TestRow (i.e. TestRow and occurrence count).
   */
  private RowTestObjectsMultiSet createExpectedTestRowMultiSet(MapJoinTestDescription testDesc,
      MapJoinTestData testData) throws HiveException {

    RowTestObjectsMultiSet expectedTestRowMultiSet = new RowTestObjectsMultiSet();

    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(testDesc.bigTableTypeInfos);

    final int bigTableColumnCount = testDesc.bigTableTypeInfos.length;
    Object[] bigTableRowObjects = new Object[bigTableColumnCount];

    final int bigTableKeyColumnCount = testDesc.bigTableKeyTypeInfos.length;
    Object[] bigTableKeyObjects = new Object[bigTableKeyColumnCount];

    VectorRandomBatchSource bigTableBatchSource = testData.getBigTableBatchSource();
    VectorizedRowBatch batch = testData.getBigTableBatch();
    bigTableBatchSource.resetBatchIteration();
    while (bigTableBatchSource.fillNextBatch(batch)) {

      final int size = testData.bigTableBatch.size;
      for (int r = 0; r < size; r++) {
        vectorExtractRow.extractRow(testData.bigTableBatch, r, bigTableRowObjects);

        // Form key object array
        boolean hasAnyNulls = false;    // NULLs may be present in {FULL|LEFT|RIGHT} OUTER joins.
        for (int k = 0; k < bigTableKeyColumnCount; k++) {
          int keyColumnNum = testDesc.bigTableKeyColumnNums[k];
          Object keyObject = bigTableRowObjects[keyColumnNum];
          if (keyObject == null) {
            hasAnyNulls = true;
          }
          bigTableKeyObjects[k] = keyObject;
          bigTableKeyObjects[k] = ((PrimitiveObjectInspector) testDesc.bigTableObjectInspectors[keyColumnNum]).copyObject(bigTableKeyObjects[k]);
        }
        RowTestObjects testKey = new RowTestObjects(bigTableKeyObjects);

        if (testData.smallTableKeyHashMap.containsKey(testKey) && !hasAnyNulls) {

          int smallTableKeyIndex = testData.smallTableKeyHashMap.get(testKey);

          switch (testDesc.vectorMapJoinVariation) {
          case INNER:
          case OUTER:
          case FULL_OUTER:
            {
              // One row per value.
              ArrayList<RowTestObjects> valueList = testData.smallTableValues.get(smallTableKeyIndex);
              final int valueCount = valueList.size();
              for (int v = 0; v < valueCount; v++) {
                Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

                addBigTableRetained(testDesc, bigTableRowObjects, outputObjects);

                int outputColumnNum = testDesc.bigTableRetainColumnNums.length;

                final int smallTableRetainKeyColumnNumsLength =
                    testDesc.smallTableRetainKeyColumnNums.length;
                for (int o = 0; o < smallTableRetainKeyColumnNumsLength; o++) {
                  outputObjects[outputColumnNum++] =
                      bigTableKeyObjects[testDesc.smallTableRetainKeyColumnNums[o]];
                }

                Object[] valueRow = valueList.get(v).getRow();
                final int smallTableRetainValueColumnNumsLength =
                    testDesc.smallTableRetainValueColumnNums.length;
                for (int o = 0; o < smallTableRetainValueColumnNumsLength; o++) {
                  outputObjects[outputColumnNum++] =
                      valueRow[testDesc.smallTableRetainValueColumnNums[o]];
                }

                addToOutput(testDesc, expectedTestRowMultiSet, outputObjects,
                    RowTestObjectsMultiSet.RowFlag.REGULAR);
              }
            }
            break;
          case INNER_BIG_ONLY:
          case LEFT_SEMI:
          case LEFT_ANTI:
            {
              Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

              addBigTableRetained(testDesc, bigTableRowObjects, outputObjects);

              int outputColumnNum = testDesc.bigTableRetainColumnNums.length;

              final int smallTableRetainKeyColumnNumsLength =
                  testDesc.smallTableRetainKeyColumnNums.length;
              for (int o = 0; o < smallTableRetainKeyColumnNumsLength; o++) {
                outputObjects[outputColumnNum++] =
                    bigTableKeyObjects[testDesc.smallTableRetainKeyColumnNums[o]];
              }

              addToOutput(testDesc, expectedTestRowMultiSet, outputObjects,
                  RowTestObjectsMultiSet.RowFlag.REGULAR);
            }
            break;
          default:
            throw new RuntimeException("Unknown operator variation " + testDesc.vectorMapJoinVariation);
          }

        } else {

          // Big Table non-match.

          if (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.OUTER ||
              testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER) {

            // We need to add a non-match row with nulls for small table values.

            Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

            addBigTableRetained(testDesc, bigTableRowObjects, outputObjects);

            int outputColumnNum = testDesc.bigTableRetainColumnNums.length;

            final int smallTableRetainKeyColumnNumsLength =
                testDesc.smallTableRetainKeyColumnNums.length;
            for (int o = 0; o < smallTableRetainKeyColumnNumsLength; o++) {
              outputObjects[outputColumnNum++] = null;
            }

            final int smallTableRetainValueColumnNumsLength =
                testDesc.smallTableRetainValueColumnNums.length;
            for (int o = 0; o < smallTableRetainValueColumnNumsLength; o++) {
              outputObjects[outputColumnNum++] = null;
            }

            addToOutput(testDesc, expectedTestRowMultiSet, outputObjects,
                RowTestObjectsMultiSet.RowFlag.LEFT_OUTER);
          }
        }
      }
    }

    if (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER) {

      System.out.println("*BENCHMARK* ----------------------------------------------------------------------");
      System.out.println("*BENCHMARK* FULL OUTER non-match key count " +
          testData.fullOuterAdditionalSmallTableKeys.size());

      // Fill in non-match Small Table key results.
      for (RowTestObjects smallTableKey : testData.fullOuterAdditionalSmallTableKeys) {

        // System.out.println(
        //     "*BENCHMARK* fullOuterAdditionalSmallTableKey " + smallTableKey.toString());

        int smallTableKeyIndex = testData.smallTableKeyHashMap.get(smallTableKey);

        // One row per value.
        ArrayList<RowTestObjects> valueList = testData.smallTableValues.get(smallTableKeyIndex);
        final int valueCount = valueList.size();
        for (int v = 0; v < valueCount; v++) {
          Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

          // Non-match Small Table keys produce NULL Big Table columns.
          final int bigTableRetainColumnNumsLength = testDesc.bigTableRetainColumnNums.length;
          for (int o = 0; o < bigTableRetainColumnNumsLength; o++) {
            outputObjects[o] = null;
          }

          int outputColumnNum = testDesc.bigTableRetainColumnNums.length;

          // The output result may include 0, 1, or more small key columns...
          Object[] smallKeyObjects = smallTableKey.getRow();
          final int smallTableRetainKeyColumnNumsLength =
              testDesc.smallTableRetainKeyColumnNums.length;
          for (int o = 0; o < smallTableRetainKeyColumnNumsLength; o++) {
            outputObjects[outputColumnNum++] =
                smallKeyObjects[testDesc.smallTableRetainKeyColumnNums[o]];
          }

          Object[] valueRow = valueList.get(v).getRow();
          final int smallTableRetainValueColumnNumsLength =
              testDesc.smallTableRetainValueColumnNums.length;
          for (int o = 0; o < smallTableRetainValueColumnNumsLength; o++) {
            outputObjects[outputColumnNum++] =
                valueRow[testDesc.smallTableRetainValueColumnNums[o]];
          }

          addToOutput(testDesc, expectedTestRowMultiSet, outputObjects,
              RowTestObjectsMultiSet.RowFlag.FULL_OUTER);
        }
      }
    }

    return expectedTestRowMultiSet;
  }

  private void generateBigAndSmallTableRowLogLines(MapJoinTestDescription testDesc,
      MapJoinTestData testData) throws HiveException {

    // Generate Big Table rows log lines...
    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(testDesc.bigTableTypeInfos);

    final int bigTableColumnCount = testDesc.bigTableTypeInfos.length;
    Object[] bigTableRowObjects = new Object[bigTableColumnCount];

    /*
    PrintStream big_ps;
    try {
      big_ps = new PrintStream("/Users/mmccline/VecFullOuterRefresh/out_big");
    } catch (Exception e) {
      throw new HiveException(e);
    }
    */

    VectorRandomBatchSource bigTableBatchSource = testData.getBigTableBatchSource();
    VectorizedRowBatch batch = testData.getBigTableBatch();
    bigTableBatchSource.resetBatchIteration();
    while (bigTableBatchSource.fillNextBatch(batch)) {

      final int size = testData.bigTableBatch.size;
      for (int r = 0; r < size; r++) {
        vectorExtractRow.extractRow(testData.bigTableBatch, r, bigTableRowObjects);

        // big_ps.println(rowToCsvString(bigTableRowObjects));
      }
    }
    // big_ps.close();

    /*
    PrintStream small_ps;
    try {
      small_ps = new PrintStream("/Users/mmccline/VecFullOuterRefresh/out_small");
    } catch (Exception e) {
      throw new HiveException(e);
    }
    */

    // Generate Small Table rows log lines...
    final int keyKeyColumnNumsLength =
        testDesc.bigTableKeyColumnNums.length;
    final int smallTableRetainValueLength =
        testDesc.smallTableRetainValueColumnNums.length;
    final int smallTableLength = keyKeyColumnNumsLength + smallTableRetainValueLength;
    for (Entry<RowTestObjects, Integer> entry : testData.smallTableKeyHashMap.entrySet()) {
      if (smallTableRetainValueLength == 0) {
        Object[] smallTableRowObjects = entry.getKey().getRow();
        // small_ps.println(rowToCsvString(smallTableRowObjects));
      } else {
        Integer valueIndex = entry.getValue();
        ArrayList<RowTestObjects> valueList = testData.smallTableValues.get(valueIndex);
        final int valueCount = valueList.size();
        for (int v = 0; v < valueCount; v++) {
          Object[] smallTableRowObjects = new Object[smallTableLength];
          System.arraycopy(entry.getKey().getRow(), 0, smallTableRowObjects, 0, keyKeyColumnNumsLength);
          int outputColumnNum = keyKeyColumnNumsLength;
          Object[] valueRow = valueList.get(v).getRow();
          for (int o = 0; o < smallTableRetainValueLength; o++) {
            smallTableRowObjects[outputColumnNum++] =
                valueRow[testDesc.smallTableRetainValueColumnNums[o]];
          }
          // small_ps.println(rowToCsvString(smallTableRowObjects));
        }
      }
    }
    // small_ps.close();
  }

  private void executeTest(MapJoinTestDescription testDesc, MapJoinTestData testData,
      String title) throws Exception {

    // So stack trace is self-explanatory.
    switch (testDesc.vectorMapJoinVariation) {
    case INNER:
      executeTestInner(testDesc, testData, title);
      break;
    case INNER_BIG_ONLY:
      executeTestInnerBigOnly(testDesc, testData, title);
      break;
    case LEFT_SEMI:
      executeTestLeftSemi(testDesc, testData, title);
      break;
    case OUTER:
      executeTestOuter(testDesc, testData, title);
      break;
    case FULL_OUTER:
      executeTestFullOuter(testDesc, testData, title);
      break;
    case LEFT_ANTI: //TODO
      break;
    default:
      throw new RuntimeException("Unexpected Vector MapJoin variation " +
          testDesc.vectorMapJoinVariation);
    }
  }

  private void executeTestInner(MapJoinTestDescription testDesc, MapJoinTestData testData,
      String title) throws Exception {
    doExecuteTest(testDesc, testData, title);
  }

  private void executeTestInnerBigOnly(MapJoinTestDescription testDesc, MapJoinTestData testData,
      String title) throws Exception {
    doExecuteTest(testDesc, testData, title);
  }

  private void executeTestLeftSemi(MapJoinTestDescription testDesc, MapJoinTestData testData,
      String title) throws Exception {
    doExecuteTest(testDesc, testData, title);
  }

  private void executeTestOuter(MapJoinTestDescription testDesc, MapJoinTestData testData,
      String title) throws Exception {
    doExecuteTest(testDesc, testData, title);
  }

  private void executeTestFullOuter(MapJoinTestDescription testDesc, MapJoinTestData testData,
      String title) throws Exception {
    doExecuteTest(testDesc, testData, title);
  }

  private void doExecuteTest(MapJoinTestDescription testDesc, MapJoinTestData testData,
      String title) throws Exception {

    RowTestObjectsMultiSet expectedTestRowMultiSet =
        createExpectedTestRowMultiSet(testDesc, testData);

    generateBigAndSmallTableRowLogLines(testDesc, testData);

    System.out.println("*BENCHMARK* expectedTestRowMultiSet " +
        " totalKeyCount " + expectedTestRowMultiSet.getTotalKeyCount() +
        " totalValueCount " + expectedTestRowMultiSet.getTotalValueCount());

    // Execute all implementation variations.
    for (MapJoinTestImplementation mapJoinImplementation : MapJoinTestImplementation.values()) {

      if (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER &&
          mapJoinImplementation == MapJoinTestImplementation.ROW_MODE_HASH_MAP) {

        // Key match tracking not supported in plain Java HashMap.
        continue;
      }
      switch (mapJoinImplementation) {
      case ROW_MODE_HASH_MAP:
        executeRowModeHashMap(
           testDesc, testData,
           expectedTestRowMultiSet,
           title);
        break;
      case ROW_MODE_OPTIMIZED:
        executeRowModeOptimized(
            testDesc, testData,
            expectedTestRowMultiSet,
            title);
        break;
      case VECTOR_PASS_THROUGH:
        executeVectorPassThrough(
            testDesc, testData,
            expectedTestRowMultiSet,
            title);
        break;
      case NATIVE_VECTOR_OPTIMIZED:
        executeNativeVectorOptimized(
            testDesc, testData,
            expectedTestRowMultiSet,
            title);
        break;
      case NATIVE_VECTOR_FAST:
        executeNativeVectorFast(
            testDesc, testData,
            expectedTestRowMultiSet,
            title);
        break;
      default:
        throw new RuntimeException(
            "Unexpected vector map join test variation");
      }
    }
  }

  private void executeRowModeHashMap(
      MapJoinTestDescription testDesc, MapJoinTestData testData,
      RowTestObjectsMultiSet expectedTestRowMultiSet,
      String title)
          throws Exception {
    executeTestImplementation(
        MapJoinTestImplementation.ROW_MODE_HASH_MAP,
        testDesc, testData,
        expectedTestRowMultiSet,
        title);
  }

  private void executeRowModeOptimized(
      MapJoinTestDescription testDesc, MapJoinTestData testData,
      RowTestObjectsMultiSet expectedTestRowMultiSet,
      String title)
          throws Exception {
    executeTestImplementation(
        MapJoinTestImplementation.ROW_MODE_OPTIMIZED,
        testDesc, testData,
        expectedTestRowMultiSet,
        title);
  }

  private void executeVectorPassThrough(
      MapJoinTestDescription testDesc, MapJoinTestData testData,
      RowTestObjectsMultiSet expectedTestRowMultiSet,
      String title)
          throws Exception {
    executeTestImplementation(
        MapJoinTestImplementation.VECTOR_PASS_THROUGH,
        testDesc, testData,
        expectedTestRowMultiSet,
        title);
  }

  private void executeNativeVectorOptimized(
      MapJoinTestDescription testDesc, MapJoinTestData testData,
      RowTestObjectsMultiSet expectedTestRowMultiSet,
      String title)
          throws Exception {
    executeTestImplementation(
        MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED,
        testDesc, testData,
        expectedTestRowMultiSet,
        title);
  }

  private void executeNativeVectorFast(
      MapJoinTestDescription testDesc, MapJoinTestData testData,
      RowTestObjectsMultiSet expectedTestRowMultiSet,
      String title)
          throws Exception {
    executeTestImplementation(
        MapJoinTestImplementation.NATIVE_VECTOR_FAST,
        testDesc, testData,
        expectedTestRowMultiSet,
        title);
  }

  private void executeTestImplementation(
      MapJoinTestImplementation mapJoinImplementation,
      MapJoinTestDescription testDesc, MapJoinTestData testData,
      RowTestObjectsMultiSet expectedTestRowMultiSet,
      String title)
          throws Exception {

    System.out.println("*BENCHMARK* Starting implementation " + mapJoinImplementation +
        " variation " + testDesc.vectorMapJoinVariation +
        " title " + title);

    // UNDONE: Parameterize for implementation variation?
    MapJoinDesc mapJoinDesc = MapJoinTestConfig.createMapJoinDesc(testDesc);

    final boolean isVectorOutput = MapJoinTestConfig.isVectorOutput(mapJoinImplementation);

    RowTestObjectsMultiSet outputTestRowMultiSet = new RowTestObjectsMultiSet();

    CreateMapJoinResult result =
        MapJoinTestConfig.createMapJoinImplementation(
            mapJoinImplementation, testDesc, testData, mapJoinDesc);
    MapJoinOperator mapJoinOperator = result.mapJoinOperator;
    MapJoinTableContainer mapJoinTableContainer = result.mapJoinTableContainer;
    MapJoinTableContainerSerDe mapJoinTableContainerSerDe = result.mapJoinTableContainerSerDe;

    CountCollectorTestOperator testCollectorOperator;
    if (!isVectorOutput) {
      testCollectorOperator =
          new TestMultiSetCollectorOperator(
              testDesc.outputObjectInspectors, outputTestRowMultiSet);
    } else {
      VectorizationContext vOutContext =
          ((VectorizationContextRegion) mapJoinOperator).getOutputVectorizationContext();
      testCollectorOperator =
          new TestMultiSetVectorCollectorOperator(
              ArrayUtils.toPrimitive(vOutContext.getProjectedColumns().toArray(new Integer[0])),
              testDesc.outputTypeInfos, testDesc.outputObjectInspectors, outputTestRowMultiSet);
    }

    MapJoinTestConfig.connectOperators(mapJoinOperator, testCollectorOperator);

    CountCollectorTestOperator interceptTestCollectorOperator = null;
    if (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER &&
        !mapJoinDesc.isDynamicPartitionHashJoin()) {

      if (mapJoinImplementation == MapJoinTestImplementation.ROW_MODE_HASH_MAP) {

        // Not supported.
        return;
      }

      // Wire in FULL OUTER Intercept.
      interceptTestCollectorOperator =
          MapJoinTestConfig.addFullOuterIntercept(
              mapJoinImplementation, testDesc, outputTestRowMultiSet, testData,
              mapJoinOperator, mapJoinTableContainer, mapJoinTableContainerSerDe);
    } else {

      // Invoke initializeOp methods.
      mapJoinOperator.initialize(
          testDesc.hiveConf, testDesc.inputObjectInspectors);

      // Fixup the mapJoinTables.
      mapJoinOperator.setTestMapJoinTableContainer(
          1, mapJoinTableContainer, mapJoinTableContainerSerDe);
    }

    if (!isVectorOutput) {
      MapJoinTestData.driveBigTableData(testDesc, testData, mapJoinOperator);
    } else {
      MapJoinTestData.driveVectorBigTableData(testDesc, testData, mapJoinOperator);
    }

    if (!testCollectorOperator.getIsClosed()) {
      Assert.fail("collector operator not closed");
    }
    if (testCollectorOperator.getIsAborted()) {
      Assert.fail("collector operator aborted");
    }
    if (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER &&
        !mapJoinDesc.isDynamicPartitionHashJoin()) {
      if (!interceptTestCollectorOperator.getIsClosed()) {
        Assert.fail("intercept collector operator not closed");
      }
      if (interceptTestCollectorOperator.getIsAborted()) {
        Assert.fail("intercept collector operator aborted");
      }
    }

    System.out.println("*BENCHMARK* executeTestImplementation row count " +
        testCollectorOperator.getRowCount());

    // Verify the output!
    String option = "";
    if (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER) {
      option = " mapJoinPlanVariation " + testDesc.mapJoinPlanVariation.name();
    }
    if (!expectedTestRowMultiSet.verify(outputTestRowMultiSet, "expected", "actual")) {
      System.out.println("*BENCHMARK* " + title + " verify failed" +
          " for implementation " + mapJoinImplementation +
          " variation " + testDesc.vectorMapJoinVariation + option);
      expectedTestRowMultiSet.displayDifferences(outputTestRowMultiSet, "expected", "actual");
    } else {
      System.out.println("*BENCHMARK* " + title + " verify succeeded " +
          " for implementation " + mapJoinImplementation +
          " variation " + testDesc.vectorMapJoinVariation + option);
    }
  }

  private HiveConf getHiveConf() {
    HiveConf hiveConf = new HiveConf();
    // TODO: HIVE-28033: TestMapJoinOperator to run on Tez
    hiveConf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    return hiveConf;
  }
}
