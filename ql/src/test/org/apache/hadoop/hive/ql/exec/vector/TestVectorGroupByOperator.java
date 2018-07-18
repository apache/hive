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

package org.apache.hadoop.hive.ql.exec.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.Constructor;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountStar;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeCaptureVectorToRowOutputOperator;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromConcat;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromLongIterables;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromObjectIterables;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromRepeats;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc.ProcessingMode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStdSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVarianceSample;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.sun.tools.javac.util.Pair;

/**
 * Unit test for the vectorized GROUP BY operator.
 */
public class TestVectorGroupByOperator {

  HiveConf hconf = new HiveConf();

  private static ExprNodeDesc buildColumnDesc(
      VectorizationContext ctx,
      String column,
      TypeInfo typeInfo) {

    return new ExprNodeColumnDesc(
        typeInfo, column, "table", false);
  }

  private static AggregationDesc buildAggregationDesc(
      VectorizationContext ctx,
      String aggregate,
      GenericUDAFEvaluator.Mode mode,
      String column,
      TypeInfo typeInfo) {

    ExprNodeDesc inputColumn = buildColumnDesc(ctx, column, typeInfo);

    ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
    params.add(inputColumn);

    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName(aggregate);
    agg.setMode(mode);
    agg.setParameters(params);

    TypeInfo[] typeInfos = new TypeInfo[] { typeInfo };

    final GenericUDAFEvaluator evaluator;
    PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    try {
      switch (aggregate) {
      case "count":
        evaluator = new GenericUDAFCount.GenericUDAFCountEvaluator();
        break;
      case "min":
        evaluator = new GenericUDAFMin.GenericUDAFMinEvaluator();
        break;
      case "max":
        evaluator = new GenericUDAFMax.GenericUDAFMaxEvaluator();
        break;
      case "sum":
        evaluator = (new GenericUDAFSum()).getEvaluator(typeInfos);
        break;
      case "avg":
        evaluator = (new GenericUDAFAverage()).getEvaluator(typeInfos);
        break;
      case "variance":
      case "var":
      case "var_pop":
        evaluator = new GenericUDAFVariance.GenericUDAFVarianceEvaluator();
        break;
      case "var_samp":
        evaluator = new GenericUDAFVarianceSample.GenericUDAFVarianceSampleEvaluator();
        break;
      case "std":
      case "stddev":
      case "stddev_pop":
        evaluator = new GenericUDAFStd.GenericUDAFStdEvaluator();
        break;
      case "stddev_samp":
        evaluator = new GenericUDAFStdSample.GenericUDAFStdSampleEvaluator();
        break;
      default:
        throw new RuntimeException("Unexpected aggregate " + aggregate);
      }
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
    agg.setGenericUDAFEvaluator(evaluator);
    return agg;
  }
  private static AggregationDesc buildAggregationDescCountStar(
      VectorizationContext ctx) {
    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName("count");
    agg.setMode(GenericUDAFEvaluator.Mode.PARTIAL1);
    agg.setParameters(new ArrayList<ExprNodeDesc>());
    agg.setGenericUDAFEvaluator(new GenericUDAFCount.GenericUDAFCountEvaluator());
    return agg;
  }


  private static Pair<GroupByDesc,VectorGroupByDesc> buildGroupByDescType(
      VectorizationContext ctx,
      String aggregate,
      GenericUDAFEvaluator.Mode mode,
      String column,
      TypeInfo dataType) {

    AggregationDesc agg = buildAggregationDesc(ctx, aggregate, mode,
        column, dataType);
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorDesc = new VectorGroupByDesc();

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    vectorDesc.setProcessingMode(ProcessingMode.GLOBAL);

    return new Pair<GroupByDesc,VectorGroupByDesc>(desc, vectorDesc);
  }

  private static Pair<GroupByDesc,VectorGroupByDesc> buildGroupByDescCountStar(
      VectorizationContext ctx) {

    AggregationDesc agg = buildAggregationDescCountStar(ctx);
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorDesc = new VectorGroupByDesc();
    vectorDesc.setVecAggrDescs(
        new VectorAggregationDesc[] {
          new VectorAggregationDesc(
              agg.getGenericUDAFName(),
              new GenericUDAFCount.GenericUDAFCountEvaluator(),
              agg.getMode(),
              null, ColumnVector.Type.NONE, null,
              TypeInfoFactory.longTypeInfo, ColumnVector.Type.LONG, VectorUDAFCountStar.class)});

    vectorDesc.setProcessingMode(VectorGroupByDesc.ProcessingMode.HASH);

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);

    return new Pair<GroupByDesc,VectorGroupByDesc>(desc, vectorDesc);
  }


  private static Pair<GroupByDesc,VectorGroupByDesc> buildKeyGroupByDesc(
      VectorizationContext ctx,
      String aggregate,
      String column,
      TypeInfo dataTypeInfo,
      String key,
      TypeInfo keyTypeInfo) {

    Pair<GroupByDesc,VectorGroupByDesc> pair =
        buildGroupByDescType(ctx, aggregate, GenericUDAFEvaluator.Mode.PARTIAL1, column, dataTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;
    vectorDesc.setProcessingMode(ProcessingMode.HASH);

    ExprNodeDesc keyExp = buildColumnDesc(ctx, key, keyTypeInfo);
    ArrayList<ExprNodeDesc> keys = new ArrayList<ExprNodeDesc>();
    keys.add(keyExp);
    desc.setKeys(keys);

    desc.getOutputColumnNames().add("_col1");

    return pair;
  }

  long outputRowCount = 0;

  @Test
  public void testMemoryPressureFlush() throws HiveException {

    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, "max",
        "Value", TypeInfoFactory.longTypeInfo,
        "Key", TypeInfoFactory.longTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;

    // Set the memory treshold so that we get 100Kb before we need to flush.
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    long maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();

    float treshold = 100.0f*1024.0f/maxMemory;
    desc.setMemoryThreshold(treshold);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    this.outputRowCount = 0;
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {
      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        ++outputRowCount;
      }
    });

    Iterable<Object> it = new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        return new Iterator<Object> () {
          long value = 0;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Object next() {
            return ++value;
          }

          @Override
          public void remove() {
          }
        };
      }
    };

    FakeVectorRowBatchFromObjectIterables data = new FakeVectorRowBatchFromObjectIterables(
        100,
        new String[] {"long", "long"},
        it,
        it);

    // The 'it' data source will produce data w/o ever ending
    // We want to see that memory pressure kicks in and some
    // entries in the VGBY are flushed.
    long countRowsProduced = 0;
    for (VectorizedRowBatch unit: data) {
      countRowsProduced += 100;
      vgo.process(unit,  0);
      if (0 < outputRowCount) {
        break;
      }
      // Set an upper bound how much we're willing to push before it should flush
      // we've set the memory treshold at 100kb, each key is distinct
      // It should not go beyond 100k/16 (key+data)
      assertTrue(countRowsProduced < 100*1024/16);
    }

    assertTrue(0 < outputRowCount);
  }

  @Test
  public void testMultiKeyIntStringInt() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"int", "string", "int", "double"},
            Arrays.asList(new Object[]{null,   1,   1,  null,    2,    2, null}),
            Arrays.asList(new Object[]{ "A", "A",  "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{null,   2,   2,  null,    2,    2, null}),
            Arrays.asList(new Object[]{1.0,  2.0, 4.0,   8.0, 16.0, 32.0, 64.0})),
        buildHashMap(
            Arrays.asList(   1,  "A",    2), 6.0,
            Arrays.asList(null,  "C", null), 8.0,
            Arrays.asList(   2, null,    2), 48.0,
            Arrays.asList(null,  "A", null), 65.0));
  }

  @Test
  public void testMultiKeyStringByteString() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            1,
            new String[] {"string", "tinyint", "string", "double"},
            Arrays.asList(new Object[]{"A", "A", null}),
            Arrays.asList(new Object[]{  1,  1,     1}),
            Arrays.asList(new Object[]{ "A", "A", "A"}),
            Arrays.asList(new Object[]{ 1.0, 1.0, 1.0})),
        buildHashMap(
            Arrays.asList( "A",    (byte)1,  "A"), 2.0,
            Arrays.asList( null,   (byte)1,  "A"), 1.0));
  }

  @Test
  public void testMultiKeyStringIntString() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"string", "int", "string", "double"},
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{null,   1,  1, null,    2,    2, null}),
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{ 1.0, 1.0,  1.0, 1.0, 1.0,  1.0,  1.0})),
        buildHashMap(
            Arrays.asList(null,    2, null), 2.0,
            Arrays.asList( "C", null,  "C"), 1.0,
            Arrays.asList( "A",    1,  "A"), 2.0,
            Arrays.asList( "A", null,  "A"), 2.0));
  }

  @Test
  public void testMultiKeyIntStringString() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"int", "string", "string", "double"},
            Arrays.asList(new Object[]{null,   1,  1, null,    2,    2, null}),
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{ 1.0, 1.0,  1.0, 1.0, 1.0,  1.0,  1.0})),
        buildHashMap(
            Arrays.asList(   2, null, null), 2.0,
            Arrays.asList(null,  "C",  "C"), 1.0,
            Arrays.asList(   1,  "A",  "A"), 2.0,
            Arrays.asList(null,  "A",  "A"), 2.0));
  }

  @Test
  public void testMultiKeyDoubleStringInt() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"double", "string", "int", "double"},
            Arrays.asList(new Object[]{null,  1.0, 1.0,  null,  2.0,  2.0, null}),
            Arrays.asList(new Object[]{ "A", "A",  "A", "C",   null, null,  "A"}),
            Arrays.asList(new Object[]{null,   2,   2,  null,     2,    2, null}),
            Arrays.asList(new Object[]{1.0,  2.0, 4.0,   8.0, 16.0, 32.0, 64.0})),
        buildHashMap(
            Arrays.asList( 1.0,  "A",    2), 6.0,
            Arrays.asList(null,  "C", null), 8.0,
            Arrays.asList( 2.0, null,    2), 48.0,
            Arrays.asList(null,  "A", null), 65.0));
  }

  @Test
  public void testMultiKeyDoubleShortString() throws HiveException {
    short s = 2;
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"double", "smallint", "string", "double"},
            Arrays.asList(new Object[]{null,  1.0, 1.0,  null,  2.0,  2.0, null}),
            Arrays.asList(new Object[]{null,  s,     s,  null,    s,    s, null}),
            Arrays.asList(new Object[]{ "A", "A",  "A",   "C", null, null,  "A"}),
            Arrays.asList(new Object[]{1.0,  2.0,  4.0,   8.0, 16.0, 32.0,  64.0})),
        buildHashMap(
            Arrays.asList( 1.0,    s,  "A"), 6.0,
            Arrays.asList(null,  null, "C"), 8.0,
            Arrays.asList( 2.0,    s, null), 48.0,
            Arrays.asList(null, null,  "A"), 65.0));
  }


  @Test
  public void testDoubleValueTypeSum() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 20.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeSumOneKey() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 39.0));
  }

  @Test
  public void testDoubleValueTypeCount() throws HiveException {
    testKeyTypeAggregate(
        "count",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 2L, null, 1L));
  }

  public void testDoubleValueTypeCountOneKey() throws HiveException {
    testKeyTypeAggregate(
        "count",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 3L));
  }

  @Test
  public void testDoubleValueTypeAvg() throws HiveException {
    testKeyTypeAggregate(
        "avg",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 10.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeAvgOneKey() throws HiveException {
    testKeyTypeAggregate(
        "avg",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 13.0));
  }

  @Test
  public void testDoubleValueTypeMin() throws HiveException {
    testKeyTypeAggregate(
        "min",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 7.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeMinOneKey() throws HiveException {
    testKeyTypeAggregate(
        "min",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 7.0));
  }

  @Test
  public void testDoubleValueTypeMax() throws HiveException {
    testKeyTypeAggregate(
        "max",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 13.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeMaxOneKey() throws HiveException {
    testKeyTypeAggregate(
        "max",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 19.0));
  }

  @Test
  public void testDoubleValueTypeVariance() throws HiveException {
    testKeyTypeAggregate(
        "variance",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 9.0, null, 0.0));
  }

  @Test
  public void testDoubleValueTypeVarianceOneKey() throws HiveException {
    testKeyTypeAggregate(
        "variance",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 24.0));
  }
  @Test
  public void testTinyintKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap((byte)1, 20L, null, 19L));
  }

  @Test
  public void testSmallintKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"smallint", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap((short)1, 20L, null, 19L));
  }

  @Test
  public void testIntKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"int", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(1, 20L, null, 19L));
  }

  @Test
  public void testBigintKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"bigint", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(1L, 20L, null, 19L));
  }

  @Test
  public void testBooleanKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"boolean", "bigint"},
            Arrays.asList(new Object[]{  true,null, true, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(true, 20L, null, 19L));
  }

  @Test
  public void testTimestampKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"timestamp", "bigint"},
            Arrays.asList(new Object[]{new Timestamp(1),null, new Timestamp(1), null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(new Timestamp(1), 20L, null, 19L));
  }

  @Test
  public void testFloatKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"float", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap((float)1.0, 20L, null, 19L));
  }

  @Test
  public void testDoubleKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"double", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(1.0, 20L, null, 19L));
  }

  @Test
  public void testCountStar() throws HiveException {
    testAggregateCountStar(
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        4L);
  }

  @Test
  public void testCountReduce() throws HiveException {
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{}),
            0L);
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{0L}),
            0L);
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{0L,0L}),
            0L);
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{0L,1L,0L}),
            1L);
    testAggregateCountReduce(
        2,
        Arrays.asList(new Long[]{13L,0L,7L,19L}),
        39L);
  }

  @Test
  public void testCountDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "count",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       3L);
  }

  @Test
  public void testMaxDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "max",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create(3));
    testAggregateDecimal(
        "Decimal",
        "max",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(3),
                HiveDecimal.create(2),
                HiveDecimal.create(1)}),
        HiveDecimal.create(3));
    testAggregateDecimal(
        "Decimal",
        "max",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(2),
                HiveDecimal.create(3),
                HiveDecimal.create(1)}),
        HiveDecimal.create(3));
  }

  @Test
  public void testMinDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "min",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create(1));
    testAggregateDecimal(
        "Decimal",
        "min",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(3),
                HiveDecimal.create(2),
                HiveDecimal.create(1)}),
        HiveDecimal.create(1));

    testAggregateDecimal(
        "Decimal",
        "min",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(2),
                HiveDecimal.create(1),
                HiveDecimal.create(3)}),
        HiveDecimal.create(1));
  }

  @Test
  public void testSumDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
       "sum",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create(1+2+3));
  }

  @Test
  public void testSumDecimalHive6508() throws HiveException {
    short scale = 4;
    testAggregateDecimal(
        "Decimal(10,4)",
        "sum",
        4,
        Arrays.asList(new Object[]{
                HiveDecimal.create("1234.2401"),
                HiveDecimal.create("1868.52"),
                HiveDecimal.ZERO,
                HiveDecimal.create("456.84"),
                HiveDecimal.create("121.89")}),
       HiveDecimal.create("3681.4901"));
  }

  @Test
  public void testAvgDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "avg",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create((1+2+3)/3));
  }

  @Test
  public void testAvgDecimalNegative() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "avg",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(-1),
                HiveDecimal.create(-2),
                HiveDecimal.create(-3)}),
        HiveDecimal.create((-1-2-3)/3));
  }

  @Test
  public void testVarianceDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "variance",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        (double) 30);
  }

  @Test
  public void testVarSampDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "var_samp",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        (double) 40);
  }

  @Test
  public void testStdPopDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "stddev_pop",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        Math.sqrt(30));
  }

  @Test
  public void testStdSampDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "stddev_samp",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        Math.sqrt(40));
  }

  @Test
  public void testDecimalKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"decimal(38,0)", "bigint"},
            Arrays.asList(new Object[]{
                    HiveDecimal.create(1),null,
                    HiveDecimal.create(1), null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(HiveDecimal.create(1), 20L, null, 19L));
  }


  @Test
  public void testCountString() throws HiveException {
    testAggregateString(
        "count",
        2,
        Arrays.asList(new Object[]{"A","B","C"}),
        3L);
  }

  @Test
  public void testMaxString() throws HiveException {
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{"A","B","C"}),
        "C");
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{"C", "B", "A"}),
        "C");
  }

  @Test
  public void testMinString() throws HiveException {
    testAggregateString(
        "min",
        2,
        Arrays.asList(new Object[]{"A","B","C"}),
        "A");
    testAggregateString(
        "min",
        2,
        Arrays.asList(new Object[]{"C", "B", "A"}),
        "A");
  }

  @Test
  public void testMaxNullString() throws HiveException {
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{"A","B",null}),
        "B");
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{null, null, null}),
        null);
  }

  @Test
  public void testCountStringWithNull() throws HiveException {
    testAggregateString(
        "count",
        2,
        Arrays.asList(new Object[]{"A",null,"C", "D", null}),
        3L);
  }

  @Test
  public void testCountStringAllNull() throws HiveException {
    testAggregateString(
        "count",
        4,
        Arrays.asList(new Object[]{null, null, null, null, null}),
        0L);
  }


  @Test
  public void testMinLongNullStringKeys() throws HiveException {
    testAggregateStringKeyAggregate(
        "min",
        2,
        Arrays.asList(new Object[]{"A",null,"A",null}),
        Arrays.asList(new Object[]{13L, 5L, 7L,19L}),
        buildHashMap("A", 7L, null, 5L));
  }

  @Test
  public void testMinLongStringKeys() throws HiveException {
    testAggregateStringKeyAggregate(
        "min",
        2,
        Arrays.asList(new Object[]{"A","B","A","B"}),
        Arrays.asList(new Object[]{13L, 5L, 7L,19L}),
        buildHashMap("A", 7L, "B", 5L));
  }

  @Test
  public void testMinLongKeyGroupByCompactBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{01L,1L,2L,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(1L, 5L, 2L, 7L));
  }

  @Test
  public void testMinLongKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        4,
        Arrays.asList(new Long[]{01L,1L,2L,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(1L, 5L, 2L, 7L));
  }

  @Test
  public void testMinLongKeyGroupByCrossBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{01L,2L,1L,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(1L, 7L, 2L, 5L));
  }

  @Test
  public void testMinLongNullKeyGroupByCrossBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 7L, 2L, 5L));
  }

  @Test
  public void testMinLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 7L, 2L, 5L));
  }

  @Test
  public void testMaxLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "max",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 13L, 2L, 19L));
  }

  @Test
  public void testCountLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "count",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 2L, 2L, 2L));
  }

  @Test
  public void testSumLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "sum",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 20L, 2L, 24L));
  }

  @Test
  public void testAvgLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "avg",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 10.0, 2L, 12.0));
  }

  @Test
  public void testVarLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "variance",
        4,
        Arrays.asList(new Long[]{null,2L,01L,02L,01L,01L}),
        Arrays.asList(new Long[]{13L, 5L,18L,19L,12L,15L}),
        buildHashMap(null, 0.0, 2L, 49.0, 01L, 6.0));
  }

  @Test
  public void testMinNullLongNullKeyGroupBy() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{null, null, null, null}),
        buildHashMap(null, null, 2L, null));
  }

  @Test
  public void testMinLongGroupBy() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        5L);
  }


  @Test
  public void testMinLongSimple() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        5L);
  }

  @Test
  public void testMinLongEmpty() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{}),
        null);
  }

  @Test
  public void testMinLongNulls() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null}),
        null);
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        null);
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        5L);
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        7L);
  }

  @Test
  public void testMinLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "min",
        42L,
        4096,
        1024,
        42L);
  }

  @Test
  public void testMinLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "min",
        null,
        4096,
        1024,
        null);
  }


  @Test
  public void testMinLongNegative () throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,-19L}),
        -19L);
  }

  @Test
  public void testMinLongMinInt () throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,(long)Integer.MIN_VALUE,-19L}),
        (long)Integer.MIN_VALUE);
  }

  @Test
  public void testMinLongMinLong () throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L, Long.MIN_VALUE, (long)Integer.MIN_VALUE}),
        Long.MIN_VALUE);
  }

  @Test
  public void testMaxLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        19L);
  }

  @Test
  public void testMaxLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{}),
        null);
  }


  @Test
  public void testMaxLongNegative () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{-13L,-5L,-7L,-19L}),
        -5L);
  }

  @Test
  public void testMaxLongMaxInt () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,(long)Integer.MAX_VALUE}),
        (long)Integer.MAX_VALUE);
  }

  @Test
  public void testMaxLongMaxLong () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{13L,Long.MAX_VALUE - 1L,Long.MAX_VALUE,(long)Integer.MAX_VALUE}),
        Long.MAX_VALUE);
  }

  @Test
  public void testMaxLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "max",
        42L,
        4096,
        1024,
        42L);
  }

  @Test
  public void testMaxLongNulls () throws HiveException {
    testAggregateLongRepeats (
        "max",
        null,
        4096,
        1024,
        null);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMinLongConcatRepeat () throws HiveException {
    testAggregateLongIterable ("min",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromRepeats(
                new Long[] {7L}, 15, 2),
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2)),
         7L);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMinLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("min",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         7L);
  }

  @Test
  public void testCountLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        4L);
  }

  @Test
  public void testCountLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{}),
        0L);
  }

  @Test
  public void testCountLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{null}),
        0L);
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        0L);
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        3L);
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        3L);
  }


  @Test
  public void testCountLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "count",
        42L,
        4096,
        1024,
        4096L);
  }

  @Test
  public void testCountLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "count",
        null,
        4096,
        1024,
        0L);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testCountLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("count",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         14L);
  }

  @Test
  public void testSumDoubleSimple() throws HiveException {
    testAggregateDouble(
        "sum",
        2,
        Arrays.asList(new Object[]{13.0,5.0,7.0,19.0}),
        13.0 + 5.0 + 7.0 + 19.0);
  }

  @Test
  public void testSumDoubleGroupByString() throws HiveException {
    testAggregateDoubleStringKeyAggregate(
        "sum",
        4,
        Arrays.asList(new Object[]{"A", null, "A", null}),
        Arrays.asList(new Object[]{13.0,5.0,7.0,19.0}),
        buildHashMap("A", 20.0, null, 24.0));
  }

  @Test
  public void testSumLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        13L + 5L + 7L + 19L);
  }

  @Test
  public void testSumLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{}),
        null);
  }

  @Test
  public void testSumLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{null}),
        null);
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        null);
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        5L + 7L + 19L);
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        13L + 7L + 19L);
  }

  @Test
  public void testSumLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "sum",
        42L,
        4096,
        1024,
        4096L * 42L);
  }

  @Test
  public void testSumLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "sum",
        null,
        4096,
        1024,
        null);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testSumLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("sum",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         19L*10L + 13L + 7L + 23L +29L);
  }

  @Test
  public void testSumLongZero () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{-(long)Integer.MAX_VALUE, (long)Integer.MAX_VALUE}),
        0L);
  }

  @Test
  public void testSumLong2MaxInt () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{(long)Integer.MAX_VALUE, (long)Integer.MAX_VALUE}),
        4294967294L);
  }

  @Test
  public void testSumLong2MinInt () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{(long)Integer.MIN_VALUE, (long)Integer.MIN_VALUE}),
        -4294967296L);
  }

  @Test
  public void testSumLong2MaxLong () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{Long.MAX_VALUE, Long.MAX_VALUE}),
        -2L); // silent overflow
  }

  @Test
  public void testSumLong2MinLong () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{Long.MIN_VALUE, Long.MIN_VALUE}),
        0L); // silent overflow
  }

  @Test
  public void testSumLongMinMaxLong () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{Long.MAX_VALUE, Long.MIN_VALUE}),
        -1L);
  }

  @Test
  public void testAvgLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        (double) (13L + 5L + 7L + 19L) / (double) 4L);
  }

  @Test
  public void testAvgLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }

  @Test
  public void testAvgLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{null}),
        0.0);
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        0.0);
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        (double) (5L + 7L + 19L) / (double) 3L);
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        (double) (13L + + 7L + 19L) / (double) 3L);
  }


  @Test
  public void testAvgLongRepeat () throws HiveException
  {
    testAggregateLongRepeats (
        "avg",
        42L,
        4096,
        1024,
        (double)42);
  }

  @Test
  public void testAvgLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "avg",
        null,
        4096,
        1024,
        0.0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAvgLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("avg",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         (double) (19L*10L + 13L + 7L + 23L +29L) / (double) 14 );
  }

  @Test
  public void testVarianceLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        (double) 30L);
  }

  @Test
  public void testVarianceLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }

  @Test
  public void testVarianceLongSingle () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{97L}),
        0.0);
  }

  @Test
  public void testVarianceLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null}),
        0.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        0.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null,13L, 5L,7L,19L}),
        30.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{13L,null,5L, 7L,19L}),
        30.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null,null,null,19L}),
        (double) 0);
  }

  @Test
  public void testVarPopLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "var_pop",
        null,
        4096,
        1024,
        0.0);
  }

  @Test
  public void testVarPopLongRepeat () throws HiveException  {
    testAggregateLongRepeats (
        "var_pop",
        42L,
        4096,
        1024,
        (double)0);
  }

  @Test
  public void testVarSampLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "var_samp",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        (double) 40L);
  }

  @Test
  public void testVarSampLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "var_samp",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }


  @Test
  public void testVarSampLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "var_samp",
        42L,
        4096,
        1024,
        (double)0);
  }

  @Test
  public void testStdLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "std",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        Math.sqrt(30));
  }

  @Test
  public void testStdLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "std",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }


  @Test
  public void testStdDevLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "stddev",
        42L,
        4096,
        1024,
        (double)0);
  }

  @Test
  public void testStdDevLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "stddev",
        null,
        4096,
        1024,
        0.0);
  }


  @Test
  public void testStdDevSampSimple () throws HiveException {
    testAggregateLongAggregate(
        "stddev_samp",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        Math.sqrt(40));
  }

  @Test
  public void testStdDevSampLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "stddev_samp",
        42L,
        3,
        1024,
        (double)0);
  }

  private void testMultiKey(
      String aggregateName,
      FakeVectorRowBatchFromObjectIterables data,
      HashMap<Object, Object> expected) throws HiveException {

    Map<String, Integer> mapColumnNames = new HashMap<String, Integer>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<ExprNodeDesc> keysDesc = new ArrayList<ExprNodeDesc>();
    Set<Object> keys = new HashSet<Object>();

    // The types array tells us the number of columns in the data
    final String[] columnTypes = data.getTypes();

    // Columns 0..N-1 are keys. Column N is the aggregate value input
    int i=0;
    for(; i<columnTypes.length - 1; ++i) {
      String columnName = String.format("_col%d", i);
      mapColumnNames.put(columnName, i);
      outputColumnNames.add(columnName);
    }

    mapColumnNames.put("value", i);
    outputColumnNames.add("value");
    VectorizationContext ctx = new VectorizationContext("name", outputColumnNames);

    ArrayList<AggregationDesc> aggs = new ArrayList(1);
    aggs.add(
        buildAggregationDesc(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1,
            "value", TypeInfoFactory.getPrimitiveTypeInfo(columnTypes[i])));

    for(i=0; i<columnTypes.length - 1; ++i) {
      String columnName = String.format("_col%d", i);
      keysDesc.add(
        buildColumnDesc(ctx, columnName,
            TypeInfoFactory.getPrimitiveTypeInfo(columnTypes[i])));
    }

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorGroupByDesc = new VectorGroupByDesc();

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    desc.setKeys(keysDesc);
    vectorGroupByDesc.setProcessingMode(ProcessingMode.HASH);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorGroupByDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private int rowIndex;
      private String aggregateName;
      private Map<Object,Object> expected;
      private Set<Object> keys;

      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(columnTypes.length, fields.length);
        ArrayList<Object> keyValue = new ArrayList<Object>(columnTypes.length-1);
        for(int i=0; i<columnTypes.length-1; ++i) {
          Object key = fields[i];
          if (null == key) {
            keyValue.add(null);
          } else if (key instanceof Text) {
            Text txKey = (Text)key;
            keyValue.add(txKey.toString());
          } else if (key instanceof ByteWritable) {
            ByteWritable bwKey = (ByteWritable)key;
            keyValue.add(bwKey.get());
          } else if (key instanceof ShortWritable) {
            ShortWritable swKey = (ShortWritable)key;
            keyValue.add(swKey.get());
          } else if (key instanceof IntWritable) {
            IntWritable iwKey = (IntWritable)key;
            keyValue.add(iwKey.get());
          } else if (key instanceof LongWritable) {
            LongWritable lwKey = (LongWritable)key;
            keyValue.add(lwKey.get());
          } else if (key instanceof TimestampWritableV2) {
            TimestampWritableV2 twKey = (TimestampWritableV2)key;
            keyValue.add(twKey.getTimestamp());
          } else if (key instanceof DoubleWritable) {
            DoubleWritable dwKey = (DoubleWritable)key;
            keyValue.add(dwKey.get());
          } else if (key instanceof FloatWritable) {
            FloatWritable fwKey = (FloatWritable)key;
            keyValue.add(fwKey.get());
          } else if (key instanceof BooleanWritable) {
            BooleanWritable bwKey = (BooleanWritable)key;
            keyValue.add(bwKey.get());
          } else {
            Assert.fail(String.format("Not implemented key output type %s: %s",
                key.getClass().getName(), key));
          }
        }

        String keyAsString = Arrays.deepToString(keyValue.toArray());
        assertTrue(expected.containsKey(keyValue));
        Object expectedValue = expected.get(keyValue);
        Object value = fields[columnTypes.length-1];
        Validator validator = getValidator(aggregateName);
        validator.validate(keyAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, Map<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }


  private void testKeyTypeAggregate(
      String aggregateName,
      FakeVectorRowBatchFromObjectIterables data,
      Map<Object, Object> expected) throws HiveException {

    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);
    Set<Object> keys = new HashSet<Object>();

    AggregationDesc agg = buildAggregationDesc(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1,
        "Value", TypeInfoFactory.getPrimitiveTypeInfo(data.getTypes()[1]));
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");
    outputColumnNames.add("_col1");

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorGroupByDesc = new VectorGroupByDesc();

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    vectorGroupByDesc.setProcessingMode(ProcessingMode.HASH);

    ExprNodeDesc keyExp = buildColumnDesc(ctx, "Key",
        TypeInfoFactory.getPrimitiveTypeInfo(data.getTypes()[0]));
    ArrayList<ExprNodeDesc> keysDesc = new ArrayList<ExprNodeDesc>();
    keysDesc.add(keyExp);
    desc.setKeys(keysDesc);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorGroupByDesc);
    if (vgo == null) {
      assertTrue(false);
    }

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private int rowIndex;
      private String aggregateName;
      private Map<Object,Object> expected;
      private Set<Object> keys;

      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(2, fields.length);
        Object key = fields[0];
        Object keyValue = null;
        if (null == key) {
          keyValue = null;
        } else if (key instanceof ByteWritable) {
          ByteWritable bwKey = (ByteWritable)key;
          keyValue = bwKey.get();
        } else if (key instanceof ShortWritable) {
          ShortWritable swKey = (ShortWritable)key;
          keyValue = swKey.get();
        } else if (key instanceof IntWritable) {
          IntWritable iwKey = (IntWritable)key;
          keyValue = iwKey.get();
        } else if (key instanceof LongWritable) {
          LongWritable lwKey = (LongWritable)key;
          keyValue = lwKey.get();
        } else if (key instanceof TimestampWritableV2) {
          TimestampWritableV2 twKey = (TimestampWritableV2)key;
          keyValue = twKey.getTimestamp().toSqlTimestamp();
        } else if (key instanceof DoubleWritable) {
          DoubleWritable dwKey = (DoubleWritable)key;
          keyValue = dwKey.get();
        } else if (key instanceof FloatWritable) {
          FloatWritable fwKey = (FloatWritable)key;
          keyValue = fwKey.get();
        } else if (key instanceof BooleanWritable) {
          BooleanWritable bwKey = (BooleanWritable)key;
          keyValue = bwKey.get();
        } else if (key instanceof HiveDecimalWritable) {
            HiveDecimalWritable hdwKey = (HiveDecimalWritable)key;
            keyValue = hdwKey.getHiveDecimal();
        } else {
          Assert.fail(String.format("Not implemented key output type %s: %s",
              key.getClass().getName(), key));
        }

        String keyValueAsString = String.format("%s", keyValue);

        assertTrue(expected.containsKey(keyValue));
        Object expectedValue = expected.get(keyValue);
        Object value = fields[1];
        Validator validator = getValidator(aggregateName);
        validator.validate(keyValueAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, Map<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }


  public void testAggregateLongRepeats (
    String aggregateName,
    Long value,
    int repeat,
    int batchSize,
    Object expected) throws HiveException {
    FakeVectorRowBatchFromRepeats fdr = new FakeVectorRowBatchFromRepeats(
        new Long[] {value}, repeat, batchSize);
    testAggregateLongIterable (aggregateName, fdr, expected);
  }

  public HashMap<Object, Object> buildHashMap(Object... pairs) {
    HashMap<Object, Object> map = new HashMap<Object, Object>();
    for(int i = 0; i < pairs.length; i += 2) {
      map.put(pairs[i], pairs[i+1]);
    }
    return map;
  }

  public void testAggregateStringKeyAggregate (
      String aggregateName,
      int batchSize,
      Iterable<Object> list,
      Iterable<Object> values,
      HashMap<Object, Object> expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize,
        new String[] {"string", "long"},
        list,
        values);
    testAggregateStringKeyIterable (aggregateName, fdr,  TypeInfoFactory.longTypeInfo, expected);
  }

  public void testAggregateDoubleStringKeyAggregate (
      String aggregateName,
      int batchSize,
      Iterable<Object> list,
      Iterable<Object> values,
      HashMap<Object, Object> expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize,
        new String[] {"string", "double"},
        list,
        values);
    testAggregateStringKeyIterable (aggregateName, fdr,  TypeInfoFactory.doubleTypeInfo, expected);
  }

  public void testAggregateLongKeyAggregate (
      String aggregateName,
      int batchSize,
      List<Long> list,
      Iterable<Long> values,
      HashMap<Object, Object> expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        list, values);
    testAggregateLongKeyIterable (aggregateName, fdr, expected);
  }

  public void testAggregateDecimal (
      String typeName,
      String aggregateName,
      int batchSize,
      Iterable<Object> values,
      Object expected) throws HiveException {

        @SuppressWarnings("unchecked")
        FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
            batchSize, new String[] {typeName}, values);
        testAggregateDecimalIterable (aggregateName, fdr, expected);
      }


  public void testAggregateString (
      String aggregateName,
      int batchSize,
      Iterable<Object> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize, new String[] {"string"}, values);
    testAggregateStringIterable (aggregateName, fdr, expected);
  }

  public void testAggregateDouble (
      String aggregateName,
      int batchSize,
      Iterable<Object> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize, new String[] {"double"}, values);
    testAggregateDoubleIterable (aggregateName, fdr, expected);
  }


  public void testAggregateLongAggregate (
      String aggregateName,
      int batchSize,
      Iterable<Long> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        values);
    testAggregateLongIterable (aggregateName, fdr, expected);
  }

  public void testAggregateCountStar (
      int batchSize,
      Iterable<Long> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        values);
    testAggregateCountStarIterable (fdr, expected);
  }

  public void testAggregateCountReduce (
      int batchSize,
      Iterable<Long> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        values);
    testAggregateCountReduceIterable (fdr, expected);
  }


  public static interface Validator {
    void validate (String key, Object expected, Object result);
  };

  public static class ValueValidator implements Validator {
    @Override
    public void validate(String key, Object expected, Object result) {

      assertEquals(true, result instanceof Object[]);
      Object[] arr = (Object[]) result;
      assertEquals(1, arr.length);

      if (expected == null) {
        assertEquals(key, null, arr[0]);
      } else if (arr[0] instanceof LongWritable) {
        LongWritable lw = (LongWritable) arr[0];
        assertEquals(key, expected, lw.get());
      } else if (arr[0] instanceof Text) {
        Text tx = (Text) arr[0];
        String sbw = tx.toString();
        assertEquals(key, expected, sbw);
      } else if (arr[0] instanceof DoubleWritable) {
        DoubleWritable dw = (DoubleWritable) arr[0];
        assertEquals (key, expected, dw.get());
      } else if (arr[0] instanceof Double) {
        assertEquals (key, expected, arr[0]);
      } else if (arr[0] instanceof Long) {
        assertEquals (key, expected, arr[0]);
      } else if (arr[0] instanceof HiveDecimalWritable) {
        HiveDecimalWritable hdw = (HiveDecimalWritable) arr[0];
        HiveDecimal hd = hdw.getHiveDecimal();
        HiveDecimal expectedDec = (HiveDecimal)expected;
        assertEquals (key, expectedDec, hd);
      } else if (arr[0] instanceof HiveDecimal) {
          HiveDecimal hd = (HiveDecimal) arr[0];
          HiveDecimal expectedDec = (HiveDecimal)expected;
          assertEquals (key, expectedDec, hd);
      } else {
        Assert.fail("Unsupported result type: " + arr[0].getClass().getName());
      }
    }
  }

  public static class AvgValidator implements Validator {

    @Override
    public void validate(String key, Object expected, Object result) {
      Object[] arr = (Object[]) result;
      assertEquals (1, arr.length);

      if (expected == null) {
        assertEquals(key, null, arr[0]);
      } else {
        assertEquals (true, arr[0] instanceof Object[]);
        Object[] vals = (Object[]) arr[0];
        assertEquals (3, vals.length);

        assertEquals (true, vals[0] instanceof LongWritable);
        LongWritable lw = (LongWritable) vals[0];

        if (vals[1] instanceof DoubleWritable) {
          DoubleWritable dw = (DoubleWritable) vals[1];
          if (lw.get() != 0L) {
            assertEquals (key, expected, dw.get() / lw.get());
          } else {
            assertEquals(key, expected, 0.0);
          }
        } else if (vals[1] instanceof HiveDecimalWritable) {
          HiveDecimalWritable hdw = (HiveDecimalWritable) vals[1];
          if (lw.get() != 0L) {
            assertEquals (key, expected, hdw.getHiveDecimal().divide(HiveDecimal.create(lw.get())));
          } else {
            assertEquals(key, expected, HiveDecimal.ZERO);
          }
        }
      }
    }

  }

  public abstract static class BaseVarianceValidator implements Validator {

    abstract void validateVariance (String key,
        double expected, long cnt, double sum, double variance);

    @Override
    public void validate(String key, Object expected, Object result) {
      Object[] arr = (Object[]) result;
      assertEquals (1, arr.length);

      if (expected == null) {
        assertEquals(null, arr[0]);
      } else {
        assertEquals (true, arr[0] instanceof Object[]);
        Object[] vals = (Object[]) arr[0];
        assertEquals (3, vals.length);

        assertEquals (true, vals[0] instanceof LongWritable);
        assertEquals (true, vals[1] instanceof DoubleWritable);
        assertEquals (true, vals[2] instanceof DoubleWritable);
        LongWritable cnt = (LongWritable) vals[0];
        if (cnt.get() == 0) {
          assertEquals(key, expected, 0.0);
        } else {
          DoubleWritable sum = (DoubleWritable) vals[1];
          DoubleWritable var = (DoubleWritable) vals[2];
          assertTrue (1 <= cnt.get());
          validateVariance (key, (Double) expected, cnt.get(), sum.get(), var.get());
        }
      }
    }
  }

  public static class VarianceValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, variance /cnt, 0.0);
    }
  }

  public static class VarianceSampValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, variance /(cnt-1), 0.0);
    }
  }

  public static class StdValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, Math.sqrt(variance / cnt), 0.0);
    }
  }

  public static class StdSampValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, Math.sqrt(variance / (cnt-1)), 0.0);
    }
  }

  private static Object[][] validators = {
      {"count", ValueValidator.class},
      {"min", ValueValidator.class},
      {"max", ValueValidator.class},
      {"sum", ValueValidator.class},
      {"avg", AvgValidator.class},
      {"variance", VarianceValidator.class},
      {"var_pop", VarianceValidator.class},
      {"var_samp", VarianceSampValidator.class},
      {"std", StdValidator.class},
      {"stddev", StdValidator.class},
      {"stddev_pop", StdValidator.class},
      {"stddev_samp", StdSampValidator.class},
  };

  public static Validator getValidator(String aggregate) throws HiveException {
    try
    {
      for (Object[] v: validators) {
        if (aggregate.equalsIgnoreCase((String) v[0])) {
          @SuppressWarnings("unchecked")
          Class<? extends Validator> c = (Class<? extends Validator>) v[1];
          Constructor<? extends Validator> ctr = c.getConstructor();
          return ctr.newInstance();
        }
      }
    }catch(Exception e) {
      throw new HiveException(e);
    }
    throw new HiveException("Missing validator for aggregate: " + aggregate);
  }

  public void testAggregateCountStarIterable (
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescCountStar (ctx);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;
    vectorDesc.setProcessingMode(ProcessingMode.HASH);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator("count");
    validator.validate("_total", expected, result);
  }

  public void testAggregateCountReduceIterable (
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType(ctx, "count", GenericUDAFEvaluator.Mode.FINAL, "A", TypeInfoFactory.longTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;
    vectorDesc.setProcessingMode(ProcessingMode.GLOBAL);  // Use GLOBAL when no key for Reduce.
    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator("count");
    validator.validate("_total", expected, result);
  }

  public void testAggregateStringIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A",
        TypeInfoFactory.stringTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }

  public void testAggregateDecimalIterable (
          String aggregateName,
          Iterable<VectorizedRowBatch> data,
          Object expected) throws HiveException {
          List<String> mapColumnNames = new ArrayList<String>();
          mapColumnNames.add("A");
          VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair =
        buildGroupByDescType(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A", TypeInfoFactory.getDecimalTypeInfo(30, 4));
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit : data) {
      vgo.process(unit, 0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }


  public void testAggregateDoubleIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType (ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A",
        TypeInfoFactory.doubleTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }

  public void testAggregateLongIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A", TypeInfoFactory.longTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }

  public void testAggregateLongKeyIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      HashMap<Object,Object> expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Set<Object> keys = new HashSet<Object>();

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, aggregateName, "Value",
        TypeInfoFactory.longTypeInfo, "Key", TypeInfoFactory.longTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private String aggregateName;
      private HashMap<Object,Object> expected;
      private Set<Object> keys;

      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(2, fields.length);
        Object key = fields[0];
        Long keyValue = null;
        if (null != key) {
          assertTrue(key instanceof LongWritable);
          LongWritable lwKey = (LongWritable)key;
          keyValue = lwKey.get();
        }
        assertTrue(expected.containsKey(keyValue));
        String keyAsString = String.format("%s", key);
        Object expectedValue = expected.get(keyValue);
        Object value = fields[1];
        Validator validator = getValidator(aggregateName);
        validator.validate(keyAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, HashMap<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }

  public void testAggregateStringKeyIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      TypeInfo dataTypeInfo,
      HashMap<Object,Object> expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);
    Set<Object> keys = new HashSet<Object>();

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, aggregateName, "Value",
       dataTypeInfo, "Key", TypeInfoFactory.stringTypeInfo);
    GroupByDesc desc = pair.fst;
    VectorGroupByDesc vectorDesc = pair.snd;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private int rowIndex;
      private String aggregateName;
      private HashMap<Object,Object> expected;
      private Set<Object> keys;

      @SuppressWarnings("deprecation")
      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(2, fields.length);
        Object key = fields[0];
        String keyValue = null;
        if (null != key) {
          assertTrue(key instanceof Text);
          Text bwKey = (Text)key;
          keyValue = bwKey.toString();
        }
        assertTrue(expected.containsKey(keyValue));
        Object expectedValue = expected.get(keyValue);
        Object value = fields[1];
        Validator validator = getValidator(aggregateName);
        String keyAsString = String.format("%s", key);
        validator.validate(keyAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, HashMap<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }


}

