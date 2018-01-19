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

package org.apache.hive.benchmark.vectorization.mapjoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
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
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType.GenerateCategory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColAndCol;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestData;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription;
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
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.benchmark.vectorization.VectorizedArithmeticBench;
import org.apache.hive.common.util.HashCodeUtil;
import org.apache.hive.common.util.ReflectionUtil;
import org.junit.Test;

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

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.profile.StackProfiler;

/*
 * Simple one long key map join benchmarks.
 *
 * Build with "mvn clean install -DskipTests -Pdist,itests" at main hive directory.
 *
 * From itests/hive-jmh directory, run:
 *     java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.mapjoin.MapJoinOneLongKeyBench
 *
 *  {INNER, INNER_BIG_ONLY, LEFT_SEMI, OUTER}
 *    X
 *  {ROW_MODE_HASH_MAP, ROW_MODE_OPTIMIZED, VECTOR_PASS_THROUGH, NATIVE_VECTOR_OPTIMIZED, NATIVE_VECTOR_FAST}
 *
 */
@State(Scope.Benchmark)
public class MapJoinOneLongKeyBench extends AbstractMapJoin {

  public static class MapJoinOneLongKeyInnerRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyInnerRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyInnerNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneLongKeyInnerBigOnlyRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneLongKeyLeftSemiRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneLongKeyOuterRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyOuterRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyOuterVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyOuterNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyOuterNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + MapJoinOneLongKeyBench.class.getSimpleName() + ".*")
        .build();
    new Runner(opt).run();
  }
}