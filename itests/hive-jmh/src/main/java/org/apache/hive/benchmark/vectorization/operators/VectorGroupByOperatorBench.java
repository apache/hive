/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.benchmark.vectorization.operators;

import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.benchmark.vectorization.ColumnVectorGenUtil;
import org.apache.orc.TypeDescription;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.LinuxPerfNormProfiler;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.google.common.collect.ImmutableList;

@State(Scope.Benchmark)
public class VectorGroupByOperatorBench extends AbstractOperatorBench {

  @Param({
    "true",
    "false"
  })
  private boolean hasNulls;

  @Param({
    "true",
    "false"
  })
  private boolean isRepeating;

  @Param({
    "PARTIAL1",
    "PARTIAL2",
    "FINAL",
    "COMPLETE"
  })
  private GenericUDAFEvaluator.Mode evalMode;

  @Param({
    "GLOBAL",
    "HASH"
  })
  private VectorGroupByDesc.ProcessingMode processMode;

  @Param({
    "count",
    "min",
    "max",
    "sum",
    "avg",
    "variance",
    "var_pop",
    "var_samp",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "bloom_filter"
  })
  private String aggregation;

  @Param({
    "bigint",
    "double",
    "string",
    "decimal(7,2)", // to use this via command line arg "decimal(7_2)"
    "decimal(38,18)", // to use this via command line arg "decimal(38_18)"
    "timestamp"
  })
  private String dataType;

  private Random rand = new Random(1234);
  private VectorGroupByOperator vgo;
  private VectorizedRowBatch vrb;
  private int size = VectorizedRowBatch.DEFAULT_SIZE;

  @Setup
  public void setup() {
    try {
      dataType = dataType.replaceAll("_", ",");
      TypeInfo typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(dataType);
      ColumnVector cv = ColumnVectorGenUtil.generateColumnVector(typeInfo, hasNulls, isRepeating, size, rand);
      TypeDescription typeDescription = TypeDescription.fromString(dataType);
      vrb = typeDescription.createRowBatch(size);
      vrb.size = size;
      vrb.cols[0] = cv;
      VectorizationContext ctx = new VectorizationContext("name", ImmutableList.of("A"));
      GroupByDesc desc = buildGroupByDescType(aggregation, evalMode, "A", typeInfo, processMode);
      Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(new CompilationOpContext(), desc);
      VectorGroupByDesc vectorGroupByDesc = new VectorGroupByDesc();
      vectorGroupByDesc.setProcessingMode(ProcessingMode.HASH);
      vgo = (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorGroupByDesc);
      vgo.initialize(new Configuration(), null);
    } catch (Exception e) {
      // likely unsupported combination of params
      // https://bugs.openjdk.java.net/browse/CODETOOLS-7901296 is not available yet to skip benchmark cleanly
      System.out.println("Skipping.. Exception: " + e.getMessage());
      System.exit(0);
    }
  }

  private GroupByDesc buildGroupByDescType(
    String aggregate,
    GenericUDAFEvaluator.Mode mode,
    String column,
    TypeInfo dataType,
    final VectorGroupByDesc.ProcessingMode processMode) throws SemanticException {

    AggregationDesc agg = buildAggregationDesc(aggregate, mode, column, dataType);
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");

    GroupByDesc desc = new GroupByDesc();
    desc.setVectorDesc(new VectorGroupByDesc());

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    ((VectorGroupByDesc) desc.getVectorDesc()).setProcessingMode(processMode);

    return desc;
  }

  private AggregationDesc buildAggregationDesc(
    String aggregate,
    GenericUDAFEvaluator.Mode mode,
    String column,
    TypeInfo typeInfo) throws SemanticException {

    ExprNodeDesc inputColumn = new ExprNodeColumnDesc(typeInfo, column, "table", false);

    ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
    params.add(inputColumn);

    AggregationDesc agg = new AggregationDesc();
    ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    GenericUDAFEvaluator genericUDAFEvaluator = FunctionRegistry.getGenericUDAFEvaluator(aggregate,
      ImmutableList.of(oi).asList(), false, false);
    agg.setGenericUDAFEvaluator(genericUDAFEvaluator);
    if (aggregate.equals("bloom_filter")) {
      GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator udafBloomFilterEvaluator =
        (GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator) agg.getGenericUDAFEvaluator();
      udafBloomFilterEvaluator.setHintEntries(10000);
    }
    agg.setGenericUDAFName(aggregate);
    agg.setMode(mode);
    agg.setParameters(params);

    return agg;
  }

  @TearDown
  public void tearDown() throws HiveException {
    vgo.close(false);
  }

  @Benchmark
  public void testAggCount() throws HiveException {
    vgo.process(vrb, 0);
  }

  /*
   * ============================== HOW TO RUN THIS TEST: ====================================
   *
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar VectorGroupByOperatorCountBench -prof perf     -f 1 (Linux)
   *    $ java -jar target/benchmarks.jar VectorGroupByOperatorCountBench -prof perfnorm -f 3 (Linux)
   *    $ java -jar target/benchmarks.jar VectorGroupByOperatorCountBench -prof perfasm  -f 1 (Linux)
   *    $ java -jar target/benchmarks.jar VectorGroupByOperatorCountBench -prof gc  -f 1 (allocation counting via gc)
   *    $ java -jar target/benchmarks.jar VectorGroupByOperatorBench -p hasNulls=true -p isRepeating=false -p aggregation=bloom_filter  -p processMode=HASH -p evalMode=PARTIAL1
   *    $ java -agentlib:jdwp=transport=dt_socket,address=127.0.0.1:6006,suspend=y,server=y -jar target/benchmarks.jar VectorGroupByOperatorBench
   */

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(VectorGroupByOperatorBench.class.getSimpleName())
      .addProfiler(LinuxPerfProfiler.class)
      .addProfiler(LinuxPerfNormProfiler.class)
      .addProfiler(LinuxPerfAsmProfiler.class)
      .build();
    new Runner(opt).run();
  }
}