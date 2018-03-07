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
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.benchmark.vectorization.BlackholeOperator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.LinuxPerfNormProfiler;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class VectorSelectOperatorBench extends AbstractOperatorBench {

  private SelectDesc selDesc;
  private VectorSelectOperator vso;
  private VectorizedRowBatch vrg;
  private List<Operator<?>> child;
  private List<Operator<?>> EMPTY_CHILD = new ArrayList<>();

  @Setup
  public void setup(Blackhole bh) throws HiveException {
    HiveConf hconf = new HiveConf();
    List<String> columns = new ArrayList<String>();
    columns.add("a");
    columns.add("b");
    columns.add("c");
    VectorizationContext vc = new VectorizationContext("name", columns);

    selDesc = new SelectDesc(false);
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ExprNodeColumnDesc colDesc1 = new ExprNodeColumnDesc(Long.class, "a", "table", false);
    ExprNodeColumnDesc colDesc2 = new ExprNodeColumnDesc(Long.class, "b", "table", false);
    ExprNodeColumnDesc colDesc3 = new ExprNodeColumnDesc(Long.class, "c", "table", false);
    ExprNodeGenericFuncDesc plusDesc = new ExprNodeGenericFuncDesc();
    GenericUDF gudf = new GenericUDFOPPlus();

    plusDesc.setGenericUDF(gudf);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(colDesc1);
    children.add(colDesc2);
    plusDesc.setChildren(children);
    plusDesc.setTypeInfo(TypeInfoFactory.longTypeInfo);

    colList.add(plusDesc);
    colList.add(colDesc3);
    selDesc.setColList(colList);

    List<String> outputColNames = new ArrayList<String>();
    outputColNames.add("_col0");
    outputColNames.add("_col1");
    selDesc.setOutputColumnNames(outputColNames);

    VectorSelectDesc vectorSelectDesc = new VectorSelectDesc();
    selDesc.setVectorDesc(vectorSelectDesc);
    List<ExprNodeDesc> selectColList = selDesc.getColList();
    VectorExpression[] vectorSelectExprs = new VectorExpression[selectColList.size()];
    for (int i = 0; i < selectColList.size(); i++) {
      ExprNodeDesc expr = selectColList.get(i);
      VectorExpression ve = vc.getVectorExpression(expr);
      vectorSelectExprs[i] = ve;
    }
    vectorSelectDesc.setSelectExpressions(vectorSelectExprs);
    vectorSelectDesc.setProjectedOutputColumns(new int[]{3, 2});

    CompilationOpContext opContext = new CompilationOpContext();
    vso = new VectorSelectOperator(opContext, selDesc, vc, vectorSelectDesc);
    // to trigger vectorForward
    child = new ArrayList<>();
    child.add(new BlackholeOperator(opContext, bh));
    child.add(new BlackholeOperator(opContext, bh));
    vso.initialize(hconf, null);
    vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(
      VectorizedRowBatch.DEFAULT_SIZE, 4, 17);
  }

  @TearDown
  public void tearDown() throws HiveException {
    vso.close(false);
  }

  @Benchmark
  public void testSelectStar() throws HiveException {
    selDesc.setSelStarNoCompute(true);
    vso.process(vrg, 0);
  }

  @Benchmark
  public void testVectorSelectBaseForward() throws HiveException {
    selDesc.setSelStarNoCompute(false);
    vso.setChildOperators(EMPTY_CHILD);
    vso.process(vrg, 0);
  }

  @Benchmark
  public void testVectorSelectVectorForward() throws HiveException {
    selDesc.setSelStarNoCompute(false);
    vso.setChildOperators(child);
    vso.process(vrg, 0);
  }

  /*
   * ============================== HOW TO RUN THIS TEST: ====================================
   *
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar VectorSelectOperatorBench -prof perf     -f 1 (Linux)
   *    $ java -jar target/benchmarks.jar VectorSelectOperatorBench -prof perfnorm -f 3 (Linux)
   *    $ java -jar target/benchmarks.jar VectorSelectOperatorBench -prof perfasm  -f 1 (Linux)
   *    $ java -jar target/benchmarks.jar VectorSelectOperatorBench -prof gc  -f 1 (allocation counting via gc)
   */

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(VectorSelectOperatorBench.class.getSimpleName())
      .addProfiler(LinuxPerfProfiler.class)
      .addProfiler(LinuxPerfNormProfiler.class)
      .addProfiler(LinuxPerfAsmProfiler.class)
      .build();
    new Runner(opt).run();
  }
}