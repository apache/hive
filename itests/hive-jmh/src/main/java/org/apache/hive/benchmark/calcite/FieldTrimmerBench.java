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
package org.apache.hive.benchmark.calcite;

import com.google.common.collect.Lists;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRelFieldTrimmer;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This test measures the performance for field trimmer.
 * <p>
 * This test uses JMH framework for benchmarking.
 * You may execute this benchmark tool using JMH command line in different ways:
 * <p>
 * To use the settings shown in the main() function, use:
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.calcite.FieldTrimmerBench
 * <p>
 * To use the default settings used by JMH, use:
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.calcite.FieldTrimmerBench
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class FieldTrimmerBench {

  RelOptCluster relOptCluster;
  RelBuilder relBuilder;
  RelNode root;
  org.apache.calcite.sql2rel.RelFieldTrimmer cft;
  HiveRelFieldTrimmer ft;
  HiveRelFieldTrimmer hft;

  @Setup(Level.Trial)
  public void initTrial() {
    // Init cluster and builder
    final RelOptPlanner planner = CalcitePlanner.createPlanner(new HiveConf());
    final RexBuilder rexBuilder = new RexBuilder(
        new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
    relOptCluster = RelOptCluster.create(planner, rexBuilder);
    relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);
    // Create operator tree
    DummyNode0 d0 = new DummyNode0(relOptCluster, relOptCluster.traitSet());
    DummyNode1 d1 = new DummyNode1(relOptCluster, relOptCluster.traitSet());
    DummyNode2 d2 = new DummyNode2(relOptCluster, relOptCluster.traitSet());
    DummyNode3 d3 = new DummyNode3(relOptCluster, relOptCluster.traitSet());
    DummyNode4 d4 = new DummyNode4(relOptCluster, relOptCluster.traitSet(), d0);
    DummyNode5 d5 = new DummyNode5(relOptCluster, relOptCluster.traitSet(), d1);
    DummyNode6 d6 = new DummyNode6(relOptCluster, relOptCluster.traitSet(), d2);
    DummyNode7 d7 = new DummyNode7(relOptCluster, relOptCluster.traitSet(), d3);
    DummyNode8 d8 = new DummyNode8(relOptCluster, relOptCluster.traitSet(), d4, d5);
    DummyNode9 d9 = new DummyNode9(relOptCluster, relOptCluster.traitSet(), d6, d7);
    root = new DummyNode9(relOptCluster, relOptCluster.traitSet(), d8, d9);
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
  @Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
  public void baseRelFieldTrimmer() {
    // We initialize the field trimmer for every execution of the benchmark
    cft = new org.apache.calcite.sql2rel.RelFieldTrimmer(null, relBuilder);
    cft.trim(root);
    cft = null;
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
  @Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
  public void modBaseRelFieldTrimmer() {
    // We initialize the field trimmer for every execution of the benchmark
    ft = HiveRelFieldTrimmer.get(false, false);
    ft.trim(relBuilder, root);
    ft = null;
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
  @Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
  public void hiveRelFieldTrimmer() {
    // We initialize the field trimmer for every execution of the benchmark
    hft = HiveRelFieldTrimmer.get(false);
    hft.trim(relBuilder, root);
    hft = null;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + FieldTrimmerBench.class.getSimpleName() +
        ".*").build();
    new Runner(opt).run();
  }

  // ~ 10 rel node classes to use in the benchmark.

  private class DummyNode0 extends AbstractRelNode {
    protected DummyNode0(RelOptCluster cluster, RelTraitSet traits) {
      super(cluster, cluster.traitSet());
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode1 extends AbstractRelNode {
    protected DummyNode1(RelOptCluster cluster, RelTraitSet traits) {
      super(cluster, cluster.traitSet());
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode2 extends AbstractRelNode {
    protected DummyNode2(RelOptCluster cluster, RelTraitSet traits) {
      super(cluster, cluster.traitSet());
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode3 extends AbstractRelNode {
    protected DummyNode3(RelOptCluster cluster, RelTraitSet traits) {
      super(cluster, cluster.traitSet());
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode4 extends SingleRel {
    protected DummyNode4(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, cluster.traitSet(), input);
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode5 extends SingleRel {
    protected DummyNode5(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, cluster.traitSet(), input);
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode6 extends SingleRel {
    protected DummyNode6(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, cluster.traitSet(), input);
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode7 extends SingleRel {
    protected DummyNode7(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, cluster.traitSet(), input);
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode8 extends BiRel {
    protected DummyNode8(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right) {
      super(cluster, cluster.traitSet(), left, right);
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }

  private class DummyNode9 extends BiRel {
    protected DummyNode9(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right) {
      super(cluster, cluster.traitSet(), left, right);
    }

    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.newArrayList());
    }
  }
}
