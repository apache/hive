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

package org.apache.hive.benchmark.ql.exec;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.ExportService;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class TableAndPartitionExportBench {

  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.SingleShotTime)
  public static class BaseBench {

    protected static final Logger LOG = LoggerFactory.getLogger(BaseBench.class);
    final HiveConf conf = new HiveConf();
    final int nTables = 500;

    @Benchmark
    public void parallel() throws SemanticException, InterruptedException, HiveException {
      ExportService exportService = new ExportService(conf);
      try {
        for (int i = 0; i < nTables; i++) {
          new DumbTableExport().parallelWrite(exportService, true, null, true);
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
      try {
        exportService.waitForTasksToFinishAndShutdown();
      } catch (HiveException e) {
        throw new HiveException(e.getMessage());
      }
      try {
        exportService.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new InterruptedException(e.getMessage());
      }
    }

    @Benchmark
    public void serial() throws SemanticException, InterruptedException {
      try {
        for (int i = 0; i < nTables; i++) {
          new DumbTableExport().serialWrite(true, null, true);
        }
      } catch (SemanticException e) {
        throw new SemanticException(e);
      }
    }
  }

    public static void main(String[] args) throws RunnerException {
      Options opt =
          new OptionsBuilder().include(".*" + TableAndPartitionExportBench.class.getSimpleName() + ".*").build();
      new Runner(opt).run();
    }
}

