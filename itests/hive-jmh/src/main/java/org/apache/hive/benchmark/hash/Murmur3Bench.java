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
package org.apache.hive.benchmark.hash;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterStringColLikeStringScalar;
import org.apache.hive.common.util.Murmur3;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This test measures the performance for vectorization.
 * <p>
 * This test uses JMH framework for benchmarking.
 * You may execute this benchmark tool using JMH command line in different ways:
 * <p>
 * To use the settings shown in the main() function, use:
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.hash.Murmur3Bench
 * <p>
 * To use the default settings used by JMH, use:
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.hash.Murmur3Bench
 * <p>
 * To specify different parameters, use:
 * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
 * display the Average Time (avgt) in Microseconds (us)
 * - Benchmark mode. Available modes are:
 * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
 * - Output time unit. Available time units are: [m, s, ms, us, ns].
 * <p>
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.hash.Murmur3Bench
 * -wi 10 -i 5 -f 2 -bm avgt -tu us
 */
@State(Scope.Benchmark)
public class Murmur3Bench {
  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class Hash64Bench {
    
    @Param({ "-1"}) //"123456789", "987654321", "1234", "4321",
    long v;
    
    
    
    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public long longHash() {
      long k = 0;
      for (int i = 0; i < 4096; i++) {
        k += Murmur3.hash64(v);
      }
      return k;
    }
    
    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public long longBytesHash() {
      ByteBuffer LONG_BUFFER = ByteBuffer.allocate(Long.BYTES);
      long k = 0;
      for (int i = 0; i < 4096; i++) {
        LONG_BUFFER.putLong(0, v+i);
        k += Murmur3.hash64(LONG_BUFFER.array());
      }
      return k;
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + Murmur3Bench.class.getSimpleName() +
        ".*").build();
    new Runner(opt).run();
  }
}