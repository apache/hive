/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.benchmark.serde;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
 * java -cp target/benchmarks.jar org.apache.hive.benchmark.serde.HyperLogLogBench
 */
@State(Scope.Benchmark)
public class HyperLogLogBench {
  public static final int DEFAULT_ITER_TIME = 1000000;

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static abstract class Abstract {

    @Setup
    public abstract void setup();

    @Benchmark
    @Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    public void bench() {

    }
  }


  public abstract static class SizeOptimizedSparseStressN extends Abstract {

    private HyperLogLog hll;
    private final int stressN;
    private final int numIterations;

    public SizeOptimizedSparseStressN(int stressN) {
      this.stressN = stressN;
      numIterations = DEFAULT_ITER_TIME / stressN;
    }

    @Override
    public void setup() {
      hll = HyperLogLog.builder().setSizeOptimized().build();
    }

    @Override
    public void bench() {
      for (int i = 0; i < numIterations; i++) {
        for (int j = 0; j < stressN; j++) {
          hll.addInt(j);
        }
      }
    }

  }

  public static class SizeOptimizedSparseStress30 extends SizeOptimizedSparseStressN {
    public SizeOptimizedSparseStress30() {
      super(30);
    }
  }

  public static class SizeOptimizedSparseStress70 extends SizeOptimizedSparseStressN {
    public SizeOptimizedSparseStress70() {
      super(70);
    }
  }

  public static class SizeOptimizedSparseStressTminus10 extends SizeOptimizedSparseStressN {
    public SizeOptimizedSparseStressTminus10() {
      super(HyperLogLog.builder().setSizeOptimized().build().getEncodingSwitchThreshold()-10);
    }
  }

  public static class SizeOptimizedSparseStressTminus1 extends SizeOptimizedSparseStressN {
    public SizeOptimizedSparseStressTminus1() {
      super(HyperLogLog.builder().setSizeOptimized().build().getEncodingSwitchThreshold() - 1);
    }
  }

  public static class SizeOptimizedSparseStressT extends SizeOptimizedSparseStressN {
    public SizeOptimizedSparseStressT() {
      super(HyperLogLog.builder().setSizeOptimized().build().getEncodingSwitchThreshold());
    }
  }

  public static class SizeOptimizedDenseStress2T extends SizeOptimizedSparseStressN {
    public SizeOptimizedDenseStress2T() {
      super(2 * HyperLogLog.builder().setSizeOptimized().build().getEncodingSwitchThreshold());
    }
  }

  public static class SizeOptimizedDenseStress8T extends SizeOptimizedSparseStressN {
    public SizeOptimizedDenseStress8T() {
      super(8*HyperLogLog.builder().setSizeOptimized().build().getEncodingSwitchThreshold());
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + HyperLogLogBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}
