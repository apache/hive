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
package org.apache.hadoop.hive.metastore.tools;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Micro-benchmark some piece of code.<p>
 *
 * Every benchmark has three parts:
 * <ul>
 *   <li>Optional pre-test</li>
 *   <li>Mandatory test</lI>
 *   <li>Optional post-test</li>
 * </ul>
 * Measurement consists of the warm-up phase and measurement phase.
 * Consumer can specify number of times the warmup and measurement is repeated.<p>
 * All time is measured in nanoseconds.
 */
class MicroBenchmark {
  // Specify defaults
  private static final int WARMUP_DEFAULT = 15;
  private static final int ITERATIONS_DEFAULT = 100;
  private static final int SCALE_DEFAULT = 1;

  private final int warmup;
  private final int iterations;
  private final int scaleFactor;

  /**
   * Create default micro benchmark measurer
   */
  public MicroBenchmark() {
    this(WARMUP_DEFAULT, ITERATIONS_DEFAULT, SCALE_DEFAULT);
  }

  /**
   * Create micro benchmark measurer.
   * @param warmup number of test calls for warmup
   * @param iterations number of test calls for measurement
   */
  MicroBenchmark(int warmup, int iterations) {
    this(warmup, iterations, SCALE_DEFAULT);
  }

  /**
   * Create micro benchmark measurer.
   *
   * @param warmup number of test calls for warmup
   * @param iterations number of test calls for measurement
   * @param scaleFactor Every delta is divided by scale factor
   */
  private MicroBenchmark(int warmup, int iterations, int scaleFactor) {
    this.warmup = warmup;
    this.iterations = iterations;
    this.scaleFactor = scaleFactor;
  }

  /**
   * Run the benchmark and measure run-time statistics in nanoseconds.<p>
   * Before the run the warm-up phase is executed.
   * @param pre Optional pre-test setup
   * @param test Mandatory test
   * @param post Optional post-test cleanup
   * @return Statistics describing the results. All times are in nanoseconds.
   */
  public DescriptiveStatistics measure(@Nullable Runnable pre,
                                       @NotNull Runnable test,
                                       @Nullable Runnable post) {
    // Warmup phase
    for (int i = 0; i < warmup; i++) {
      if (pre != null) {
        pre.run();
      }
      test.run();
      if (post != null) {
        post.run();
      }
    }
    // Run the benchmark
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (int i = 0; i < iterations; i++) {
      if (pre != null) {
        pre.run();
      }
      long start = System.nanoTime();
      test.run();
      long end = System.nanoTime();
      stats.addValue((double)(end - start) / scaleFactor);
      if (post != null) {
        post.run();
      }
    }
    return stats;
  }

  /**
   * Run the benchmark and measure run-time statistics in nanoseconds.<p>
   * Before the run the warm-up phase is executed. No pre or post operations are executed.
   * @param test test to measure
   * @return Statistics describing the results. All times are in nanoseconds.
   */
  public DescriptiveStatistics measure(@NotNull Runnable test) {
    return measure(null, test, null);
  }
}
