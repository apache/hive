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
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.metastore.tools.Util.filterMatches;

/**
 * Group of benchmarks that can be joined together.
 * Every benchmark has an associated name and code to run it.
 * It is possible to run all benchmarks or only ones matching the filter.<p>
 *
 * Results can be optionally sanitized - any result that is outside of
 * mean +/- margin * delta is removed from the result set. This helps remove random
 * outliers.
 *
 * <h1>Example</h1>
 *
 * <pre>
 *   StringBuilder sb = new StringBuilder();
 *   Formatter fmt = new Formatter(sb);
 *   BenchmarkSuite suite = new BenchmarkSuite();
 *      // Arrange various benchmarks in a suite
 *      BenchmarkSuite result = suite
 *           .setScale(scale)
 *           .doSanitize(true)
 *           .add("someBenchmark", someBenchmarkFunc)
 *           .add("anotherBenchmark", anotherBenchmarkFunc)
 *           .runMatching(patterns, exclude);
 *      result.display(fmt);
 * </pre>
 *
 */
public final class BenchmarkSuite {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkSuite.class);
  // Delta margin for data sanitizing. When sanitizing is enabled, we filter out
  // all result which are outside
  // mean +/- MARGIN * stddev
  private static final double MARGIN = 2;
  // Collection of benchmarks
  private final Map<String, Supplier<DescriptiveStatistics>> suite = new HashMap<>();
  // List of benchmarks. All benchmarks are executed in the order
  // they are inserted
  private final List<String> benchmarks = new ArrayList<>();
  // Once benchmarks are executed, results are stored in TreeMap to preserve the order.
  private final Map<String, DescriptiveStatistics> result = new TreeMap<>();
  // Whether sanitizing of results is requested
  private boolean doSanitize = false;
  // Time units - we use milliseconds.
  private TimeUnit scale = TimeUnit.MILLISECONDS;

  /**
   * Set scaling factor for displaying results.
   * When data is reported, all times are divided by scale functor.
   * Data is always collected in nanoseconds, so this can be used to present
   * data using different time units.
   * @param scale: scaling factor
   * @return this for chaining
   */
  public BenchmarkSuite setScale(TimeUnit scale) {
    this.scale = scale;
    return this;
  }

  /**
   * Enable or disable result sanitization.
   * This should be done before benchmarks are executed.
   * @param sanitize enable sanitization if true, disable if false
   * @return this object, allowing chained calls.
   */
  public BenchmarkSuite doSanitize(boolean sanitize) {
    this.doSanitize = sanitize;
    return this;
  }

  /**
   * Get raw benchmark results
   * @return map of benchmark name to the statistics describing the result
   */
  public Map<String, DescriptiveStatistics> getResult() {
    return result;
  }

  /**
     * Run all benchmarks in the 'names' list.
   * @param names list of benchmarks to run
   * @return this to allow chaining
   */
  private BenchmarkSuite runAll(List<String> names) {
    if (doSanitize) {
      names.forEach(name -> {
        LOG.info("Running benchmark {}", name);
        result.put(name, sanitize(suite.get(name).get()));
      });
    } else {
      names.forEach(name -> {
        LOG.info("Running benchmark {}", name);
        result.put(name, suite.get(name).get());
      });
    }
    return this;
  }

  /**
   * Return list of benchmark names that match positive patterns and do not
   * match negative patterns.
   * @param positive regexp patterns that should match benchmark name
   * @param negatve regexp patterns that should be excluded when matches
   * @return list of benchmark names
   */
  public List<String> listMatching(@Nullable Pattern[] positive,
                                   @Nullable Pattern[] negatve) {
    return filterMatches(benchmarks, positive, negatve);
  }

  /**
   * Run all benchmarks (filtered by positive and negative matches.
   * See {@link #listMatching(Pattern[], Pattern[])} for details.
   * @param positive regexp patterns that should match benchmark name
   * @param negatve regexp patterns that should be excluded when matches
   * @return this
   */
  public BenchmarkSuite runMatching(@Nullable Pattern[] positive,
                                    @Nullable Pattern[] negatve) {
    return runAll(filterMatches(benchmarks, positive, negatve));
  }

  /**
   * Add new benchmark to the suite.
   * @param name benchmark name
   * @param b benchmark corresponding to name
   * @return this
   */
  public BenchmarkSuite add(@NotNull String name, @NotNull Supplier<DescriptiveStatistics> b) {
    suite.put(name, b);
    benchmarks.add(name);
    return this;
  }

  /**
   * Get new statistics that excludes values beyond mean +/- 2 * stdev
   *
   * @param data Source data
   * @return new {@link @DescriptiveStatistics objects with sanitized data}
   */
  private static DescriptiveStatistics sanitize(@NotNull DescriptiveStatistics data) {
    double meanValue = data.getMean();
    double delta = MARGIN * meanValue;
    double minVal = meanValue - delta;
    double maxVal = meanValue + delta;
    return new DescriptiveStatistics(Arrays.stream(data.getValues())
        .filter(x -> x > minVal && x < maxVal)
        .toArray());
  }

  /**
   * Get median value for given statistics.
   * @param data collected datapoints.
   * @return median value.
   */
  private static double median(@NotNull DescriptiveStatistics data) {
    return new Median().evaluate(data.getValues());
  }

  /**
   * Produce printable result
   * @param fmt text formatter - destination of formatted results.
   * @param name benchmark name
   * @param stats benchmark data
   */
  private void displayStats(@NotNull Formatter fmt, @NotNull String name,
                            @NotNull DescriptiveStatistics stats) {
    double mean = stats.getMean();
    double err = stats.getStandardDeviation() / mean * 100;
    long conv = scale.toNanos(1);

    fmt.format("%-30s %-8.4g %-8.4g %-8.4g %-8.4g %-8.4g%n",
        name,
        mean / conv,
        median(stats) / conv,
        stats.getMin() / conv,
        stats.getMax() / conv,
        err);
  }

  /**
   * Produce results in printable CSV format, separated by separator.
   * @param fmt text formatter - destination of formatted results.
   * @param name benchmark name
   * @param stats benchmark data
   * @param separator field separator
   */
  private void displayCSV(@NotNull Formatter fmt, @NotNull String name,
                          @NotNull DescriptiveStatistics stats, @NotNull String separator) {
    double mean = stats.getMean();
    double err = stats.getStandardDeviation() / mean * 100;
    long conv = scale.toNanos(1);

    fmt.format("%s%s%g%s%g%s%g%s%g%s%g%n",
        name, separator,
        mean / conv, separator,
        median(stats) / conv, separator,
        stats.getMin() / conv, separator,
        stats.getMax() / conv, separator,
        err);
  }

  /**
   * Format all results
   * @param fmt text formatter - destination of formatted results.
   * @return this
   */
  BenchmarkSuite display(Formatter fmt) {
    fmt.format("%-30s %-8s %-8s %-8s %-8s %-8s%n",
        "Operation", "Mean", "Med", "Min", "Max", "Err%");
    result.forEach((name, stat) -> displayStats(fmt, name, stat));
    return this;
  }

  /**
   * Format all results in CSV format
   * @param fmt text formatter - destination of formatted results.
   * @param separator field separator
   * @return this
   */
  BenchmarkSuite displayCSV(Formatter fmt, String separator) {
    fmt.format("%s%s%s%s%s%s%s%s%s%s%s%n",
        "Operation", separator, "Mean", separator, "Med", separator, "Min",
        separator, "Max", separator, "Err%");
    result.forEach((name, s) -> displayCSV(fmt, name, s, separator));
    return this;
  }
}
