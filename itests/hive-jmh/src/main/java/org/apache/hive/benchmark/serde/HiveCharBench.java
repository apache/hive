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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.io.Text;
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

@State(Scope.Benchmark)
public class HiveCharBench {

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BaseBench {

    private Text value;

    @Setup
    public void setup() throws Exception {
      value = new Text("    asdfghjk    ");
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testStrippedValueOld() {
      for (int i = 0; i < 10000; i++) {
        HiveChar hiveChar = new HiveChar(value.toString(), -1);
        new Text(StringUtils.stripEnd(hiveChar.getValue(), " "));
      }
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testStrippedValueNew() {
      for (int j = 0; j < 10000; j++) {
        byte[] input = value.getBytes();
        int i = input.length;
        while (i-- > 0 && (input[i] == 32 || input[i] == 0)) {
        }
        byte[] output = new byte[i + 1];
        System.arraycopy(input, 0, output, 0, i + 1);
        new Text(output);
      }
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder().include(".*" + HiveCharBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}
