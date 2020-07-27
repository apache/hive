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

package org.apache.hive.benchmark.probe;

import org.apache.hadoop.hive.llap.io.probe.OrcProbeLongHashSet;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeMultiKeyHashSet;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeStringHashSet;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;

/**
 * This test measures the performance of probedecode MJ HashSet filtering on LLAP.
 * <p/>
 * This test uses JMH framework for benchmarking.
 * You may execute this benchmark tool using JMH command line in different ways:
 * <p/>
 * To use the settings shown in the main() function, use:
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashSetBench
 * <p/>
 * To use the default settings used by JMH, use:
 * $ java -jar -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashSetBench
 * <p/>
 * To specify different parameters, use:
 * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
 * display the Average Time (avgt) in Microseconds (us)
 * - Benchmark mode. Available modes are:
 * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
 * - Output time unit. Available time units are: [m, s, ms, us, ns].
 * <p/>
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashSetBench
 * -wi 10 -i 5 -f 2 -bm avgt -tu us
 */
@State(Scope.Benchmark)
public class ProbeHashSetBench {

  public static class DummyLongProbeHashSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getLongColumnVector();
      this.probeLongHashTable = new OrcDummyProbeLongHashSet(getLongHashSet((LongColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }


  public static class LongProbeHashSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getLongColumnVector();
      this.probeLongHashTable = new OrcProbeLongHashSet(getLongHashSet((LongColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  // Baseline
  public static class DummyStringHashSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getBytesColumnVector();
      this.probeLongHashTable = new OrcDummyProbeStringHashSet(getBytesHashSet((BytesColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  public static class StringProbeHashSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getBytesColumnVector();
      this.probeLongHashTable = new OrcProbeStringHashSet(getBytesHashSet((BytesColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  public static class DummyMultiKeyProbeHashSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getTimestampColumnVector();
      VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();
      vectorMapJoinInfo.setBigTableKeyColumnMap(new int[]{0});
      vectorMapJoinInfo.setBigTableKeyTypeInfos(new TypeInfo[]{TypeInfoFactory.timestampTypeInfo});
      this.probeLongHashTable = new OrcDummyProbeMultiKeyHashSet(getTimestampHashSet((TimestampColumnVector) filterColumnVector), vectorMapJoinInfo);
      this.probeLongHashTable.init();
    }
  }

  public static class MultiKeyProbeHashSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getTimestampColumnVector();
      VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();
      vectorMapJoinInfo.setBigTableKeyColumnMap(new int[]{0});
      vectorMapJoinInfo.setBigTableKeyTypeInfos(new TypeInfo[]{TypeInfoFactory.timestampTypeInfo});
      this.probeLongHashTable = new OrcProbeMultiKeyHashSet(getTimestampHashSet((TimestampColumnVector) filterColumnVector), vectorMapJoinInfo);
      this.probeLongHashTable.init();
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + ProbeHashSetBench.class.getSimpleName() + ".*")
        .build();
    new Runner(opt).run();
  }
//  Laptop Benchmark
//Benchmark                                          (SELECT_PERCENT)  Mode  Cnt    Score   Error  Units
//  ProbeHashSetBench.DummyLongProbeHashSet.bench                  0.01  avgt    2    6.348          ms/op
//  ProbeHashSetBench.DummyLongProbeHashSet.bench                   0.2  avgt    2    7.287          ms/op
//  ProbeHashSetBench.DummyMultiKeyProbeHashSet.bench              0.01  avgt    2  133.973          ms/op
//  ProbeHashSetBench.DummyMultiKeyProbeHashSet.bench               0.2  avgt    2  134.353          ms/op
//  ProbeHashSetBench.DummyStringHashSet.bench                     0.01  avgt    2   14.442          ms/op
//  ProbeHashSetBench.DummyStringHashSet.bench                      0.2  avgt    2   25.385          ms/op
//  ProbeHashSetBench.LongProbeHashSet.bench                       0.01  avgt    2    7.714          ms/op
//  ProbeHashSetBench.LongProbeHashSet.bench                        0.2  avgt    2    9.184          ms/op
//  ProbeHashSetBench.MultiKeyProbeHashSet.bench                   0.01  avgt    2  166.786          ms/op
//  ProbeHashSetBench.MultiKeyProbeHashSet.bench                    0.2  avgt    2  225.394          ms/op
//  ProbeHashSetBench.StringProbeHashSet.bench                     0.01  avgt    2   23.602          ms/op
//  ProbeHashSetBench.StringProbeHashSet.bench                      0.2  avgt    2   33.431          ms/op
}
