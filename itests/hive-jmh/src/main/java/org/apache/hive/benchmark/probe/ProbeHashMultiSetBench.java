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

import org.apache.hadoop.hive.llap.io.probe.OrcProbeLongHashMultiSet;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeMultiKeyHashMultiSet;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeStringHashMultiSet;
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
 * This test measures the performance of probedecode MJ HashMultiSet filtering on LLAP.
 * <p/>
 * This test uses JMH framework for benchmarking.
 * You may execute this benchmark tool using JMH command line in different ways:
 * <p/>
 * To use the settings shown in the main() function, use:
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashMultiSetBench
 * <p/>
 * To use the default settings used by JMH, use:
 * $ java -jar -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashMultiSetBench
 * <p/>
 * To specify different parameters, use:
 * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
 * display the Average Time (avgt) in Microseconds (us)
 * - Benchmark mode. Available modes are:
 * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
 * - Output time unit. Available time units are: [m, s, ms, us, ns].
 * <p/>
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashMultiSetBench
 * -wi 10 -i 5 -f 2 -bm avgt -tu us
 */
@State(Scope.Benchmark)
public class ProbeHashMultiSetBench {

  public static class DummyLongProbeHashMultiSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getLongColumnVector();
      this.probeLongHashTable = new OrcDummyProbeLongHashMultiSet(getLongHashMultiSet((LongColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }


  public static class LongProbeHashMultiSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getLongColumnVector();
      this.probeLongHashTable = new OrcProbeLongHashMultiSet(getLongHashMultiSet((LongColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  // Baseline
  public static class DummyStringHashMultiSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getBytesColumnVector();
      this.probeLongHashTable = new OrcDummyProbeStringHashMultiSet(getBytesHashMultiSet((BytesColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  public static class StringProbeHashMultiSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getBytesColumnVector();
      this.probeLongHashTable = new OrcProbeStringHashMultiSet(getBytesHashMultiSet((BytesColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  public static class DummyMultiKeyProbeHashMultiSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getTimestampColumnVector();
      VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();
      vectorMapJoinInfo.setBigTableKeyColumnMap(new int[]{0});
      vectorMapJoinInfo.setBigTableKeyTypeInfos(new TypeInfo[]{TypeInfoFactory.timestampTypeInfo});
      this.probeLongHashTable = new OrcDummyProbeMultiKeyHashMultiSet(getTimestampHashMultiSet((TimestampColumnVector) filterColumnVector), vectorMapJoinInfo);
      this.probeLongHashTable.init();
    }
  }

  public static class MultiKeyProbeHashMultiSet extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getTimestampColumnVector();
      VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();
      vectorMapJoinInfo.setBigTableKeyColumnMap(new int[]{0});
      vectorMapJoinInfo.setBigTableKeyTypeInfos(new TypeInfo[]{TypeInfoFactory.timestampTypeInfo});
      this.probeLongHashTable = new OrcProbeMultiKeyHashMultiSet(getTimestampHashMultiSet((TimestampColumnVector) filterColumnVector), vectorMapJoinInfo);
      this.probeLongHashTable.init();
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + ProbeHashMultiSetBench.class.getSimpleName() + ".*")
        .build();
    new Runner(opt).run();
  }
//  Laptop Benchmark
//Benchmark                                                    (SELECT_PERCENT)  Mode  Cnt    Score    Error  Units
//  ProbeHashMultiSetBench.DummyLongProbeHashMultiSet.bench                  0.01  avgt    5    6.462 ±  1.297  ms/op
//  ProbeHashMultiSetBench.DummyLongProbeHashMultiSet.bench                   0.2  avgt    5    6.313 ±  0.940  ms/op
//  ProbeHashMultiSetBench.DummyMultiKeyProbeHashMultiSet.bench              0.01  avgt    5   91.979 ± 18.517  ms/op
//  ProbeHashMultiSetBench.DummyMultiKeyProbeHashMultiSet.bench               0.2  avgt    5   92.038 ± 17.364  ms/op
//  ProbeHashMultiSetBench.DummyStringHashMultiSet.bench                     0.01  avgt    5   10.672 ±  1.415  ms/op
//  ProbeHashMultiSetBench.DummyStringHashMultiSet.bench                      0.2  avgt    5   26.093 ±  0.593  ms/op
//  ProbeHashMultiSetBench.LongProbeHashMultiSet.bench                       0.01  avgt    5    6.546 ±  0.375  ms/op
//  ProbeHashMultiSetBench.LongProbeHashMultiSet.bench                        0.2  avgt    5    7.535 ±  0.149  ms/op
//  ProbeHashMultiSetBench.MultiKeyProbeHashMultiSet.bench                   0.01  avgt    5  105.387 ±  0.273  ms/op
//  ProbeHashMultiSetBench.MultiKeyProbeHashMultiSet.bench                    0.2  avgt    5  111.257 ±  0.281  ms/op
//  ProbeHashMultiSetBench.StringProbeHashMultiSet.bench                     0.01  avgt    5   16.362 ±  1.473  ms/op
//  ProbeHashMultiSetBench.StringProbeHashMultiSet.bench                      0.2  avgt    5   25.227 ±  1.324  ms/op
}
