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

import org.apache.hadoop.hive.llap.io.probe.OrcProbeLongHashMap;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeMultiKeyHashMap;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeStringHashMap;
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
 * This test measures the performance of probedecode MJ HashMap filtering on LLAP.
 * <p/>
 * This test uses JMH framework for benchmarking.
 * You may execute this benchmark tool using JMH command line in different ways:
 * <p/>
 * To use the settings shown in the main() function, use:
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashMapBench
 * <p/>
 * To use the default settings used by JMH, use:
 * $ java -jar -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashMapBench
 * <p/>
 * To specify different parameters, use:
 * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
 * display the Average Time (avgt) in Microseconds (us)
 * - Benchmark mode. Available modes are:
 * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
 * - Output time unit. Available time units are: [m, s, ms, us, ns].
 * <p/>
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.probe.ProbeHashMapBench
 * -wi 10 -i 5 -f 2 -bm avgt -tu us
 */
@State(Scope.Benchmark)
public class ProbeHashMapBench {

  public static class DummyLongProbeHashMap extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getLongColumnVector();
      this.probeLongHashTable = new OrcDummyProbeLongHashMap(getLongHashMap((LongColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }


  public static class LongProbeHashMap extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getLongColumnVector();
      this.probeLongHashTable = new OrcProbeLongHashMap(getLongHashMap((LongColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  // Baseline
  public static class DummyStringHashMap extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getBytesColumnVector();
      this.probeLongHashTable = new OrcDummyProbeStringHashMap(getBytesHashMap((BytesColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  public static class StringProbeHashMap extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getBytesColumnVector();
      this.probeLongHashTable = new OrcProbeStringHashMap(getBytesHashMap((BytesColumnVector) filterColumnVector), null);
      this.probeLongHashTable.init();
    }
  }

  public static class DummyMultiKeyProbeHashMap extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getTimestampColumnVector();
      VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();
      vectorMapJoinInfo.setBigTableKeyColumnMap(new int[]{0});
      vectorMapJoinInfo.setBigTableKeyTypeInfos(new TypeInfo[]{TypeInfoFactory.timestampTypeInfo});
      this.probeLongHashTable = new OrcDummyProbeMultiKeyHashMap(getTimestampHashMap((TimestampColumnVector) filterColumnVector), vectorMapJoinInfo);
      this.probeLongHashTable.init();
    }
  }

  public static class MultiKeyProbeHashMap extends AbstractProbeHashTableBench {
    @Override
    public void setup() throws HiveException, IOException {
      this.filterColumnVector = getTimestampColumnVector();
      VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();
      vectorMapJoinInfo.setBigTableKeyColumnMap(new int[]{0});
      vectorMapJoinInfo.setBigTableKeyTypeInfos(new TypeInfo[]{TypeInfoFactory.timestampTypeInfo});
      this.probeLongHashTable = new OrcProbeMultiKeyHashMap(getTimestampHashMap((TimestampColumnVector) filterColumnVector), vectorMapJoinInfo);
      this.probeLongHashTable.init();
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + ProbeHashMapBench.class.getSimpleName() + ".*")
        .build();
    new Runner(opt).run();
  }
//  Laptop Benchmark
//Benchmark                                          (SELECT_PERCENT)  Mode  Cnt    Score   Error  Units
//  ProbeHashMapBench.DummyLongProbeHashMap.bench                  0.01  avgt    2    6.723          ms/op
//  ProbeHashMapBench.DummyLongProbeHashMap.bench                   0.2  avgt    2    8.363          ms/op
//  ProbeHashMapBench.DummyMultiKeyProbeHashMap.bench              0.01  avgt    2  134.172          ms/op
//  ProbeHashMapBench.DummyMultiKeyProbeHashMap.bench               0.2  avgt    2  134.070          ms/op
//  ProbeHashMapBench.DummyStringHashMap.bench                     0.01  avgt    2   14.830          ms/op
//  ProbeHashMapBench.DummyStringHashMap.bench                      0.2  avgt    2   29.053          ms/op
//  ProbeHashMapBench.LongProbeHashMap.bench                       0.01  avgt    2    7.731          ms/op
//  ProbeHashMapBench.LongProbeHashMap.bench                        0.2  avgt    2    9.019          ms/op
//  ProbeHashMapBench.MultiKeyProbeHashMap.bench                   0.01  avgt    2  167.197          ms/op
//  ProbeHashMapBench.MultiKeyProbeHashMap.bench                    0.2  avgt    2  208.904          ms/op
//  ProbeHashMapBench.StringProbeHashMap.bench                     0.01  avgt    2   26.041          ms/op
//  ProbeHashMapBench.StringProbeHashMap.bench                      0.2  avgt    2   37.801          ms/op
}
