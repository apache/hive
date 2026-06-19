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
package org.apache.hive.benchmark.vectorization.mapjoin.load;

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/*
 * Build with "mvn clean install -DskipTests -Pperf" at main itests directory.
 * From itests/hive-jmh directory, run:
 *     java -jar -Xmx8g -Xms8g target/benchmarks.jar org.apache.hive.benchmark.vectorization.mapjoin.load.VectorFastHTBytesKeyBench
 *
 *  {HASH_MAP, HASH_SET, HASH_MULTISET}
 *    X
 *  {NATIVE_VECTOR_FAST}
 */
public class VectorFastHTBytesKeyBench {

  public static class FastHashVectorBench extends BytesKeyBase {
    @Setup
    public void setup() throws Exception {
      LOG.info("Do Setup");
      doSetup(VectorMapJoinVariation.valueOf(JOIN_TYPE), MapJoinTestImplementation.NATIVE_VECTOR_FAST, ROWS_NUM);
    }

    @TearDown(Level.Invocation)
    public void doTearDown() {
      LOG.info("Do TearDown");
      customKeyValueReader.reset();
    }
  }

  /**
   * For output log check: itests/target/tmp/log/hive-jmh.log
   */
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + VectorFastHTBytesKeyBench.class.getSimpleName() + ".*")
        .warmupIterations(4) // number of times the warmup iteration should take place
        .measurementIterations(4) //number of times the actual iteration should take place
        .forks(1)
        .jvmArgs("-Xmx8g", "-Xms8g", "-XX:MaxDirectMemorySize=512M")
        .build();
    new Runner(opt).run();
  }
}
