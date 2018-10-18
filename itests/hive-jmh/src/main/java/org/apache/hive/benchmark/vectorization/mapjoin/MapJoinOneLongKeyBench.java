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

package org.apache.hive.benchmark.vectorization.mapjoin;

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/*
 * Simple one long key map join benchmarks.
 *
 * Build with "mvn clean install -DskipTests -Pdist,itests" at main hive directory.
 *
 * From itests/hive-jmh directory, run:
 *     java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.mapjoin.MapJoinOneLongKeyBench
 *
 *  {INNER, INNER_BIG_ONLY, LEFT_SEMI, OUTER}
 *    X
 *  {ROW_MODE_HASH_MAP, ROW_MODE_OPTIMIZED, VECTOR_PASS_THROUGH, NATIVE_VECTOR_OPTIMIZED, NATIVE_VECTOR_FAST}
 *
 */
@State(Scope.Benchmark)
public class MapJoinOneLongKeyBench extends AbstractMapJoin {

  public static class MapJoinOneLongKeyInnerRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyInnerRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyInnerNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneLongKeyInnerBigOnlyRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyInnerBigOnlyNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneLongKeyLeftSemiRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyLeftSemiNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneLongKeyOuterRowModeHashMapBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneLongKeyOuterRowModeOptimized_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyOuterVectorPassThrough_Bench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneLongKeyOuterNativeVectorOptimizedBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneLongKeyOuterNativeVectorFastBench extends MapJoinOneLongKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + MapJoinOneLongKeyBench.class.getSimpleName() + ".*")
        .build();
    new Runner(opt).run();
  }
}