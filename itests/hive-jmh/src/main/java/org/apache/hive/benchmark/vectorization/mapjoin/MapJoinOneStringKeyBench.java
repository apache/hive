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
 *     java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.mapjoin.MapJoinOneStringKeyBench
 *
 *  {INNER, INNER_BIG_ONLY, LEFT_SEMI, OUTER}
 *    X
 *  {ROW_MODE_HASH_MAP, ROW_MODE_OPTIMIZED, VECTOR_PASS_THROUGH, NATIVE_VECTOR_OPTIMIZED, NATIVE_VECTOR_FAST}
 *
 */
@State(Scope.Benchmark)
public class MapJoinOneStringKeyBench extends AbstractMapJoin {

  public static class MapJoinOneStringKeyInnerRowModeHashMapBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneStringKeyInnerRowModeOptimized_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyInnerVectorPassThrough_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneStringKeyInnerNativeVectorOptimizedBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyInnerNativeVectorFastBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneStringKeyInnerBigOnlyRowModeHashMapBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneStringKeyInnerBigOnlyRowModeOptimized_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyInnerBigOnlyVectorPassThrough_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneStringKeyInnerBigOnlyNativeVectorOptimizedBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyInnerBigOnlyNativeVectorFastBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.INNER_BIG_ONLY, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneStringKeyLeftSemiRowModeHashMapBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneStringKeyLeftSemiRowModeOptimized_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyLeftSemiVectorPassThrough_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneStringKeyLeftSemiNativeVectorOptimizedBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyLeftSemiNativeVectorFastBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.LEFT_SEMI, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static class MapJoinOneStringKeyOuterRowModeHashMapBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.ROW_MODE_HASH_MAP);
    }
  }

  public static class MapJoinOneStringKeyOuterRowModeOptimized_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyOuterVectorPassThrough_Bench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.VECTOR_PASS_THROUGH);
    }
  }

  public static class MapJoinOneStringKeyOuterNativeVectorOptimizedBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.NATIVE_VECTOR_OPTIMIZED);
    }
  }

  public static class MapJoinOneStringKeyOuterNativeVectorFastBench extends MapJoinOneStringKeyBenchBase {

    @Setup
    public void setup() throws Exception {
      doSetup(VectorMapJoinVariation.OUTER, MapJoinTestImplementation.NATIVE_VECTOR_FAST);
    }
  }

  //-----------------------------------------------------------------------------------------------

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + MapJoinOneStringKeyBench.class.getSimpleName() + ".*")
        .build();
    new Runner(opt).run();
  }
}