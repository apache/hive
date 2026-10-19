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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.benchmark.ql.exec;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.BigRowInspector;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hive.benchmark.ql.exec.fory.PropertiesSerializer;
import org.apache.fory.Fory;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.Language;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluation-only benchmark comparing Apache Fory (formerly "Apache Fury") against Kryo (see
 * {@link KryoBench}) as a candidate replacement for Hive's plan serialization. Not wired into any production code path.
 */
@State(Scope.Benchmark)
public class FuryBench {
  private static final Logger LOG = LoggerFactory.getLogger(FuryBench.class);

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BaseBench {

    private Fory fory;
    private MapWork mapWork;
    private byte[] serialized;

    @Setup
    public void setup() throws Exception {
      // withRefTracking mirrors Kryo's setReferences(true) (SerializationUtilities.createNewKryo())
      // so the comparison is apples-to-apples on cyclic/shared-reference object graphs.
      fory = Fory.builder()
          .withLanguage(Language.JAVA)
          .withRefTracking(true)
          .requireClassRegistration(false)
          .build();

      mapWork = KryoBench.mockMapWork("my_table", 1000, new BigRowInspector());
      serialized = fory.serialize(mapWork);
      LOG.info("Fory-serialized MapWork payload size: {} bytes", serialized.length);
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public byte[] testSerializeMapWork() {
      return fory.serialize(mapWork);
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object testDeserializeMapWork() {
      return fory.deserialize(serialized);
    }
  }

  /**
   * Same payload as {@link BaseBench}, with Fory's {@code ignoreStringRef} default (true) flipped
   * to false. This is the headline fix: Fory does not ref-track/dedupe repeated String instances
   * by default even with {@code withRefTracking(true)} (that flag only covers non-String object
   * refs), so a string repeated across many sibling objects -- e.g. the input-format class name
   * on every per-partition {@code VectorPartitionDesc} in this benchmark's 1000-partition
   * MapWork -- gets written out in full on every occurrence instead of once.
   * Isolation measurements found this (not Properties/JDK-fallback as originally suspected, and not
   * CompatibleMode schema metadata) to be the single largest contributor to the Kryo-vs-Fory
   * payload-size gap. Zero new serializer classes needed for this one.
   */
  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BaseBenchWithStringRefTracking {

    private Fory fory;
    private MapWork mapWork;
    private byte[] serialized;

    @Setup
    public void setup() throws Exception {
      fory = Fory.builder()
          .withLanguage(Language.JAVA)
          .withRefTracking(true)
          .ignoreStringRef(false)
          .requireClassRegistration(false)
          .build();

      mapWork = KryoBench.mockMapWork("my_table", 1000, new BigRowInspector());
      serialized = fory.serialize(mapWork);
      LOG.info("Fory-serialized MapWork payload size (string ref-tracking): {} bytes", serialized.length);
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public byte[] testSerializeMapWork() {
      return fory.serialize(mapWork);
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object testDeserializeMapWork() {
      return fory.deserialize(serialized);
    }
  }

  /**
   * Combines the string-ref-tracking fix above with the custom {@link PropertiesSerializer} (for
   * the single shared TableDesc.properties instance, which still falls back to slow/bloated JDK
   * serialization without it) and {@link CompatibleMode#SCHEMA_CONSISTENT} (Fory's non-default,
   * Kryo-equivalent mode that skips writing per-object schema-evolution metadata) -- the best
   * combined configuration found in this investigation.
   */
  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BaseBenchWithCustomSerializers {

    private Fory fory;
    private MapWork mapWork;
    private byte[] serialized;

    @Setup
    public void setup() throws Exception {
      fory = Fory.builder()
          .withLanguage(Language.JAVA)
          .withRefTracking(true)
          .ignoreStringRef(false)
          .requireClassRegistration(false)
          .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
          .build();
      fory.registerSerializer(Properties.class, new PropertiesSerializer(fory.getTypeResolver()));

      mapWork = KryoBench.mockMapWork("my_table", 1000, new BigRowInspector());
      serialized = fory.serialize(mapWork);
      LOG.info("Fory-serialized MapWork payload size (combined fixes): {} bytes", serialized.length);
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public byte[] testSerializeMapWork() {
      return fory.serialize(mapWork);
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object testDeserializeMapWork() {
      return fory.deserialize(serialized);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder().include(".*" + FuryBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}
