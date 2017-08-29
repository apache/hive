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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazy.fast.StringToDouble;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
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
public class LazySimpleSerDeBench {
  /**
   * This test measures the performance for LazySimpleSerDe.
   * <p/>
   * This test uses JMH framework for benchmarking. You may execute this
   * benchmark tool using JMH command line in different ways:
   * <p/>
   * To run using default settings, use: 
   * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.serde.LazySimpleSerDeBench
   * <p/>
   */
  public static final int DEFAULT_ITER_TIME = 1000000;
  public static final int DEFAULT_DATA_SIZE = 4096;

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static abstract class AbstractDeserializer {

    public int[] offsets = new int[DEFAULT_DATA_SIZE];
    public int[] sizes = new int[DEFAULT_DATA_SIZE];
    protected final ByteArrayRef ref = new ByteArrayRef();

    @Setup
    public abstract void setup();

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    public void bench() {

    }
  }

  public static abstract class RandomDataInitializer extends
      AbstractDeserializer {

    final int width;

    public RandomDataInitializer(final int width) {
      this.width = width;
    }

    @Override
    public void setup() {
      int len = 0;
      Random r = new Random();
      for (int i = 0; i < sizes.length; i++) {
        sizes[i] = (int) (r.nextInt(width));
        offsets[i] = len;
        len += sizes[i];
      }
      byte[] data = new byte[len + 1];
      r.nextBytes(data);
      ref.setData(data);
    }
  }

  public static abstract class GoodDataInitializer extends AbstractDeserializer {

    public final int max;

    public GoodDataInitializer(final int max) {
      this.max = max;
    }

    @Override
    public void setup() {
      sizes = new int[1024];
      offsets = new int[sizes.length];
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Random r = new Random();
      int len = 0;
      for (int i = 0; i < sizes.length / 2; i++) {
        int p = r.nextInt(max);
        int n = -1 * (p - 1);
        byte[] ps = String.format("%d", p).getBytes();
        byte[] ns = String.format("%d", n).getBytes();
        sizes[2 * i] = ps.length;
        sizes[2 * i + 1] = ns.length;
        offsets[2 * i] = len;
        offsets[2 * i + 1] = len + ps.length;
        len += ps.length + ns.length;
        try {
          bos.write(ns);
          bos.write(ps);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
      ref.setData(bos.toByteArray());
    }
  }

  public static class RandomLazyByte extends RandomDataInitializer {

    public RandomLazyByte() {
      super(2);
    }

    final LazyByte obj = new LazyByte(
        LazyPrimitiveObjectInspectorFactory.LAZY_BYTE_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyByte extends RandomDataInitializer {

    public WorstLazyByte() {
      super(8);
    }

    final LazyByte obj = new LazyByte(
        LazyPrimitiveObjectInspectorFactory.LAZY_BYTE_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class GoodLazyByte extends GoodDataInitializer {

    final LazyByte obj = new LazyByte(
        LazyPrimitiveObjectInspectorFactory.LAZY_BYTE_OBJECT_INSPECTOR);

    public GoodLazyByte() {
      super(Integer.MAX_VALUE);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class RandomLazyShort extends RandomDataInitializer {

    public RandomLazyShort() {
      super(2);
    }

    final LazyShort obj = new LazyShort(
        LazyPrimitiveObjectInspectorFactory.LAZY_SHORT_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyShort extends RandomDataInitializer {

    public WorstLazyShort() {
      super(8);
    }

    final LazyShort obj = new LazyShort(
        LazyPrimitiveObjectInspectorFactory.LAZY_SHORT_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class GoodLazyShort extends GoodDataInitializer {

    final LazyShort obj = new LazyShort(
        LazyPrimitiveObjectInspectorFactory.LAZY_SHORT_OBJECT_INSPECTOR);

    public GoodLazyShort() {
      super(Integer.MAX_VALUE);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class RandomLazyInteger extends RandomDataInitializer {

    public RandomLazyInteger() {
      super(2);
    }

    final LazyInteger obj = new LazyInteger(
        LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyInteger extends RandomDataInitializer {

    public WorstLazyInteger() {
      super(8);
    }

    final LazyInteger obj = new LazyInteger(
        LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class GoodLazyInteger extends GoodDataInitializer {

    final LazyInteger obj = new LazyInteger(
        LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR);

    public GoodLazyInteger() {
      super(Integer.MAX_VALUE);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class RandomLazyFloat extends RandomDataInitializer {

    public RandomLazyFloat() {
      super(2);
    }

    final LazyFloat obj = new LazyFloat(
        LazyPrimitiveObjectInspectorFactory.LAZY_FLOAT_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyFloat extends RandomDataInitializer {

    public WorstLazyFloat() {
      super(8);
    }

    final LazyFloat obj = new LazyFloat(
        LazyPrimitiveObjectInspectorFactory.LAZY_FLOAT_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class GoodLazyFloat extends GoodDataInitializer {

    final LazyFloat obj = new LazyFloat(
        LazyPrimitiveObjectInspectorFactory.LAZY_FLOAT_OBJECT_INSPECTOR);

    public GoodLazyFloat() {
      super(Integer.MAX_VALUE);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class RandomLazyLong extends RandomDataInitializer {

    public RandomLazyLong() {
      super(2);
    }

    final LazyLong obj = new LazyLong(
        LazyPrimitiveObjectInspectorFactory.LAZY_LONG_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyLong extends RandomDataInitializer {

    public WorstLazyLong() {
      super(8);
    }

    final LazyLong obj = new LazyLong(
        LazyPrimitiveObjectInspectorFactory.LAZY_LONG_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class GoodLazyLong extends GoodDataInitializer {

    final LazyLong obj = new LazyLong(
        LazyPrimitiveObjectInspectorFactory.LAZY_LONG_OBJECT_INSPECTOR);

    public GoodLazyLong() {
      super(Integer.MAX_VALUE);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class RandomLazyDouble extends RandomDataInitializer {

    public RandomLazyDouble() {
      super(2);
    }

    final LazyDouble obj = new LazyDouble(
        LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyDouble extends RandomDataInitializer {

    public WorstLazyDouble() {
      super(8);
    }

    final LazyDouble obj = new LazyDouble(
        LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR);

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class GoodLazyDouble extends GoodDataInitializer {

    final LazyDouble obj = new LazyDouble(
        LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR);

    public GoodLazyDouble() {
      super(Integer.MAX_VALUE);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 4, time = 2, timeUnit = TimeUnit.MILLISECONDS)
  @Measurement(iterations = 4, time = 2, timeUnit = TimeUnit.MILLISECONDS)
  @State(Scope.Thread)
  public static class ParseDouble {
    byte[] bytes = "1234567890.12345".getBytes(StandardCharsets.UTF_8);

    @Benchmark
    public void floatingDecimalBench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        StringToDouble.strtod(bytes, 0, bytes.length);
      }
    }

    @Benchmark
    public void doubleBench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        Double.parseDouble(new String(bytes, 0, bytes.length, StandardCharsets.UTF_8));
      }
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class GoodLazyDate {

    final LazyDate obj = new LazyDate(
        LazyPrimitiveObjectInspectorFactory.LAZY_DATE_OBJECT_INSPECTOR);

    public int[] offsets = new int[DEFAULT_DATA_SIZE];
    public int[] sizes = new int[DEFAULT_DATA_SIZE];
    protected final ByteArrayRef ref = new ByteArrayRef();

    @Setup
    public void setup() {
      sizes = new int[DEFAULT_DATA_SIZE];
      offsets = new int[sizes.length];
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Random r = new Random();
      int len = 0;
      final long base = -320000000L*1000L; // 1959
      for (int i = 0; i < DEFAULT_DATA_SIZE; i++) {
        // -ve dates are also valid dates - the dates are within 1959 to 2027
        Date dt = new Date(base + (Math.abs(r.nextLong()) % (Integer.MAX_VALUE*1000L)));
        byte[] ds = dt.toString().getBytes();
        sizes[i] = ds.length;
        offsets[i] = len;
        len += ds.length;
        try {
          bos.write(ds);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
      ref.setData(bos.toByteArray());
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class RandomLazyDate extends RandomDataInitializer {

    final LazyDate obj = new LazyDate(
        LazyPrimitiveObjectInspectorFactory.LAZY_DATE_OBJECT_INSPECTOR);

    public RandomLazyDate() {
      super(4);
    }


    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyDate extends RandomDataInitializer {

    final LazyDate obj = new LazyDate(
        LazyPrimitiveObjectInspectorFactory.LAZY_DATE_OBJECT_INSPECTOR);

    public WorstLazyDate() {
      super(8);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }
  
  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class GoodLazyTimestamp {

    final LazyTimestamp obj = new LazyTimestamp(
        LazyPrimitiveObjectInspectorFactory.LAZY_TIMESTAMP_OBJECT_INSPECTOR);

    public int[] offsets = new int[DEFAULT_DATA_SIZE];
    public int[] sizes = new int[DEFAULT_DATA_SIZE];
    protected final ByteArrayRef ref = new ByteArrayRef();

    @Setup
    public void setup() {
      sizes = new int[DEFAULT_DATA_SIZE];
      offsets = new int[sizes.length];
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Random r = new Random();
      int len = 0;
      final long base = -320000000L * 1000L; // 1959
      for (int i = 0; i < DEFAULT_DATA_SIZE; i++) {
        // -ve dates are also valid Timestamps - dates are within 1959 to 2027
        Date dt = new Date(base + (Math.abs(r.nextLong()) % (Integer.MAX_VALUE * 1000L)));
        byte[] ds = String.format("%s 00:00:01", dt.toString()).getBytes();
        sizes[i] = ds.length;
        offsets[i] = len;
        len += ds.length;
        try {
          bos.write(ds);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
      ref.setData(bos.toByteArray());
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class RandomLazyTimestamp extends RandomDataInitializer {

    final LazyTimestamp obj = new LazyTimestamp(
        LazyPrimitiveObjectInspectorFactory.LAZY_TIMESTAMP_OBJECT_INSPECTOR);

    public RandomLazyTimestamp() {
      super(4);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static class WorstLazyTimestamp extends RandomDataInitializer {

    final LazyTimestamp obj = new LazyTimestamp(
        LazyPrimitiveObjectInspectorFactory.LAZY_TIMESTAMP_OBJECT_INSPECTOR);

    public WorstLazyTimestamp() {
      super(8);
    }

    @Override
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        obj.init(ref, offsets[i % sizes.length], sizes[i % sizes.length]);
      }
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(
        ".*" + LazySimpleSerDeBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}
