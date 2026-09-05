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

package org.apache.hive.benchmark.vectorization.parquet;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedColumnReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.probe.ParquetProbeFilter;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmark that compares the pre-patch decode path (a bare 3-arg
 * {@code readBatch(total, col, type)} call — what {@code VectorizedParquetRecordReader.nextBatch}
 * would issue before HIVE-30019) against the post-patch filter-aware path
 * ({@code readBatch(total, col, type, ParquetProbeFilter)} with a supplied bitmap) across a
 * sweep of filter selectivities.
 *
 * <p>A single {@code @Benchmark} method ({@link #readBatch}) drives every projected column
 * through {@link #BATCHES_PER_INVOCATION} batches per invocation; the {@link #filter} @Param
 * chooses which shape to call:
 * <ul>
 *   <li><b>filter=none</b> — baseline, unfiltered decode via the 3-arg readBatch. Represents
 *       the reader's behaviour before HIVE-30019: every row materialises regardless of whether
 *       a downstream operator will drop it.</li>
 *   <li><b>filter=P</b> (integer pass-percentage) — filter-aware readBatch with a clumpy bitmap
 *       that accepts the first {@code P} rows of every 100-row block and rejects the rest. On
 *       dict-encoded pages the coalesced skip fast-path
 *       ({@code readDictionaryIDs} → {@code DictionaryValuesReader.skip(int)}
 *       → {@code RunLengthBitPackingHybridDecoder.skipInts}) drops each reject run in one call;
 *       on PLAIN pages each filtered row still costs a per-row {@code dataColumn.skip()} but
 *       never a value-materialise / null-set / setVal copy.</li>
 * </ul>
 *
 * <p>Two encodings are covered:
 * <ul>
 *   <li><b>dict</b> — small distinct-value set, so pages use dictionary encoding.</li>
 *   <li><b>plain</b> — every value distinct, so pages fall back to PLAIN.</li>
 * </ul>
 *
 * <p>Note on worst case: the current sweep uses a <i>clumpy</i> shape ({@code (i % 100) < P}),
 * which matches realistic ProbeDecode join-key hit distributions and is favourable to both the
 * dict bulk-skip and to branch prediction on PLAIN. An alternating half-filter
 * ({@code i % 2 == 0}) is the branch-predictor worst case but is not the shape ProbeDecode
 * produces in practice.
 *
 * <p>Run: {@code
 *   java -jar itests/hive-jmh/target/benchmarks.jar
 *        org.apache.hive.benchmark.vectorization.parquet.VectorizedParquetReadBench
 *        -wi 5 -i 15 -f 2 -bm avgt -tu us
 * }
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class VectorizedParquetReadBench {

  // Enough rows in a single row group that the fixed per-invocation cost -- reader open, split
  // init, checkEndOfRowGroup, JobConf lookup -- is amortised across many readBatch calls. With
  // 131072 rows (128 batches of 1024) written into one row group, each invocation drives ~128
  // filter-aware calls per column vs one, so the isFilteredOut / skip fast-path signal is no
  // longer diluted by the ~5 ms setup floor.
  private static final int N_ROWS = 131072;
  private static final int BATCHES_PER_INVOCATION = N_ROWS / VectorizedRowBatch.DEFAULT_SIZE;
  private static final MessageType WRITE_SCHEMA = MessageTypeParser.parseMessageType(
      "message pd_read { "
          + "required int32  int_col; "
          + "required int64  long_col; "
          + "required double dbl_col; "
          + "required binary str_col (UTF8); "
          + "}");

  @Param({"dict", "plain"})
  public String encoding;

  /**
   * Selectivity sweep. {@code none} = call the 3-arg readBatch (baseline / pre-patch shape);
   * every other value is an integer pass-percentage in {@code [0, 100]} used to build a bitmap
   * where the first {@code p} rows of every 100-row block accept and the remaining
   * {@code 100 - p} reject. Clumpy, not alternating: matches real-world join-key hit distributions
   * and lets the dict {@code readDictionaryIDs} bulk-skip coalesce the reject runs. {@code 50}
   * with a clumpy shape is not the same worst-case as an alternating half-filter (see the
   * "worst-case" note in the class javadoc).
   */
  @Param({"none", "10", "50", "90"})
  public String filter;

  /**
   * Toggles {@code hive.optimize.scan.probedecode.parquet.plain.filter.enabled}. When
   * {@code off}, the PLAIN-path {@code isFilteredOutPlain} check is constant-folded away by the
   * JIT and every row on a PLAIN page is materialised; the dictionary path is unaffected.
   * Included as a bench param so the JIT-elimination claim can be verified end-to-end (i.e. the
   * {@code plain × <any filter> × off} cells should match {@code plain × none} within noise).
   *
   * <p>Ignored when {@code filter=none}: the 3-arg readBatch path never calls the check
   * regardless of the config, so a {@code none × off} run is not informative and only widens
   * the sweep matrix.
   */
  @Param({"on", "off"})
  public String plainFilter;

  private File dataDir;
  private Path dataFile;
  private JobConf jobConf;

  private LongColumnVector intVec;
  private LongColumnVector longVec;
  private DoubleColumnVector dblVec;
  private BytesColumnVector strVec;

  private TypeInfo intType;
  private TypeInfo longType;
  private TypeInfo dblType;
  private TypeInfo strType;

  /**
   * Filter for this trial, built from the {@link #filter} @Param. {@code null} when
   * {@code filter=none}, in which case the bench calls the 3-arg readBatch (pre-patch shape).
   */
  private ParquetProbeFilter probeFilter;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    dataDir = Files.createTempDirectory("pd-read-").toFile();
    dataFile = new Path(new File(dataDir, "data.parquet").toURI());

    boolean dict = "dict".equals(encoding);

    Configuration writeConf = new Configuration();
    GroupWriteSupport.setSchema(WRITE_SCHEMA, writeConf);
    SimpleGroupFactory gf = new SimpleGroupFactory(WRITE_SCHEMA);
    // Row-group size: 256 MB so all N_ROWS live in a single row group. Page size: 1 MB so each
    // column still has several pages inside the row group (exercises page-boundary handling
    // inside readBatch). Dictionary size: 1 MB so 8 distinct values easily fit.
    try (ParquetWriter<Group> writer = new ParquetWriter<>(dataFile, new GroupWriteSupport(),
        CompressionCodecName.UNCOMPRESSED, 256 * 1024 * 1024, 1024 * 1024, 1024 * 1024,
        dict, false, ParquetWriter.DEFAULT_WRITER_VERSION, writeConf)) {
      for (int i = 0; i < N_ROWS; i++) {
        int intV = dict ? (i % 8) : i;
        long lngV = dict ? (i % 8) : (long) i;
        double dblV = dict ? (i % 8) : (double) i;
        String strV = dict ? ("v" + (i % 8)) : ("v" + i);
        writer.write(gf.newGroup()
            .append("int_col", intV)
            .append("long_col", lngV)
            .append("dbl_col", dblV)
            .append("str_col", Binary.fromString(strV)));
      }
    }

    jobConf = buildJobConf();

    int batchSize = VectorizedRowBatch.DEFAULT_SIZE;
    intVec = new LongColumnVector(batchSize);
    longVec = new LongColumnVector(batchSize);
    dblVec = new DoubleColumnVector(batchSize);
    strVec = new BytesColumnVector(batchSize);

    intType = TypeInfoFactory.getPrimitiveTypeInfo("int");
    longType = TypeInfoFactory.getPrimitiveTypeInfo("bigint");
    dblType = TypeInfoFactory.getPrimitiveTypeInfo("double");
    strType = TypeInfoFactory.getPrimitiveTypeInfo("string");

    probeFilter = buildFilter(filter, batchSize);
  }

  /**
   * Build a {@link ParquetProbeFilter} matching the {@link #filter} @Param, or {@code null} for
   * {@code "none"} (baseline, 3-arg readBatch).
   *
   * <p>Accept bits are clumpy: for pass-percentage {@code p}, the first {@code p} rows of each
   * 100-row block accept and the remaining {@code 100 - p} reject. Realistic ProbeDecode
   * hit-distributions come in runs, and this shape lets the dict {@code readDictionaryIDs}
   * bulk-skip coalesce a whole reject run into a single {@code skipInts} call. An alternating
   * half-filter would be the branch-predictor worst case but is not the shape ProbeDecode
   * produces in practice.
   */
  private static ParquetProbeFilter buildFilter(String mode, int batchSize) {
    if ("none".equals(mode)) {
      return null;
    }
    int pass = Integer.parseInt(mode);
    if (pass < 0 || pass > 100) {
      throw new IllegalArgumentException("filter pass % must be in [0, 100], got: " + mode);
    }
    boolean[] bits = new boolean[batchSize];
    for (int i = 0; i < batchSize; i++) {
      bits[i] = (i % 100) < pass;
    }
    return ParquetProbeFilter.newBitmap(bits);
  }

  private JobConf buildJobConf() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "int_col,long_col,dbl_col,str_col");
    conf.set(IOConstants.COLUMNS_TYPES, "int,bigint,double,string");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,1,2,3");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    HiveConf.setBoolVar(conf,
        HiveConf.ConfVars.HIVE_OPTIMIZE_SCAN_PROBEDECODE_PARQUET_PLAIN_FILTER,
        "on".equals(plainFilter));

    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
            .getStandardStructObjectInspector(
                java.util.Collections.<String>emptyList(),
                java.util.Collections.<org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector>emptyList()),
        new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    Utilities.setMapWork(conf, mapWork);

    Job job = new Job(conf, "pd-read");
    ParquetInputFormat.setInputPaths(job, dataFile);
    return new JobConf(conf);
  }

  private VectorizedParquetRecordReader openReader() throws Exception {
    Job job = new Job(jobConf, "pd-read-split");
    ParquetInputFormat.setInputPaths(job, dataFile);
    ParquetInputFormat<?> pif = new ParquetInputFormat<>(
        org.apache.parquet.hadoop.example.GroupReadSupport.class);
    org.apache.hadoop.mapreduce.InputSplit inputSplit = pif.getSplits(job).get(0);
    org.apache.hadoop.mapred.FileSplit fs = new org.apache.hadoop.mapred.FileSplit(dataFile, 0L,
        inputSplit.getLength(), inputSplit.getLocations());
    return new VectorizedParquetRecordReader(fs, jobConf);
  }

  @SuppressWarnings("unchecked")
  private VectorizedColumnReader[] primeReaders(VectorizedParquetRecordReader r) throws Exception {
    Method m = VectorizedParquetRecordReader.class.getDeclaredMethod("checkEndOfRowGroup");
    m.setAccessible(true);
    m.invoke(r);
    Field f = VectorizedParquetRecordReader.class.getDeclaredField("columnReaders");
    f.setAccessible(true);
    return (VectorizedColumnReader[]) f.get(r);
  }

  private void resetVectors() {
    intVec.reset();
    intVec.init();
    longVec.reset();
    longVec.init();
    dblVec.reset();
    dblVec.init();
    strVec.reset();
    strVec.init();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    File f = new File(dataFile.toUri());
    if (f.exists()) {
      f.delete();
    }
    if (dataDir != null && dataDir.exists()) {
      dataDir.delete();
    }
  }

  /**
   * Drive {@link VectorizedColumnReader#readBatch} for every projected column across all
   * {@link #BATCHES_PER_INVOCATION} batches in the row group. When {@link #probeFilter} is
   * {@code null} ({@code filter=none}), calls the 3-arg readBatch (pre-patch shape); otherwise
   * calls the 4-arg filter-aware readBatch. The branch is once per invocation, not per row --
   * JMH resolves it as a single conditional at the top and the loop bodies run without further
   * decisions.
   *
   * <p>The fixed per-invocation cost (openReader, split init, checkEndOfRowGroup, JobConf lookup
   * -- collectively ~5 ms in earlier runs) is amortised over ~128 readBatch calls per column so
   * the isFilteredOut / skip fast-path signal is not diluted by setup.
   */
  @Benchmark
  public void readBatch(Blackhole bh) throws Exception {
    try (VectorizedParquetRecordReader r = openReader()) {
      VectorizedColumnReader[] readers = primeReaders(r);
      int n = VectorizedRowBatch.DEFAULT_SIZE;
      if (probeFilter == null) {
        for (int b = 0; b < BATCHES_PER_INVOCATION; b++) {
          resetVectors();
          readers[0].readBatch(n, intVec, intType);
          readers[1].readBatch(n, longVec, longType);
          readers[2].readBatch(n, dblVec, dblType);
          readers[3].readBatch(n, strVec, strType);
          consume(bh);
        }
      } else {
        for (int b = 0; b < BATCHES_PER_INVOCATION; b++) {
          resetVectors();
          readers[0].readBatch(n, intVec, intType, probeFilter);
          readers[1].readBatch(n, longVec, longType, probeFilter);
          readers[2].readBatch(n, dblVec, dblType, probeFilter);
          readers[3].readBatch(n, strVec, strType, probeFilter);
          consume(bh);
        }
      }
    }
  }

  private void consume(Blackhole bh) {
    bh.consume(intVec.vector);
    bh.consume(longVec.vector);
    bh.consume(dblVec.vector);
    bh.consume(strVec.vector);
    bh.consume(intVec.isNull);
    bh.consume(longVec.isNull);
    bh.consume(dblVec.isNull);
    bh.consume(strVec.isNull);
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(VectorizedParquetReadBench.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
