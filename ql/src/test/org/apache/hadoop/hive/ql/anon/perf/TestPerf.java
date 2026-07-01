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

package org.apache.hadoop.hive.ql.anon.perf;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.IndexType;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.anonymize.RowAnonymizer;
import org.apache.hadoop.hive.ql.anon.builders.AlterIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateDataErasurePolicyStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateTableStatementBuilder;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverterFactory;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.hive.ql.anon.extract.ExtractorFactory;
import org.apache.hadoop.hive.ql.anon.index.IndexReader;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.tez.AnonContext;
import org.apache.hadoop.hive.ql.anon.tez.IndexReaderFactory;
import org.apache.hadoop.hive.ql.anon.tez.IndexResultConverter;
import org.apache.hadoop.hive.ql.anon.tez.OrcFileProcessor;
import org.apache.hadoop.hive.ql.anon.tez.RowContext;
import org.apache.hadoop.hive.ql.anon.tez.Stats;
import org.apache.hadoop.hive.ql.anon.utils.FileUtils;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.hive.ql.anon.verify.Verifier;
import org.apache.hadoop.hive.ql.anon.verify.VerifierFactory;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import static org.apache.hadoop.hive.ql.anon.TestUtils.WH_DIR;
import static org.apache.hadoop.hive.ql.anon.TestUtils.getTestPolicy;
import static org.apache.hadoop.hive.ql.anon.TestUtils.getTestPolicyDsl;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
public class TestPerf extends BaseTest {

  private final DataErasurePolicy policy = getTestPolicy();
  private RowAnonymizer anonymizer;
  private Extractor extractor;
  private RowContext rowContext;

  private static final String CSV_HEADER =
    "id,num_k,ratio,unique,f_len,ser_fmt,ix_type,time,anon_time,index_time,seek_time,run_num,error,total_msg,pii_msg,msg_per_usr,bw,nf,ft\n";
  private static final String SUMMARY_HEADER =
    "id,num_k,ratio,unique,f_len,ser_fmt,ix_type,reps,proc_med_ms,proc_min_ms,proc_max_ms,proc_iqr_ms,anon_med_ms,idx_med_ms,errors,total_msg,pii_msg,msg_per_usr,nf,ft\n";

  private static boolean perfEnabled() {
    return Boolean.getBoolean("perf.run");
  }

  public void gen(final TestContext ctx) throws CommandProcessorException, IOException {
    {
      createPolicy();
      createTable(ctx.getTableName(), ctx);
      setPolicy(ctx);

      for (int fix = 0; fix < ctx.numFiles; fix++) {
        Path tblFile = ctx.getTblFilePath()[fix];
        FileUtils.deleteIfExists(tblFile, conf);
        genData(tblFile, ctx, fix);
      }
    }

    {
      for (int i = 0; i < ctx.pageSize.length; i++) {
        createBtreeIndex(ctx.getTableName(), ctx.getBtIxName()[i], ctx.pageSize[i], ctx.bufferPoolSize);
        alterBtreeIndex(ctx.getTableName(), ctx.getBtIxName()[i]);
      }

      createDirectoryIndex(ctx.getTableName(), ctx.getDirIxName());
      alterDirectoryIndex(ctx.getTableName(), ctx.getDirIxName());

      createTabularIndex(ctx.getTableName(), ctx.getTabIxName());
      alterTabularIndex(ctx.getTableName(), ctx.getTabIxName());
    }
  }

  public void run(final TestContext ctx) throws CommandProcessorException, IOException {
    {
      conf.set(ANON_COLUMN_INTERNAL_FORMAT, ctx.internalFormat.name());

      conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, INDEX_HEADER_SIZE);
      conf.set(INDEX_ADDR_TYPE, ANON_INDEX_TEST_POINTER_TYPE);
      conf.set(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE);
      conf.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
      conf.setBoolean(ANON_SKIP_SWAP, true);

      final ConstCode code = Utils.getConstCode(ctx.internalFormat);
      anonymizer = new RowAnonymizer(conf, policy);
      extractor = ExtractorFactory.getExtractor(code);
      rowContext = new RowContext(0, 1, 2, IDENTITY_FIELD_NAME, ctx.internalFormat);
    }

    {
      LOG.info("Running perf tests for: {}", ctx.internalFormat.name());
      runBtree(ctx);
      runDirectory(ctx);
      runTabular(ctx);
      runNoIx(ctx);
    }
  }

  private void runBtree(final TestContext ctx) throws IOException {
    for (int i = 0; i < ctx.pageSize.length; i++) {
      conf.setInt(BTREE_CONF_BUFFER_SIZE, ctx.bufferPoolSize * ctx.pageSize[i]);
      conf.setInt(BTREE_CONF_PAGE_SIZE, ctx.pageSize[i]);
      conf.setInt(BTREE_CONF_DUMP_BYTES_PER_ROW, ctx.pageSize[i]);
      final int total = ctx.getNumWarmup() + ctx.getNumRepetitions();
      for (int r = 0; r < total; r++) {
        final Stats stats = new Stats();
        final MapWritable mw = indexLookup(IndexType.BTREE, ctx.getBtIxPath()[i].toString(), ctx, stats);
        perf2(ctx, mw, ctx.getOutAnonBt()[i][0], ctx.getBtRunners()[i], stats, r == 0);
        if (r >= ctx.getNumWarmup()) {
          stats.runNumber = r - ctx.getNumWarmup();
          ctx.addStats(stats);
        }
      }
    }
  }

  private void runDirectory(final TestContext ctx) throws IOException {
    final int total = ctx.getNumWarmup() + ctx.getNumRepetitions();
    for (int r = 0; r < total; r++) {
      final Stats stats = new Stats();
      final MapWritable mw = indexLookup(IndexType.DIRECTORY, ctx.getLkpIxPath().toString(), ctx, stats);
      perf2(ctx, mw, ctx.getOutAnonLkp()[0], "L", stats, r == 0);
      if (r >= ctx.getNumWarmup()) {
        stats.runNumber = r - ctx.getNumWarmup();
        ctx.addStats(stats);
      }
    }
  }

  private void runTabular(final TestContext ctx) throws IOException {
    final int total = ctx.getNumWarmup() + ctx.getNumRepetitions();
    for (int r = 0; r < total; r++) {
      final Stats stats = new Stats();
      final MapWritable mw = indexLookup(IndexType.TABULAR, ctx.getTabIxPath().toString(), ctx, stats);
      perf2(ctx, mw, ctx.getOutAnonTab()[0], "T", stats, r == 0);
      if (r >= ctx.getNumWarmup()) {
        stats.runNumber = r - ctx.getNumWarmup();
        ctx.addStats(stats);
      }
    }
  }

  private void runNoIx(final TestContext ctx) throws IOException {
    final Path[] paths = ctx.getTblFilePath();
    final int total = ctx.getNumWarmup() + ctx.getNumRepetitions();
    for (int r = 0; r < total; r++) {
      final Stats stats = new Stats();
      final OrcFileProcessor fileProcessor = new OrcFileProcessor(conf, extractor, anonymizer, rowContext, ctx.getKeys(), stats);
      for (int fix = 0; fix < ctx.numFiles; fix++) {
        FileUtils.deleteIfExists(ctx.getOutAnonNoIx()[fix], conf);
      }
      stats.start();
      for (int fix = 0; fix < ctx.numFiles; fix++) {
        final AnonContext anonContext = new AnonContext(paths[fix]);
        anonContext.setTmpPath(ctx.getOutAnonNoIx()[fix]);
        fileProcessor.processFile(anonContext);
      }
      stats.end();
      stats.runnerName = "N";
      stats.colFormat = ctx.internalFormat.name().substring(0, 1);
      if (r == 0) {
        try {
          for (int fix = 0; fix < ctx.numFiles; fix++) {
            verify(ctx.getOutAnonNoIx()[fix], ctx);
          }
        } catch (Exception e) {
          stats.error = 1;
          LOG.error(e.getMessage());
        }
      }
      for (int fix = 0; fix < ctx.numFiles; fix++) {
        cleanUp(ctx.getOutAnonNoIx()[fix]);
      }
      if (r >= ctx.getNumWarmup()) {
        stats.runNumber = r - ctx.getNumWarmup();
        ctx.addStats(stats);
      }
    }
  }

  private MapWritable indexLookup(final IndexType type, final String ixPath, final TestContext ctx, final Stats stats) throws IOException {
    final long t0 = System.nanoTime();
    final IndexReader reader = IndexReaderFactory.getIndexReader(type, conf, ixPath);
    final long tLoaded = System.nanoTime();
    final MapWritable mw = Utils.convert(IndexResultConverter.convert(reader, ctx.getKeys()));
    final long t1 = System.nanoTime();
    stats.indexTime = (t1 - t0) / 1_000_000L;
    stats.seekTime = (t1 - tLoaded) / 1_000_000L;
    if (reader instanceof Closeable) {
      ((Closeable) reader).close();
    }
    return mw;
  }

  private void perf2(final TestContext ctx, final MapWritable mw, final Path anonOutput, final String runner, final Stats stats, final boolean doVerify) throws IOException {
    final OrcFileProcessor fileProcessor = new OrcFileProcessor(conf, extractor, anonymizer, rowContext, ctx.getKeys(), stats);
    Assertions.assertNotEquals(0, mw.size());
    final List<Path> inPaths = new ArrayList<>();
    final List<Writable> values = new ArrayList<>();
    final List<Path> outPaths = new ArrayList<>();
    for (final Map.Entry<Writable, Writable> entry : mw.entrySet()) {
      final Path keyPath = new Path(entry.getKey().toString());
      final Path anonOutputFile = new Path(anonOutput.getParent(), keyPath.getName());
      FileUtils.deleteIfExists(anonOutputFile, conf);
      inPaths.add(keyPath);
      values.add(entry.getValue());
      outPaths.add(anonOutputFile);
    }
    stats.start();
    for (int k = 0; k < inPaths.size(); k++) {
      final AnonContext anonContext = new AnonContext(inPaths.get(k));
      anonContext.setTmpPath(outPaths.get(k));
      fileProcessor.processFile(anonContext, values.get(k));
    }
    stats.end();
    stats.runnerName = runner;
    stats.colFormat = ctx.internalFormat.name().substring(0, 1);
    if (doVerify) {
      try {
        for (final Path outPath : outPaths) {
          verify(outPath, ctx);
        }
      } catch (Exception e) {
        stats.error = 1;
        LOG.error(e.getMessage());
      }
    }
    for (final Path outPath : outPaths) {
      cleanUp(outPath);
    }
  }

  private void cleanUp(final Path path) throws IOException {
    LOG.info("Removing: {}", path);
    FileUtils.deleteIfExists(path, conf);
  }

  public void createTable(final String tableName, final TestContext context) throws CommandProcessorException {
    final String cmd = new CreateTableStatementBuilder(tableName, "m", "o", "b")
      .withInternalFormat(context.internalFormat)
      .withFileType(context.fileType)
      .withIfNotExists()
      .build();
    execute(cmd);
  }

  public void createPolicy() throws CommandProcessorException {
    final String cmd = new CreateDataErasurePolicyStatementBuilder()
      .withPolicyName("p")
      .withPolicySource(getTestPolicyDsl())
      .withIfNotExists()
      .build();
    execute(cmd);
    execute("VALIDATE ERASURE POLICY p");
    execute("ACTIVATE ERASURE POLICY p");
  }

  public void setPolicy(final TestContext ctx) throws CommandProcessorException {
    execute("ATTACH DATA ERASURE POLICY p ON TABLE " + ctx.getTableName() + " COLUMN b"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (" + ctx.internalFormat.name() + ") )"
        + " RESOLUTION ( EXPLICIT )");
  }

  public void createBtreeIndex(final String tableName, final String ixName, final int pageSize, final int bufferPoolSize) throws CommandProcessorException {
    String cmd = new CreateIndexStatementBuilder(ixName, tableName, "b").withBtreeOptions(pageSize, bufferPoolSize, "int").withIfNotExists().build();
    execute(cmd);
  }

  public void createDirectoryIndex(final String tableName, final String ixName) throws CommandProcessorException {
    String cmd = new CreateIndexStatementBuilder(ixName, tableName, "b").withDirectoryOptions("int").withIfNotExists().build();
    execute(cmd);
  }

  public void createTabularIndex(final String tableName, final String ixName) throws CommandProcessorException {
    String cmd = new CreateIndexStatementBuilder(ixName, tableName, "b").withTabularOptions().withIfNotExists().build();
    execute(cmd);
  }

  public void alterBtreeIndex(final String tableName, final String ixName) throws CommandProcessorException {
    final String cmd = new AlterIndexStatementBuilder(ixName, tableName).build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  public void alterDirectoryIndex(final String tableName, final String ixName) throws CommandProcessorException {
    final String cmd = new AlterIndexStatementBuilder(ixName, tableName).build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  public void alterTabularIndex(final String tableName, final String ixName) throws CommandProcessorException {
    final String cmd = new AlterIndexStatementBuilder(ixName, tableName).build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  private void genData(final Path tblFilePath, final TestContext ctx, final int fileIx) throws IOException {
    LOG.info("Generating {} for {}", tblFilePath, ctx.internalFormat.name());
    LOG.info("total: {}, pii ratio: {}, uniques: {}", ctx.totalMessages, ctx.allToPiiRatio, ctx.numUniqueUsers);

    final RowWriter rowWriter = RowWriterFactory.create(ctx, tblFilePath, conf);
    final FileGenerator fileGenerator = new FileGenScenario1();
    fileGenerator.genFile(rowWriter, ctx, fileIx);
    rowWriter.close();
  }

  private void verify(final Path path, final TestContext ctx) throws IOException {
    if (!ctx.verify) {
      LOG.warn("skipping verify");
      return;
    }
    LOG.info("Verifying {}", path);
    final Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    final StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();

    final BodyConverter converter = BodyConverterFactory.getBodyConverter(ctx.internalFormat);
    final Verifier verifier = VerifierFactory.createVerifier(ctx.internalFormat);

    final Set<WritableComparable> keySet = new HashSet<>(ctx.getKeys());
    final RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);

      final List<Object> rowValues = inputSOI.getStructFieldsDataAsList(rowIn);
      final WritableComparable msgId = (WritableComparable) rowValues.get(rowContext.getMsgIdIx());
      final Writable body = (Writable) rowValues.get(rowContext.getBodyIx());

      Object o = converter.convertBody(msgId, body);
      boolean ret = verifier.verify(o, keySet);
      Assertions.assertTrue(ret);
    }
    recordReader.close();
  }

  @Test
  public void testGen() throws CommandProcessorException, IOException {
    Assumptions.assumeTrue(perfEnabled(),
        "set -Dperf.run=true to generate the benchmark corpus");
    final int from = Integer.getInteger("perf.from", 1);
    final int to = Integer.getInteger("perf.to", Integer.MAX_VALUE);
    final List<TestParams> lst = TestParams.getLst();
    LOG.info("lst size: {} (gen cells [{}, {}])", lst.size(), from, to);
    int done = 0;
    for (final TestParams params : lst) {
      if (params.id < from || params.id > to) {
        continue;
      }
      final TestContext context = new TestContext(params);
      final int piiMSg = context.totalMessages / params.allToPiiRatio;
      final int msgPerUser = piiMSg / params.numUniqueUsers;
      Assertions.assertEquals(context.totalMessages, params.allToPiiRatio * msgPerUser * params.numUniqueUsers);
      LOG.info("test gen [{}]: {}", params.id, context);
      gen(context);
      done++;
    }
    LOG.info("gen done: {} cells in [{}, {}]", done, from, to);
  }

  @Test
  public void testGenAndPerf() throws Exception {
    Assumptions.assumeTrue(perfEnabled(),
        "set -Dperf.run=true to run the data-path microbenchmark");
    writePerf(TestParams.getLst(), true);
  }

  @Test
  public void testPerfOnly() throws Exception {
    Assumptions.assumeTrue(perfEnabled(),
        "set -Dperf.run=true to run the data-path microbenchmark");
    writePerf(TestParams.getLst(), false);
  }

  @Test
  public void testCleanWarehouse() throws IOException {
    Assumptions.assumeTrue(perfEnabled(),
        "set -Dperf.run=true to clean the benchmark warehouse");
    final FileSystem fs = new Path(WH_DIR).getFileSystem(conf);
    int removed = 0;
    for (final String glob : new String[]{"t_*", "ix__*", "anon_out_*"}) {
      final FileStatus[] sts = fs.globStatus(new Path(WH_DIR, glob));
      if (sts == null) {
        continue;
      }
      for (final FileStatus st : sts) {
        if (fs.delete(st.getPath(), true)) {
          removed++;
        }
      }
    }
    LOG.info("cleaned {} benchmark dirs under {}", removed, WH_DIR);
    System.out.println("PERF_CLEAN removed=" + removed + " under " + WH_DIR);
  }

  private void writePerf(final List<TestParams> lst, final boolean doGen) throws Exception {
    LOG.info("perf param-list size: {} (gen={})", lst.size(), doGen);
    final long ts = System.currentTimeMillis();
    final java.nio.file.Path outDir = Paths.get(System.getProperty("perf.out", "target"));
    Files.createDirectories(outDir);
    final java.nio.file.Path csv = outDir.resolve("microbench-perf-" + ts + ".csv");
    final java.nio.file.Path sum = outDir.resolve("microbench-perf-summary-" + ts + ".csv");
    try (final BufferedWriter writer = Files.newBufferedWriter(csv);
         final BufferedWriter summary = Files.newBufferedWriter(sum)) {
      writer.write(CSV_HEADER);
      summary.write(SUMMARY_HEADER);
      final int from = Integer.getInteger("perf.from", 1);
      final int to = Integer.getInteger("perf.to", Integer.MAX_VALUE);
      for (final TestParams params : lst) {
        if (params.id < from || params.id > to) {
          continue;
        }
        final TestContext context = new TestContext(params);
        LOG.info("perf{}: {}", doGen ? "+gen" : "", context);
        if (doGen) {
          gen(context);
        }
        run(context, writer, summary);
      }
    }
    System.out.println("PERF_PER_REP=" + csv.toAbsolutePath());
    System.out.println("PERF_SUMMARY=" + sum.toAbsolutePath());
  }

  public void testPerf() {
    conf = new HiveConf();
    final List<TestParams> lst = TestParams.getLst();
    LOG.info("perf param lst size: {}", lst.size());
    final long ts = System.currentTimeMillis();
    final java.nio.file.Path path = Paths.get("stats-" + ts + ".csv");
    final java.nio.file.Path sumPath = Paths.get("stats-summary-" + ts + ".csv");

    try (final BufferedWriter writer = Files.newBufferedWriter(path);
         final BufferedWriter summary = Files.newBufferedWriter(sumPath)) {
      writer.write("id,num_k,ratio,unique,f_len,ser_fmt,ix_type,time,anon_time,index_time,seek_time,run_num,error,total_msg,pii_msg,msg_per_usr,bw,nf,ft\n");
      summary.write("id,num_k,ratio,unique,f_len,ser_fmt,ix_type,reps,proc_med_ms,proc_min_ms,proc_max_ms,proc_iqr_ms,anon_med_ms,idx_med_ms,errors,total_msg,pii_msg,msg_per_usr,nf,ft\n");

      for (final TestParams params : lst) {
        final TestContext context = new TestContext(params);
        final int piiMSg = context.totalMessages / params.allToPiiRatio;
        final int msgPerUser = piiMSg / params.numUniqueUsers;
        Assertions.assertEquals(context.totalMessages, params.allToPiiRatio * msgPerUser * params.numUniqueUsers);
        LOG.info("test: {}", context);
        run(context, writer, summary);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      LOG.info("done; per-rep={} summary={}", path, sumPath);
    }
  }

  private void run(TestContext context, BufferedWriter writer, BufferedWriter summary) {
    try {
      run(context);
      final List<String> lines = new ArrayList<>();
      context.convertStats(lines);
      for (final String line : lines) {
        writer.write(line);
      }
      final List<String> sumLines = new ArrayList<>();
      context.convertSummary(sumLines);
      for (final String line : sumLines) {
        summary.write(line);
      }
      writer.flush();
      summary.flush();
    } catch (Exception e) {
      LOG.error("{}", e.getMessage());
    }
  }

  public static void main(String[] args) throws CommandProcessorException, IOException {
    TestPerf testPerf = new TestPerf();
    testPerf.testPerf();
  }
}
