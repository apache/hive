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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.builders.AlterIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateDataErasurePolicyStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.EraseStatementBuilder;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.apache.hadoop.hive.ql.anon.TestUtils.getTestPolicyDsl;
@Tag("perf")
public class TestStandaloneScale extends BaseTest {

  private static final int TOTAL_ROWS = Integer.parseInt(
      System.getProperty("anon.std.archive.rows", "10000000"));

  private static final int SUBJECT_DENSITY = 100;

  private static final int SCHEMA_ID = 3;

  private static final int SUBJECT_POOL_SIZE = TOTAL_ROWS / SUBJECT_DENSITY;

  private static final int[] BATCH_SIZES = {1, 10, 100, 1000, 10_000};

  private static final int MAX_BATCH = 10_000;

  private static final String RUN_TAG = String.valueOf(System.currentTimeMillis() / 1000);
  private static final String TABLE  = "tss_archive_" + RUN_TAG;
  private static final String POLICY = "tss_pol_"     + RUN_TAG;
  private static final String INDEX  = "tss_btree_"   + RUN_TAG;

  @BeforeAll
  public void setup() throws CommandProcessorException, IOException {
    Assumptions.assumeTrue(Boolean.getBoolean("perf.run"), "set -Dperf.run=true to run this benchmark");
    execute("CREATE TEMPORARY FUNCTION gen_anon_body"
        + " AS 'org.apache.hadoop.hive.ql.anon.udf.UDFGenAnonBody'");

    try { execute("DROP TABLE IF EXISTS " + TABLE); } catch (CommandProcessorException ignored) {}

    execute("CREATE TABLE " + TABLE + " (m INT, o BIGINT, b STRING) STORED AS ORC");
    bulkLoad();

    execute(new CreateDataErasurePolicyStatementBuilder()
        .withPolicyName(POLICY)
        .withPolicySource(getTestPolicyDsl())
        .withIfNotExists()
        .build());
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
    execute("ATTACH DATA ERASURE POLICY " + POLICY + " ON TABLE " + TABLE + " COLUMN b"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( EXPLICIT )");

    execute(new CreateIndexStatementBuilder(INDEX, TABLE, "b")
        .withBtreeOptions(8192, 10, "int")
        .withIfNotExists()
        .build());
    execute(new AlterIndexStatementBuilder(INDEX, TABLE).build());

    LOG.info("standalone-scale: archive provisioned, total_rows={}, table={}",
        TOTAL_ROWS, TABLE);
  }

  private void bulkLoad() throws CommandProcessorException {
    final int chunkLimit = Math.min(TOTAL_ROWS, 1_000_000);
    int remaining = TOTAL_ROWS;
    int offsetBase = 0;
    while (remaining > 0) {
      final int chunk = Math.min(remaining, chunkLimit);
      execute(
          "INSERT INTO " + TABLE + " SELECT"
          + " " + SCHEMA_ID + " AS m,"
          + " CAST(x.p * 1000 + y.p + " + offsetBase + " AS BIGINT) AS o,"
          + " gen_anon_body("
          +   "CASE WHEN (x.p * 1000 + y.p) % " + SUBJECT_DENSITY + " = 0"
          +   " THEN CAST(((x.p * 1000 + y.p + " + offsetBase
          +     ") / " + SUBJECT_DENSITY + ") + 1 AS INT)"
          +   " ELSE CAST(" + (SUBJECT_POOL_SIZE + 1_000_000)
          +     " + (x.p * 1000 + y.p + " + offsetBase + ") AS INT)"
          + " END) AS b"
          + " FROM"
          + " (SELECT p FROM (SELECT split(space(999), ' ') AS arr) tx"
          + "    LATERAL VIEW posexplode(arr) vx AS p, val) x"
          + " CROSS JOIN"
          + " (SELECT p FROM (SELECT split(space(999), ' ') AS arr) ty"
          + "    LATERAL VIEW posexplode(arr) vy AS p, val) y"
          + " LIMIT " + chunk);
      remaining  -= chunk;
      offsetBase += chunk;
    }
  }

  @Test
  public void runStandaloneSweep() throws CommandProcessorException, IOException {
    Assumptions.assumeTrue(Boolean.getBoolean("perf.run"), "set -Dperf.run=true to run this benchmark");
    final java.nio.file.Path outDir = Paths.get("target");
    Files.createDirectories(outDir);
    final java.nio.file.Path csv = outDir.resolve(
        "standalone-scale-" + System.currentTimeMillis() + ".csv");

    LOG.info("standalone-scale sweep: total_rows={} batch_sizes={}",
        TOTAL_ROWS, Arrays.toString(BATCH_SIZES));

    try (BufferedWriter w = Files.newBufferedWriter(csv)) {
      w.write("id,total_rows,batch_size,wall_time_ms,bytes_before,bytes_after,"
            + "files_before,files_after,files_rewritten,messages_per_file,error\n");

      int id = 0;
      int batchOffset = 0;
      for (final int batch : BATCH_SIZES) {
        if (batch > MAX_BATCH) {
          continue;
        }
        emit(w, id++, runBatch(batch, batchOffset));
        batchOffset += batch;
        w.flush();
      }
    }
    LOG.info("standalone-scale csv: {}", csv.toAbsolutePath());
  }

  private Sample runBatch(final int batchSize, final int batchOffset) {
    final Sample s = new Sample(batchSize);
    final int[] ids = new int[batchSize];
    final int step = Math.max(1, SUBJECT_POOL_SIZE / batchSize);
    for (int i = 0; i < batchSize; i++) {
      final int idx = (i * step + batchOffset) % SUBJECT_POOL_SIZE;
      ids[i] = idx + 1;
    }
    try {
      s.bytesBefore = warehouseBytes(TABLE);
      s.filesBefore = warehouseFileCount(TABLE);
      final long batchStartMs = System.currentTimeMillis();
      execute(new EraseStatementBuilder(TABLE).withIds(ids).build());
      s.wallTimeMs = System.currentTimeMillis() - batchStartMs;
      s.bytesAfter = warehouseBytes(TABLE);
      s.filesAfter = warehouseFileCount(TABLE);
      s.filesRewritten = filesRewrittenByMtime(TABLE, batchStartMs);
      if (s.filesRewritten > 0) {
        s.messagesPerFile = (double) batchSize / s.filesRewritten;
      }
    } catch (Exception e) {
      s.error = safeMsg(e);
    }
    return s;
  }

  private long warehouseBytes(final String tbl) throws IOException {
    final FileSystem fs = FileSystem.get((Configuration) conf);
    final Path p = new Path(TestUtils.WH_DIR, tbl.toLowerCase());
    if (!fs.exists(p)) {
      return 0L;
    }
    final ContentSummary cs = fs.getContentSummary(p);
    return cs.getLength();
  }

  private int warehouseFileCount(final String tbl) throws IOException {
    final FileSystem fs = FileSystem.get((Configuration) conf);
    final Path p = new Path(TestUtils.WH_DIR, tbl.toLowerCase());
    if (!fs.exists(p)) {
      return 0;
    }
    int count = 0;
    for (final FileStatus fst : fs.listStatus(p)) {
      if (fst.isFile() && !fst.getPath().getName().startsWith(".")) {
        count++;
      }
    }
    return count;
  }

  private int filesRewrittenByMtime(final String tbl, final long batchStartMs) throws IOException {
    final FileSystem fs = FileSystem.get((Configuration) conf);
    final Path p = new Path(TestUtils.WH_DIR, tbl.toLowerCase());
    if (!fs.exists(p)) {
      return 0;
    }
    int count = 0;
    for (final FileStatus fst : fs.listStatus(p)) {
      if (!fst.isFile()) continue;
      final String name = fst.getPath().getName();
      if (name.startsWith(".") || name.endsWith(".anon.bak") || name.endsWith(".anon.tmp")) continue;
      if (fst.getModificationTime() >= batchStartMs) {
        count++;
      }
    }
    return count;
  }

  private static String safeMsg(final Throwable t) {
    final String m = t.getMessage();
    if (m == null) return t.getClass().getSimpleName();
    final String first = m.split("\\R", 2)[0];
    return first.length() > 200 ? first.substring(0, 200) : first;
  }

  private static final class Sample {
    final int batchSize;
    long wallTimeMs;
    long bytesBefore;
    long bytesAfter;
    int filesBefore;
    int filesAfter;
    int filesRewritten;
    double messagesPerFile;
    String error;

    Sample(final int batchSize) { this.batchSize = batchSize; }
  }

  private static void emit(final BufferedWriter w, final int id, final Sample s) throws IOException {
    w.write(id + "," + TOTAL_ROWS + "," + s.batchSize + "," + s.wallTimeMs + ","
        + s.bytesBefore + "," + s.bytesAfter + ","
        + s.filesBefore + "," + s.filesAfter + ","
        + s.filesRewritten + "," + String.format("%.3f", s.messagesPerFile) + ","
        + (s.error == null ? "" : s.error.replace(',', ';')) + "\n");
  }
}
