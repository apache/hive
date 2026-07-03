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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.builders.CreateDataErasurePolicyStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.EraseStatementBuilder;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
@Tag("perf")
public class TestErasureVsAcidUpdate extends BaseTest {

  private static final int TOTAL_ROWS = Integer.parseInt(
      System.getProperty("anon.acid.compare.rows", "1000000"));

  private static final int SUBJECT_PERIOD = 100;
  private static final int SUBJECT_ID = 1;

  private static final int SCHEMA_ID_MSG3 = 3;

  private static final String POLICY_NAME = "tea_msg3_policy";

  private static final String POLICY_ERP =
        """
        VERSION v1
        IDENTITY userId TYPE INT
        SCHEMA TYPE INT

        FOR SCHEMA 3
            ERASE telephone, country
        """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY_NAME };
  }

  @BeforeAll
  public void enableAcid() throws Exception {
    try {
      if (driver != null) driver.close();
    } catch (Exception ignored) {  }

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.set("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 1);
    conf.set("hive.compactor.check.interval", "2s");

    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);

    sessionState = SessionState.start(conf);
    driver = new Driver(conf);
    SessionState.get().initTxnMgr(conf);
    if (!(SessionState.get().getTxnMgr() instanceof DbTxnManager)) {
      throw new IllegalStateException(
          "expected DbTxnManager after initTxnMgr but got " + SessionState.get().getTxnMgr());
    }

    execute("CREATE TEMPORARY FUNCTION gen_anon_body"
        + " AS 'org.apache.hadoop.hive.ql.anon.udf.UDFGenAnonBody'");
    execute("CREATE TEMPORARY FUNCTION anonymize_body"
        + " AS 'org.apache.hadoop.hive.ql.anon.udf.UDFAnonymizeBody'");

    execute(new CreateDataErasurePolicyStatementBuilder()
        .withPolicyName(POLICY_NAME)
        .withPolicySource(POLICY_ERP)
        .withIfNotExists()
        .build());

    execute("VALIDATE ERASURE POLICY " + POLICY_NAME);
    execute("ACTIVATE ERASURE POLICY " + POLICY_NAME);
  }

  @Test
  public void compareErasureVsAcidUpdate() throws IOException {
    Assumptions.assumeTrue(Boolean.getBoolean("perf.run"), "set -Dperf.run=true to run this benchmark");
    final java.nio.file.Path outDir = Paths.get("target");
    Files.createDirectories(outDir);
    final java.nio.file.Path csv = outDir.resolve(
        "update-compare-acid-" + System.currentTimeMillis() + ".csv");

    LOG.info("acid compare: total_rows={} subject_period={} subject_id={}",
        TOTAL_ROWS, SUBJECT_PERIOD, SUBJECT_ID);

    try (BufferedWriter w = Files.newBufferedWriter(csv)) {
      w.write("id,mode,subject_count,table_rows,t_conform_ms,bytes_after,"
            + "read_lat_ms,order_preserved,run_num,error\n");
      int id = 0;
      emit(w, id++, runErase(SUBJECT_ID,  0));
      w.flush();
      emit(w, id++, runUpdateOnly(SUBJECT_ID,  0));
      w.flush();
      emit(w, id++, runUpdatePlusCompact(SUBJECT_ID,  0));
      w.flush();
    }
    LOG.info("acid update-compare csv: {}", csv.toAbsolutePath());
  }

  private void bulkLoad(final String tbl, final int subjectId) throws CommandProcessorException {
    final int outerLimit = Math.min(TOTAL_ROWS, 1_000_000);
    execute(
        "INSERT INTO " + tbl
        + " SELECT"
        +   " " + SCHEMA_ID_MSG3 + " AS m,"
        +   " CAST(x.p * 1000 + y.p AS BIGINT) AS o,"
        +   " gen_anon_body("
        +     "CASE WHEN (x.p * 1000 + y.p) % " + SUBJECT_PERIOD + " = 0 THEN " + subjectId
        +     " ELSE " + (subjectId + 1) + " END) AS b"
        + " FROM"
        +   " (SELECT p FROM (SELECT split(space(999), ' ') AS arr) tx"
        +   "    LATERAL VIEW posexplode(arr) vx AS p, val) x"
        + " CROSS JOIN"
        +   " (SELECT p FROM (SELECT split(space(999), ' ') AS arr) ty"
        +   "    LATERAL VIEW posexplode(arr) vy AS p, val) y"
        + " LIMIT " + outerLimit);
  }

  private void attachPolicy(final String tbl) throws CommandProcessorException {
    execute("ATTACH DATA ERASURE POLICY " + POLICY_NAME + " ON TABLE " + tbl + " COLUMN b"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( EXPLICIT )");
  }

  private Sample runErase(final int subjectId, final int run) {
    final Sample s = new Sample("ERASE", subjectId, run);
    s.tableRows = TOTAL_ROWS;
    s.orderPreserved = true;
    final String tbl = "ts_erase_" + System.nanoTime();
    try {
      execute("CREATE TABLE " + tbl + " (m INT, o BIGINT, b STRING) STORED AS ORC");
      bulkLoad(tbl, subjectId);
      attachPolicy(tbl);
      final long t0 = System.currentTimeMillis();
      execute(new EraseStatementBuilder(tbl).withIds(subjectId).build());
      s.tConformMs = System.currentTimeMillis() - t0;
      s.bytesAfter = warehouseBytes(tbl);
      s.readLatencyMs = timeSelect(tbl);
    } catch (Exception e) {
      s.error = "erase: " + safeMsg(e);
    } finally {
      tryDrop(tbl);
    }
    return s;
  }

  private Sample runUpdateOnly(final int subjectId, final int run) {
    final Sample s = new Sample("UPDATE_ONLY", subjectId, run);
    s.tableRows = TOTAL_ROWS;
    final String tbl = "ts_upd_" + System.nanoTime();
    try {
      execute("CREATE TABLE " + tbl + " (m INT, o BIGINT, b STRING) "
            + "STORED AS ORC TBLPROPERTIES ('transactional'='true')");
      bulkLoad(tbl, subjectId);
      final long t0 = System.currentTimeMillis();
      execute("UPDATE " + tbl
          + " SET b = anonymize_body(b, " + subjectId + ")"
          + " WHERE m = " + SCHEMA_ID_MSG3);
      final long updMs = System.currentTimeMillis() - t0;
      s.tConformMs = Long.MAX_VALUE;
      s.bytesAfter = warehouseBytes(tbl);
      s.readLatencyMs = timeSelect(tbl);
      s.orderPreserved = false;
      s.error = "update_only_t_us=" + updMs;
    } catch (Exception e) {
      s.error = "skipped: " + safeMsg(e);
    } finally {
      tryDrop(tbl);
    }
    return s;
  }

  private Sample runUpdatePlusCompact(final int subjectId, final int run) {
    final Sample s = new Sample("UPDATE_PLUS_COMPACT", subjectId, run);
    s.tableRows = TOTAL_ROWS;
    final String tbl = "ts_uc_" + System.nanoTime();
    try {
      execute("CREATE TABLE " + tbl + " (m INT, o BIGINT, b STRING) "
            + "STORED AS ORC TBLPROPERTIES ('transactional'='true')");
      bulkLoad(tbl, subjectId);
      final long t0 = System.currentTimeMillis();
      execute("UPDATE " + tbl
          + " SET b = anonymize_body(b, " + subjectId + ")"
          + " WHERE m = " + SCHEMA_ID_MSG3);
      execute("ALTER TABLE " + tbl + " COMPACT 'major'");
      s.tConformMs = System.currentTimeMillis() - t0;
      s.bytesAfter = warehouseBytes(tbl);
      s.readLatencyMs = timeSelect(tbl);
      s.orderPreserved = false;
      s.error = "compaction_submitted_non_blocking";
    } catch (Exception e) {
      s.error = "skipped: " + safeMsg(e);
    } finally {
      tryDrop(tbl);
    }
    return s;
  }

  private long timeSelect(final String tbl) throws CommandProcessorException {
    final long t0 = System.currentTimeMillis();
    execute("SELECT COUNT(*) FROM " + tbl);
    return System.currentTimeMillis() - t0;
  }

  private long warehouseBytes(final String tbl) throws IOException {
    final FileSystem fs = FileSystem.get((Configuration) conf);
    final Path p = new Path(TestUtils.WH_DIR, tbl.toLowerCase());
    if (!fs.exists(p)) return 0L;
    final ContentSummary cs = fs.getContentSummary(p);
    return cs.getLength();
  }

  private void tryDrop(final String tbl) {
    try {
      execute("DROP TABLE IF EXISTS " + tbl);
    } catch (Exception ignored) {  }
  }

  private static String safeMsg(final Throwable t) {
    final String m = t.getMessage();
    if (m == null) return t.getClass().getSimpleName();
    final String first = m.split("\\R", 2)[0];
    return first.length() > 200 ? first.substring(0, 200) : first;
  }

  private static final class Sample {
    final String mode;
    final int subjectCount;
    int tableRows;
    long tConformMs;
    long bytesAfter;
    long readLatencyMs;
    boolean orderPreserved;
    final int runNumber;
    String error;

    Sample(final String mode, final int subjectCount, final int runNumber) {
      this.mode = mode;
      this.subjectCount = subjectCount;
      this.runNumber = runNumber;
    }
  }

  private static void emit(final BufferedWriter w, final int id, final Sample s)
      throws IOException {
    w.write(id + "," + s.mode + "," + s.subjectCount + "," + s.tableRows + ","
        + s.tConformMs + "," + s.bytesAfter + "," + s.readLatencyMs + ","
        + s.orderPreserved + "," + s.runNumber + ","
        + (s.error == null ? "" : s.error.replace(',', ';')) + "\n");
  }
}
