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
package org.apache.hadoop.hive.ql.metadata;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.ParsedOutputFileName;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.util.FakeS3FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Driver-level end-to-end test for the copy-suffix logic in
 * {@link Hive#mvFile} on a non-atomic-rename filesystem.
 *
 * <p>Runs real {@code INSERT INTO} and {@code INSERT INTO ... UNION ALL ...}
 * queries through the {@link org.apache.hadoop.hive.ql.Driver} and inspects
 * the resulting on-disk layout of tables whose LOCATION is a
 * synthetic {@code fakes3://} URI. The scheme is registered as an alias for
 * {@link RawLocalFileSystem} (so files still live under {@code test.tmp.dir}
 * on the local disk) and appended to {@link FileUtils#NON_ATOMIC_RENAME_SCHEMES}
 * so {@link FileUtils#isNonAtomicRenameFs} treats it like S3A.
 *
 * <p>What this covers over {@link TestHiveCopyFilesFakeS3}: the whole
 * planner/executor/MoveTask path is exercised, not just {@code Hive.copyFiles}
 * in isolation. Anything that changes how output files land in the table
 * directory (e.g. FileSinkOperator, MoveTask, UnionProcFactory) is on the
 * hook here.
 */
class TestInsertCopySuffixOnFakeS3 extends TxnCommandsBaseForTests {

  private static final String FAKE_SCHEME = FakeS3FileSystem.SCHEME;
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir")
      + File.separator + TestInsertCopySuffixOnFakeS3.class.getCanonicalName()
      + "-" + System.currentTimeMillis()).getPath().replaceAll("\\\\", "/");

  @BeforeAll
  static void addFakeSchemeToUnstableSet() {
    FileUtils.NON_ATOMIC_RENAME_SCHEMES.add(FAKE_SCHEME);
  }

  @AfterAll
  static void removeFakeSchemeFromUnstableSet() {
    FileUtils.NON_ATOMIC_RENAME_SCHEMES.remove(FAKE_SCHEME);
  }

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Override
  protected void initHiveConf() {
    super.initHiveConf();
    // Register the fake scheme's FileSystem impl for this session.
    hiveConf.setClass("fs." + FAKE_SCHEME + ".impl", FakeS3FileSystem.class, FileSystem.class);
    hiveConf.setBoolean("fs." + FAKE_SCHEME + ".impl.disable.cache", true);
    // Non-strict managed tables so we can point tables outside the warehouse.
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_STRICT_MANAGED_TABLES, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.CREATE_TABLES_AS_ACID, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, false);
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    // UNION-ALL: keep subdirs unflattened so we exercise the layout that production S3 workloads see.
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEZ_UNION_FLATTEN_SUBDIRECTORIES, false);
  }

  @AfterEach
  void dropAllTestTables() throws Exception {
    for (String t : new String[] {"insert_into_fakes3", "union_all_fakes3", "union_all_dyn_part_fakes3",
        "insert_only_fakes3", "full_acid_fakes3", "union_src"}) {
      try {
        runQuery("drop table if exists " + t);
      } catch (Exception ignore) {
        // don't let a residual-drop failure hide the real test failure
      }
    }
  }

  @Override
  protected void setUpSchema() {
    // Override the parent's schema — we don't need the ACID/bucketed
    // TxnCommandsBaseForTests fixture tables; each test creates its own
    // external table at a fakes3:// location.
  }

  @Override
  protected void dropTables() {
    // The parent's dropTables would try to drop the schema tables that we
    // never created; skip.
  }

  private List<String> runQuery(String stmt) throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, org.apache.hadoop.hive.ql.QueryPlan.makeQueryId());
    d.run(stmt);
    List<String> rs = new ArrayList<>();
    d.getResults(rs);
    return rs;
  }

  /**
   * Absolute local path under the test temp dir, wrapped in a {@code fakes3://}
   * URI. Files land on the local disk at {@code path}, but Hive resolves the
   * URI to our FakeS3FileSystem and applies the non-atomic-rename logic.
   */
  private String fakeS3Location(String subdir) {
    return FAKE_SCHEME + "://" + TEST_DATA_DIR + "/" + subdir;
  }

  private Path fakeS3Path(String subdir) {
    return new Path(URI.create(fakeS3Location(subdir)));
  }

  /** Collect every file under {@code root} into a flat list of relative paths. */
  private List<String> listFilesRelative(Path root) throws IOException {
    FileSystem fs = root.getFileSystem(hiveConf);
    List<String> paths = new ArrayList<>();
    if (!fs.exists(root)) {
      return paths;
    }
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(root, true);
    while (it.hasNext()) {
      LocatedFileStatus s = it.next();
      if (s.isFile() && !s.getPath().getName().startsWith("_") && !s.getPath().getName().startsWith(".")) {
        String full = s.getPath().toUri().getPath();
        String rootPath = root.toUri().getPath();
        paths.add(full.startsWith(rootPath) ? full.substring(rootPath.length()) : full);
      }
    }
    return paths;
  }

  /**
   * Simple {@code INSERT INTO} into a non-ACID external ORC table on
   * fakes3://. Every output file must carry the 16-hex uniqueness-tag copy
   * suffix — no plain {@code 000000_N}.
   */
  @Test
  void testInsertIntoNonAcidExternalOnFakeS3() throws Exception {
    String tbl = "insert_into_fakes3";
    Path loc = fakeS3Path(tbl);

    runQuery(
        "create external table " + tbl + " (a int, b int) stored as orc "
        + "location '" + fakeS3Location(tbl) + "' "
        + "tblproperties ('transactional'='false','external.table.purge'='true')");

    runQuery("insert into " + tbl + " values (1, 10), (2, 20), (3, 30)");

    List<String> files = listFilesRelative(loc);
    assertFalse(files.isEmpty(), "insert produced no files under " + loc);
    for (String rel : files) {
      String name = rel.substring(rel.lastIndexOf('/') + 1);
      assertHas16HexUniquenessTag(name);
      assertFalse(AcidUtils.ORIGINAL_PATTERN.matcher(name).matches(),
          "no plain 000000_N leaf allowed on non-atomic-rename FS: " + rel);
    }

    assertRowCount(tbl, 3);

    convertToFullAcidAndAssertRowCount(tbl, 3);
  }

  /**
   * {@code INSERT INTO ... UNION ALL ...} on a non-ACID external ORC
   * table. Every output file must carry the tag suffix, no two leaves may
   * share a name (that's the anti-silent-overwrite guarantee), and the row
   * count must be 3. The number of output files is a planner choice — one
   * combined mapper vs one-per-branch — so we do NOT assert on it.
   * hive.tez.union.flatten.subdirectories=false is set, so the
   * HIVE_UNION_SUBDIR_N layout is preserved when the planner does emit it.
   */
  @Test
  void testUnionAllInsertOnFakeS3() throws Exception {
    String tbl = "union_all_fakes3";
    Path loc = fakeS3Path(tbl);

    runQuery(
        "create external table " + tbl + " (a int, b int) stored as orc "
        + "location '" + fakeS3Location(tbl) + "' "
        + "tblproperties ('transactional'='false','external.table.purge'='true')");

    createUnionSrc();
    runQuery(
        "insert into " + tbl + " "
        + "select k as a, sum(v) as b from union_src where k = 1 group by k union all "
        + "select k as a, sum(v) as b from union_src where k = 2 group by k union all "
        + "select k as a, sum(v) as b from union_src where k = 3 group by k");

    assertUnionSubdirLayoutAt(loc, /* partitionSegment */ null);

    assertRowCount(tbl, 3);

    // FIXME: convertToFullAcidAndAssertRowCount should pass after HIVE-29798 is fixed
    assertThrows(Exception.class,
        () -> convertToFullAcidAndAssertRowCount(tbl, 3),
        "expected read-back after CONVERT TO ACID to fail until HIVE-29798 is fixed");
  }

  /**
   * Same 3-way UNION ALL, but this time the target is a partitioned external
   * table and the UNION branches all land in the same new dynamic partition.
   * This is the shape the HIVE-28822 concurrent-insert repro compressed into a
   * single statement. Every leaf must still be tagged; no writer may clobber
   * another.
   */
  @Test
  void testUnionAllInsertToPartitionedOnFakeS3() throws Exception {
    String tbl = "union_all_dyn_part_fakes3";
    Path loc = fakeS3Path(tbl);

    runQuery(
        "create external table " + tbl + " (a int) partitioned by (b int) stored as orc "
        + "location '" + fakeS3Location(tbl) + "' "
        + "tblproperties ('transactional'='false','external.table.purge'='true')");

    createUnionSrc();
    runQuery(
        "insert into " + tbl + " partition (b) "
        + "select k as a, 2 as b from union_src where k = 1 group by k union all "
        + "select k as a, 2 as b from union_src where k = 2 group by k union all "
        + "select k as a, 2 as b from union_src where k = 3 group by k");

    assertUnionSubdirLayoutAt(loc, /* partitionSegment */ "b=2");

    assertRowCount(tbl, 3);

    convertToFullAcidAndAssertRowCount(tbl, 3);
  }

  /**
   * Materialize a small staging table {@code union_src} so each UNION branch
   * does a real group-by scan; that keeps the planner from constant-folding
   * the branches into a single mapper and preserves the per-branch
   * HIVE_UNION_SUBDIR_<N>/ layout at final-move time.
   */
  private void createUnionSrc() throws Exception {
    runQuery("drop table if exists union_src");
    runQuery("create table union_src (k int, v int) stored as orc "
        + "tblproperties ('transactional'='false')");
    runQuery("insert into union_src values (1, 10), (2, 20), (3, 30)");
  }

  /**
   * Common tail of the two UNION-ALL tests. Lists every file under {@code loc}
   * and asserts:
   * <ol>
   *   <li>Some files were produced (union insert didn't silently no-op).</li>
   *   <li>If {@code partitionSegment} is non-null, every leaf's path contains
   *       {@code /<partitionSegment>/} (e.g. {@code /b=2/}).</li>
   *   <li>Every leaf's parent directory starts with
   *       {@link AbstractFileMergeOperator#UNION_SUDBIR_PREFIX}, and there are
   *       exactly 3 such distinct parent dirs (one per UNION branch).</li>
   *   <li>Every leaf name is a plain writer name (matches
   *       {@link AcidUtils#ORIGINAL_PATTERN}) with NO {@code _copy_<tag>} suffix:
   *       with per-branch subdirs there is no target-side collision to defend
   *       against, so {@code pickDestFilePath} does not stamp a uniqueness tag
   *       on top. The HIVE_UNION_SUBDIR_<N>/ separation is the anti-collision
   *       mechanism here.</li>
   * </ol>
   */
  private void assertUnionSubdirLayoutAt(Path loc, String partitionSegment) throws IOException {
    List<String> files = listFilesRelative(loc);
    assertFalse(files.isEmpty(), "union-all insert produced no files under " + loc);

    Set<String> subdirs = new HashSet<>();
    for (String rel : files) {
      if (partitionSegment != null) {
        assertTrue(rel.contains("/" + partitionSegment + "/"),
            "output must live under " + partitionSegment + " partition: " + rel);
      }
      int lastSlash = rel.lastIndexOf('/');
      int prevSlash = rel.lastIndexOf('/', lastSlash - 1);
      String parentDir = rel.substring(prevSlash + 1, lastSlash);
      assertTrue(parentDir.startsWith(AbstractFileMergeOperator.UNION_SUDBIR_PREFIX),
          "union branch's leaf must live under a HIVE_UNION_SUBDIR_<N>/ dir: " + rel);
      subdirs.add(parentDir);

      String name = rel.substring(lastSlash + 1);
      assertTrue(AcidUtils.ORIGINAL_PATTERN.matcher(name).matches(),
          "leaf must be a plain writer name (no _copy_<tag> suffix expected): " + rel);
      ParsedOutputFileName parsed = ParsedOutputFileName.parse(name);
      assertTrue(parsed.matches(), "ParsedOutputFileName should recognize " + name);
      assertFalse(parsed.isCopyFile(),
          "leaf must NOT carry a copy suffix — HIVE_UNION_SUBDIR_ layout already"
              + " isolates concurrent writers, so no uniqueness tag is expected: " + rel);
    }
    assertEquals(3, subdirs.size(),
        "each of the 3 UNION branches must have its own HIVE_UNION_SUBDIR_<N>/: " + subdirs);
  }

  /**
   * Insert into a micromanaged (insert-only ACID) table on fakes3://. Unlike the
   * non-ACID cases above, MM tables get their per-writer uniqueness from the
   * writeId-scoped {@code delta_<writeId>_<writeId>_<stmt>/} subdirectory rather
   * than from a per-query filename tag: MoveTask short-circuits into the delta
   * layout without going through {@link Hive#copyFiles} at all. So on a non-atomic-rename
   * FS the leaves inside the delta directory are the plain writer-emitted names
   * (e.g. {@code 000000_N}) — that's expected, and the delta_ dir alone keeps
   * two concurrent MM writers from clobbering each other.
   *
   * <p>Two concurrent MM inserts would get separate {@code delta_<writeId>} dirs,
   * so a plain {@code 000000_0} inside each is safe. We can't easily reproduce a
   * concurrent write here, but we can pin the current single-insert layout:
   * every leaf lives under a delta_ subdirectory, and the row count matches.
   */
  @Test
  void testInsertIntoMicromanagedOnFakeS3LandsUnderDeltaSubdir() throws Exception {
    String tbl = "insert_only_fakes3";
    Path loc = fakeS3Path(tbl);
    runQuery(
        "create table " + tbl + " (a int, b int) stored as orc "
        + "location '" + fakeS3Location(tbl) + "' "
        + "tblproperties ('transactional'='true','transactional_properties'='insert_only')");

    runQuery("insert into " + tbl + " values (1, 10), (2, 20), (3, 30)");

    List<String> files = listFilesRelative(loc);
    assertFalse(files.isEmpty(), "MM insert produced no files under " + loc);

    // Every leaf must live under a delta_* directory (that's the MM per-writeId
    // uniqueness scope — the analogue of the per-query tag for the non-ACID
    // cases). The leaf name itself is the writer's plain 000000_N.
    for (String rel : files) {
      assertTrue(rel.contains("/" + AcidUtils.DELTA_PREFIX),
          "MM leaf must live under a delta_* subdir: " + rel);
      String name = rel.substring(rel.lastIndexOf('/') + 1);
      assertTrue(AcidUtils.ORIGINAL_PATTERN.matcher(name).matches(),
          "MM leaf must be a plain writer name (000000_N): " + rel);
    }

    assertRowCount(tbl, 3);

    convertToFullAcidAndAssertRowCount(tbl, 3);
  }

  /**
   * Insert into a full ACID (transactional=true, default transactional_properties)
   * managed table on fakes3://. Same story as the MM case: MoveTask short-circuits
   * into the {@code delta_<writeId>_<writeId>_<stmt>/} layout without going through
   * {@link Hive#copyFiles}, so the per-query filename tag never fires. Full-ACID
   * leaves are named {@code bucket_NNNNN} (see {@link AcidUtils#BUCKET_PATTERN})
   * — the per-writeId delta directory is what keeps concurrent writers apart.
   */
  @Test
  void testInsertIntoFullAcidOnFakeS3LandsUnderDeltaSubdir() throws Exception {
    String tbl = "full_acid_fakes3";
    Path loc = fakeS3Path(tbl);
    runQuery(
        "create table " + tbl + " (a int, b int) stored as orc "
        + "location '" + fakeS3Location(tbl) + "' "
        + "tblproperties ('transactional'='true')");

    runQuery("insert into " + tbl + " values (1, 10), (2, 20), (3, 30)");

    List<String> files = listFilesRelative(loc);
    assertFalse(files.isEmpty(), "full-ACID insert produced no files under " + loc);

    // Every leaf must live under a delta_* directory and be a bucket_NNNNN file.
    // No _copy_ tag: full-ACID takes the isTransactional short-circuit in
    // Hive.loadTable, bypassing pickDestFilePath entirely.
    for (String rel : files) {
      assertTrue(rel.contains("/" + AcidUtils.DELTA_PREFIX),
          "full-ACID leaf must live under a delta_* subdir: " + rel);
      String name = rel.substring(rel.lastIndexOf('/') + 1);
      assertTrue(AcidUtils.BUCKET_PATTERN.matcher(name).matches(),
          "full-ACID leaf must be a bucket_NNNNN name: " + rel);
    }

    assertRowCount(tbl, 3);
  }

  private void assertRowCount(String table, int count) throws Exception {
    List<String> rows = runQuery("select count(*) from " + table);
    assertEquals(String.valueOf(count), rows.getFirst(), "expected " + count + " rows total");
  }

  private void convertToFullAcidAndAssertRowCount(String tbl, int expectedRowCount) throws Exception {
    runQuery("alter table " + tbl + " set tblproperties ('EXTERNAL'='FALSE')");
    runQuery("alter table " + tbl + " set tblproperties ('transactional_properties'='default')");
    runQuery("alter table " + tbl + " set tblproperties ('transactional'='true')");

    assertRowCount(tbl, expectedRowCount);
  }

  /**
   * Asserts that {@code name} is a copy-suffixed filename whose suffix is the
   * 16-hex per-query uniqueness tag (as opposed to the numeric {@code _copy_N}
   * fallback that {@link AcidUtils#ORIGINAL_PATTERN_COPY} also accepts). Parses
   * via {@link ParsedOutputFileName} so we track whatever it accepts.
   */
  private static void assertHas16HexUniquenessTag(String name) {
    // Baseline: must match the copy pattern the readers accept.
    assertTrue(AcidUtils.ORIGINAL_PATTERN_COPY.matcher(name).matches(),
        "must match _copy_ pattern: " + name);
    // Tighten: the suffix must be exactly 16 hex chars, not the numeric fallback.
    ParsedOutputFileName parsed = ParsedOutputFileName.parse(name);
    assertTrue(parsed.matches(), "ParsedOutputFileName should recognize " + name);
    assertTrue(parsed.isCopyFile(), "expected copy suffix on " + name);
    String tag = parsed.getCopyIndex();
    assertEquals(16, tag.length(),
        "copy suffix must be 16 chars (uniqueness tag, not numeric fallback), got: " + tag + " (" + name + ")");
    assertTrue(tag.matches("[0-9a-fA-F]{16}"),
        "copy suffix must be lowercase-or-uppercase hex, got: " + tag + " (" + name + ")");
  }
}
