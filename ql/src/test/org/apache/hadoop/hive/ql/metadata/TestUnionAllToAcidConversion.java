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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers the non-ACID→full-ACID conversion of a table whose pre-conversion
 * data was written by {@code INSERT ... UNION ALL ...} — i.e. the branches
 * materialize under {@code HIVE_UNION_SUBDIR_<N>/000000_0}.
 *
 * <p>Both DDL paths — the dedicated {@code ALTER TABLE ... CONVERT TO ACID}
 * and the historical {@code ALTER TABLE ... SET TBLPROPERTIES
 * ('transactional'='true')} — are metadata-only flips: they preserve the
 * pre-conversion on-disk layout, including the {@code HIVE_UNION_SUBDIR_<N>/}
 * subdirs. Existing callers depend on this (see
 * {@code TestTxnNoBuckets.testToAcidConversionMultiBucket}, which verifies
 * both the preserved layout and specific ROW__ID assignments encoded per
 * subdir). A subsequent {@code SELECT} must therefore read the pre-conversion
 * layout cleanly — which requires the ACID reader's parent-walk to skip
 * non-delta directory names rather than feeding them to
 * {@code ParsedDeltaLight.parse}. That reader-side guard is what these tests
 * exercise end-to-end.
 */
class TestUnionAllToAcidConversion extends TxnCommandsBaseForTests {

  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir")
      + File.separator + TestUnionAllToAcidConversion.class.getCanonicalName()
      + "-" + System.currentTimeMillis()).getPath().replaceAll("\\\\", "/");

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Override
  protected void initHiveConf() {
    super.initHiveConf();
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_STRICT_MANAGED_TABLES, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.CREATE_TABLES_AS_ACID, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, false);
    // Keep the UNION-ALL branches in their own HIVE_UNION_SUBDIR_<N>/ intermediate
    // directories so the conversion sees the multi-level layout.
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEZ_UNION_FLATTEN_SUBDIRECTORIES, false);
  }

  @Override
  protected void setUpSchema() {
    // Skip the parent's ACID/bucketed fixture tables — each test creates its own.
  }

  @Override
  protected void dropTables() {
    // Match setUpSchema override: nothing to drop for the parent's fixtures.
  }

  @AfterEach
  public void dropAllTestTables() throws Exception {
    for (String t : new String[] {
        "union_all_repro",
        "union_all_repro_setprops",
        "union_all_repro_part",
        "union_all_repro_part_setprops",
        "union_src"}) {
      try {
        runQuery("drop table if exists " + t);
      } catch (Exception ignore) {
        // don't let a residual-drop failure hide the real test failure
      }
    }
  }

  private void insertUnionAllInto(String tbl) throws Exception {
    // Real staging table + per-branch group-by, so the planner can't
    // constant-fold the branches into one mapper. This is what surfaces the
    // HIVE_UNION_SUBDIR_<N>/ layout at final-move time.
    runQuery("create table if not exists union_src (k int, v int) stored as orc "
        + "tblproperties ('transactional'='false')");
    runQuery("insert into union_src values (1, 10), (2, 20), (3, 30)");

    runQuery(
        "insert into " + tbl + " "
        + "select k as a, sum(v) as b from union_src where k = 1 group by k union all "
        + "select k as a, sum(v) as b from union_src where k = 2 group by k union all "
        + "select k as a, sum(v) as b from union_src where k = 3 group by k");
  }

  /**
   * Same 3-way UNION ALL pattern as {@link #insertUnionAllInto(String)}, but
   * targeted at a static partition {@code p='X'} of a partitioned table.
   */
  private void insertUnionAllIntoPartition(String tbl, String partValue) throws Exception {
    runQuery("create table if not exists union_src (k int, v int) stored as orc "
        + "tblproperties ('transactional'='false')");
    runQuery("insert into union_src values (1, 10), (2, 20), (3, 30)");

    runQuery(
        "insert into " + tbl + " partition (p='" + partValue + "') "
        + "select k as a, sum(v) as b from union_src where k = 1 group by k union all "
        + "select k as a, sum(v) as b from union_src where k = 2 group by k union all "
        + "select k as a, sum(v) as b from union_src where k = 3 group by k");
  }

  /**
   * Return the table's on-disk layout as a sorted list of warehouse-relative
   * paths (i.e. including the table directory name as the top-level segment,
   * one level higher than the table root itself), so tests can assert against
   * an exact expected literal.
   */
  private List<String> layoutOf(String tbl) throws Exception {
    org.apache.hadoop.fs.Path loc = new org.apache.hadoop.fs.Path(
        hiveConf.get("hive.metastore.warehouse.dir") + "/" + tbl);
    org.apache.hadoop.fs.FileSystem fs = loc.getFileSystem(hiveConf);
    List<String> paths = new ArrayList<>();
    if (fs.exists(loc)) {
      org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it = fs.listFiles(loc, true);
      // Strip the *parent* of the table location (the warehouse dir), so the
      // returned paths start with "/<table-name>/…" — one level higher than
      // just the table root.
      String warehousePath = loc.getParent().toUri().getPath();
      while (it.hasNext()) {
        org.apache.hadoop.fs.LocatedFileStatus s = it.next();
        String full = s.getPath().toUri().getPath();
        paths.add(full.startsWith(warehousePath) ? full.substring(warehousePath.length()) : full);
      }
    }
    Collections.sort(paths);
    return paths;
  }

  private List<String> runQuery(String stmt) throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, org.apache.hadoop.hive.ql.QueryPlan.makeQueryId());
    d.run(stmt);
    List<String> rs = new ArrayList<>();
    d.getResults(rs);
    return rs;
  }

  /**
   * {@code ALTER TABLE ... CONVERT TO ACID} on a table previously loaded via
   * UNION ALL is a metadata-only flip: the on-disk layout is unchanged, and
   * the subsequent SELECT returns the expected row count.
   */
  @Test
  void testUnionAllInsertThenConvertToAcid() throws Exception {
    String tbl = "union_all_repro";
    runQuery("create table " + tbl + " (a int, b int) stored as orc "
        + "tblproperties ('transactional'='false')");

    insertUnionAllInto(tbl);

    // The UNION-ALL insert materializes each branch under its own
    // HIVE_UNION_SUBDIR_<N>/000000_0. Assert the exact layout so a reader of
    // this test can see what CONVERT TO ACID is going to be run against.
    List<String> expectedLayout = List.of(
        "/union_all_repro/HIVE_UNION_SUBDIR_1/000000_0",
        "/union_all_repro/HIVE_UNION_SUBDIR_2/000000_0",
        "/union_all_repro/HIVE_UNION_SUBDIR_3/000000_0");
    List<String> beforeLayout = layoutOf(tbl);
    assertEquals(expectedLayout, beforeLayout, "pre-conversion layout");

    runQuery("alter table " + tbl + " convert to acid");

    // CONVERT TO ACID is a metadata-only flip: the on-disk layout is unchanged.
    List<String> afterLayout = layoutOf(tbl);
    assertEquals(expectedLayout, afterLayout,
        "CONVERT TO ACID should be a metadata-only flip and preserve the pre-conversion layout");

    List<String> rows = runQuery("select count(*) from " + tbl);
    assertEquals("3", rows.getFirst(), "expected 3 rows after UNION-ALL insert + CONVERT TO ACID");
  }

  /**
   * {@code ALTER TABLE ... SET TBLPROPERTIES ('transactional'='true')} — the
   * path {@code UpgradeTool} emits — is likewise a metadata-only flip. The
   * subsequent SELECT returns the expected row count.
   */
  @Test
  void testUnionAllInsertThenSetTblpropertiesAcid() throws Exception {
    String tbl = "union_all_repro_setprops";
    runQuery("create table " + tbl + " (a int, b int) stored as orc "
        + "tblproperties ('transactional'='false')");

    insertUnionAllInto(tbl);

    // Same shape of pre-conversion layout as the CONVERT TO ACID case above,
    // rooted under this test's own table directory.
    List<String> expectedLayout = List.of(
        "/union_all_repro_setprops/HIVE_UNION_SUBDIR_1/000000_0",
        "/union_all_repro_setprops/HIVE_UNION_SUBDIR_2/000000_0",
        "/union_all_repro_setprops/HIVE_UNION_SUBDIR_3/000000_0");
    List<String> beforeLayout = layoutOf(tbl);
    assertEquals(expectedLayout, beforeLayout, "pre-conversion layout");

    runQuery("alter table " + tbl + " set tblproperties ('transactional'='true')");

    List<String> afterLayout = layoutOf(tbl);
    assertEquals(expectedLayout, afterLayout,
        "SET TBLPROPERTIES ('transactional'='true') should preserve the pre-conversion layout");

    List<String> rows = runQuery("select count(*) from " + tbl);
    assertEquals("3", rows.getFirst(),
        "expected 3 rows after UNION-ALL insert + SET TBLPROPERTIES ACID conversion");
  }

  /**
   * Partitioned-table variant of {@link #testUnionAllInsertThenConvertToAcid()}
   * — the UNION-ALL insert materializes each branch under {@code p=x/HIVE_UNION_SUBDIR_<N>/000000_0}
   * inside the partition directory. CONVERT TO ACID is still a metadata-only
   * flip, and the subsequent SELECT returns the expected row count.
   */
  @Test
  void testPartitionedUnionAllInsertThenConvertToAcid() throws Exception {
    String tbl = "union_all_repro_part";
    runQuery("create table " + tbl + " (a int, b int) partitioned by (p string) "
        + "stored as orc tblproperties ('transactional'='false')");

    insertUnionAllIntoPartition(tbl, "x");

    List<String> expectedLayout = List.of(
        "/union_all_repro_part/p=x/HIVE_UNION_SUBDIR_1/000000_0",
        "/union_all_repro_part/p=x/HIVE_UNION_SUBDIR_2/000000_0",
        "/union_all_repro_part/p=x/HIVE_UNION_SUBDIR_3/000000_0");
    List<String> beforeLayout = layoutOf(tbl);
    assertEquals(expectedLayout, beforeLayout, "pre-conversion layout");

    runQuery("alter table " + tbl + " convert to acid");

    List<String> afterLayout = layoutOf(tbl);
    assertEquals(expectedLayout, afterLayout,
        "CONVERT TO ACID should be a metadata-only flip and preserve the pre-conversion layout");

    List<String> rows = runQuery("select count(*) from " + tbl);
    assertEquals("3", rows.getFirst(),
        "expected 3 rows after partitioned UNION-ALL insert + CONVERT TO ACID");
  }

  /**
   * Partitioned-table variant of
   * {@link #testUnionAllInsertThenSetTblpropertiesAcid()} — the
   * {@code UpgradeTool}-style path preserves the pre-conversion partition
   * layout, and the subsequent SELECT returns the expected row count.
   */
  @Test
  void testPartitionedUnionAllInsertThenSetTblpropertiesAcid() throws Exception {
    String tbl = "union_all_repro_part_setprops";
    runQuery("create table " + tbl + " (a int, b int) partitioned by (p string) "
        + "stored as orc tblproperties ('transactional'='false')");

    insertUnionAllIntoPartition(tbl, "x");

    List<String> expectedLayout = List.of(
        "/union_all_repro_part_setprops/p=x/HIVE_UNION_SUBDIR_1/000000_0",
        "/union_all_repro_part_setprops/p=x/HIVE_UNION_SUBDIR_2/000000_0",
        "/union_all_repro_part_setprops/p=x/HIVE_UNION_SUBDIR_3/000000_0");
    List<String> beforeLayout = layoutOf(tbl);
    assertEquals(expectedLayout, beforeLayout, "pre-conversion layout");

    runQuery("alter table " + tbl + " set tblproperties ('transactional'='true')");

    List<String> afterLayout = layoutOf(tbl);
    assertEquals(expectedLayout, afterLayout,
        "SET TBLPROPERTIES ('transactional'='true') should preserve the pre-conversion layout");

    List<String> rows = runQuery("select count(*) from " + tbl);
    assertEquals("3", rows.getFirst(),
        "expected 3 rows after partitioned UNION-ALL insert + SET TBLPROPERTIES ACID conversion");
  }

  // ---------------------------------------------------------------------------
  // flatten=true variants
  //
  // hive.tez.union.flatten.subdirectories=true asks MoveTask to hoist the
  // HIVE_UNION_SUBDIR_<N>/000000_0 leaves into the target directory at write
  // time. MoveTask.flattenUnionSubdirectories folds the subdir index into the
  // attempt-id portion of the writer-name — NOT the _copy_ suffix — so:
  //   HIVE_UNION_SUBDIR_<N>/000000_<A> -> 000000_<N*100000+A>
  //     e.g. HIVE_UNION_SUBDIR_1/000000_0  -> 000000_100000
  //          HIVE_UNION_SUBDIR_23/000000_2 -> 000000_2300002
  // That name matches the metastore's ORIGINAL_PATTERN ([0-9]+_[0-9]+), so a
  // subsequent ACID conversion (either CONVERT TO ACID or the SET TBLPROPERTIES
  // flip) is accepted by TransactionalValidationListener.validateTableStructureForPath.
  // Keeping the fold out of the _copy_ namespace also means a subsequent
  // Hive.pickDestFilePath on a non-atomic-rename FS composes cleanly on top,
  // appending its own _copy_<HIVE-28822 uniqueness tag> without collision.
  // ---------------------------------------------------------------------------

  /**
   * Enable the write-time flatten and re-run the given block. Restores the
   * previous value on exit.
   */
  private void withUnionFlattenAtWriteTime(ThrowingRunnable body) throws Exception {
    boolean previous = HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEZ_UNION_FLATTEN_SUBDIRECTORIES);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEZ_UNION_FLATTEN_SUBDIRECTORIES, true);
    try {
      body.run();
    } finally {
      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEZ_UNION_FLATTEN_SUBDIRECTORIES, previous);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /**
   * flatten=true + unpartitioned + CONVERT TO ACID. MoveTask hoists the
   * union-subdir leaves into {@code 000000_<N*100000>} files at the table
   * root — a plain-writer name that matches ORIGINAL_PATTERN, so the
   * subsequent metadata-only CONVERT TO ACID is accepted and the SELECT
   * returns all rows.
   */
  @Test
  void testUnionAllInsertWithFlattenThenConvertToAcid() throws Exception {
    String tbl = "union_all_repro";
    withUnionFlattenAtWriteTime(() -> {
      runQuery("create table " + tbl + " (a int, b int) stored as orc "
          + "tblproperties ('transactional'='false')");

      insertUnionAllInto(tbl);

      List<String> expectedLayout = List.of(
          "/union_all_repro/000000_100000",
          "/union_all_repro/000000_200000",
          "/union_all_repro/000000_300000");
      assertEquals(expectedLayout, layoutOf(tbl), "pre-conversion layout (write-time flatten on)");

      runQuery("alter table " + tbl + " convert to acid");

      assertEquals(expectedLayout, layoutOf(tbl),
          "CONVERT TO ACID should be a metadata-only flip and preserve the flattened layout");

      List<String> rows = runQuery("select count(*) from " + tbl);
      assertEquals("3", rows.getFirst(),
          "expected 3 rows after flattened UNION-ALL insert + CONVERT TO ACID");
    });
  }

  /**
   * flatten=true + unpartitioned + SET TBLPROPERTIES ACID conversion. Same
   * shape as the CONVERT TO ACID variant: {@code 000000_<N*100000>} matches
   * ORIGINAL_PATTERN, so the conversion succeeds.
   */
  @Test
  void testUnionAllInsertWithFlattenThenSetTblpropertiesAcid() throws Exception {
    String tbl = "union_all_repro_setprops";
    withUnionFlattenAtWriteTime(() -> {
      runQuery("create table " + tbl + " (a int, b int) stored as orc "
          + "tblproperties ('transactional'='false')");

      insertUnionAllInto(tbl);

      List<String> expectedLayout = List.of(
          "/union_all_repro_setprops/000000_100000",
          "/union_all_repro_setprops/000000_200000",
          "/union_all_repro_setprops/000000_300000");
      assertEquals(expectedLayout, layoutOf(tbl), "pre-conversion layout (write-time flatten on)");

      runQuery("alter table " + tbl + " set tblproperties ('transactional'='true')");

      assertEquals(expectedLayout, layoutOf(tbl),
          "SET TBLPROPERTIES ('transactional'='true') should preserve the flattened layout");

      List<String> rows = runQuery("select count(*) from " + tbl);
      assertEquals("3", rows.getFirst(),
          "expected 3 rows after flattened UNION-ALL insert + SET TBLPROPERTIES ACID conversion");
    });
  }

  /**
   * flatten=true + partitioned + CONVERT TO ACID. Same shape as the
   * unpartitioned variant, inside the partition directory: the conversion
   * succeeds and the SELECT returns all rows.
   */
  @Test
  void testPartitionedUnionAllInsertWithFlattenThenConvertToAcid() throws Exception {
    String tbl = "union_all_repro_part";
    withUnionFlattenAtWriteTime(() -> {
      runQuery("create table " + tbl + " (a int, b int) partitioned by (p string) "
          + "stored as orc tblproperties ('transactional'='false')");

      insertUnionAllIntoPartition(tbl, "x");

      List<String> expectedLayout = List.of(
          "/union_all_repro_part/p=x/000000_100000",
          "/union_all_repro_part/p=x/000000_200000",
          "/union_all_repro_part/p=x/000000_300000");
      assertEquals(expectedLayout, layoutOf(tbl), "pre-conversion layout (write-time flatten on)");

      runQuery("alter table " + tbl + " convert to acid");

      assertEquals(expectedLayout, layoutOf(tbl),
          "CONVERT TO ACID should be a metadata-only flip and preserve the flattened layout");

      List<String> rows = runQuery("select count(*) from " + tbl);
      assertEquals("3", rows.getFirst(),
          "expected 3 rows after partitioned flattened UNION-ALL insert + CONVERT TO ACID");
    });
  }

  /**
   * flatten=true + partitioned + SET TBLPROPERTIES ACID conversion.
   */
  @Test
  void testPartitionedUnionAllInsertWithFlattenThenSetTblpropertiesAcid() throws Exception {
    String tbl = "union_all_repro_part_setprops";
    withUnionFlattenAtWriteTime(() -> {
      runQuery("create table " + tbl + " (a int, b int) partitioned by (p string) "
          + "stored as orc tblproperties ('transactional'='false')");

      insertUnionAllIntoPartition(tbl, "x");

      List<String> expectedLayout = List.of(
          "/union_all_repro_part_setprops/p=x/000000_100000",
          "/union_all_repro_part_setprops/p=x/000000_200000",
          "/union_all_repro_part_setprops/p=x/000000_300000");
      assertEquals(expectedLayout, layoutOf(tbl), "pre-conversion layout (write-time flatten on)");

      runQuery("alter table " + tbl + " set tblproperties ('transactional'='true')");

      assertEquals(expectedLayout, layoutOf(tbl),
          "SET TBLPROPERTIES ('transactional'='true') should preserve the flattened layout");

      List<String> rows = runQuery("select count(*) from " + tbl);
      assertEquals("3", rows.getFirst(),
          "expected 3 rows after partitioned flattened UNION-ALL insert + SET TBLPROPERTIES ACID conversion");
    });
  }
}
