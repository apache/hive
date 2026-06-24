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
package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HIVE-26758: staging directory path selection controlled by
 * hive.use.scratchdir.for.staging.
 */
public class TestGenMapRedUtilsStagingPath {

  private static final Path DEST_PATH =
      new Path("hdfs://namenode/warehouse/mydb.db/target_tbl");
  private static final Path FINAL_JOB_STAGING =
      new Path("hdfs://namenode/warehouse/mydb.db/target_tbl/.hive-staging_job0");
  private static final Path INTERIM_JOB_STAGING =
      new Path("hdfs://namenode/tmp/hive-scratch/.hive-staging_job0");

  @BeforeClass
  public static void initializeSessionState() {
    HiveConf conf = new HiveConf();
    conf.set("_hive.hdfs.session.path",  "file:///tmp/hive-test/hdfs-session");
    conf.set("_hive.local.session.path", "file:///tmp/hive-test/local-session");
    conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    SessionState.start(conf);
  }

  private static HiveConf buildConf() {
    HiveConf conf = new HiveConf();
    conf.set("_hive.hdfs.session.path",  "file:///tmp/hive-test/hdfs-session");
    conf.set("_hive.local.session.path", "file:///tmp/hive-test/local-session");
    conf.set(ConfVars.STAGING_DIR.varname, ".hive-staging");
    return conf;
  }

  /**
   * Builds a minimal FileSinkDesc whose isMmTable and
   * isDirectInsert flags are both false, so that
   * GenMapRedUtils#createMoveTask enters the staging-path branch.
   */
  private static FileSinkDesc buildFileSinkDesc(Path destPath) {
    TableDesc tableDesc = new TableDesc(null, null, new Properties());
    FileSinkDesc fsd = new FileSinkDesc(
        destPath,
        tableDesc,
        false,
        0,
        false,
        false,
        1,
        1,
        null,
        null,
        destPath,
        false,
        false,
        false,
        false,
        false,
        AcidUtils.Operation.NOT_ACID,
        false);
    return fsd;
  }

  private static FileSinkOperator buildFileSinkOperator(FileSinkDesc fsd) {
    FileSinkOperator fsOp = mock(FileSinkOperator.class);
    when(fsOp.getConf()).thenReturn(fsd);
    when(fsOp.getSchema()).thenReturn(mock(RowSchema.class));
    when(fsOp.getCompilationOpContext()).thenReturn(mock(CompilationOpContext.class));
    return fsOp;
  }

  private static Context buildMockContext() {
    Context mockCtx = mock(Context.class);
    when(mockCtx.getTempDirForFinalJobPath(any(Path.class))).thenReturn(FINAL_JOB_STAGING);
    when(mockCtx.getTempDirForInterimJobPath(any(Path.class))).thenReturn(INTERIM_JOB_STAGING);
    return mockCtx;
  }

  private static ParseContext buildMockParseContext(Context ctx) {
    ParseContext mockParseCtx = mock(ParseContext.class);
    when(mockParseCtx.getContext()).thenReturn(ctx);
    return mockParseCtx;
  }

  @Test
  public void testCreateMoveTaskUsesFinalJobPathWhenConfigFalse() {
    HiveConf conf = buildConf();
    conf.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, false);
    Context mockCtx = buildMockContext();
    FileSinkDesc fsd = buildFileSinkDesc(DEST_PATH);
    FileSinkOperator fsOp = buildFileSinkOperator(fsd);

    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, fsOp,
        buildMockParseContext(mockCtx),
        Collections.emptyList(), conf, null);

    verify(mockCtx).getTempDirForFinalJobPath(DEST_PATH);
    verify(mockCtx, never()).getTempDirForInterimJobPath(any());

    Path dirName = fsd.getDirName();

    assertNotNull("FileSinkDesc.getDirName() must not be null", dirName);
    assertEquals(
        "With scratchdir.for.staging=false, FileSinkDesc.getDirName() must equal "
            + "the value returned by getTempDirForFinalJobPath (FINAL_JOB_STAGING).",
        FINAL_JOB_STAGING.toString(), dirName.toString());
    assertNotEquals(
        "With scratchdir.for.staging=false, getDirName() must NOT be the interim staging path.",
        INTERIM_JOB_STAGING.toString(), dirName.toString());
  }

  @Test
  public void testCreateMoveTaskUsesInterimJobPathWhenConfigTrue() {
    HiveConf conf = buildConf();
    conf.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, true);
    Context mockCtx = buildMockContext();
    FileSinkDesc fsd = buildFileSinkDesc(DEST_PATH);
    FileSinkOperator fsOp = buildFileSinkOperator(fsd);

    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, fsOp,
        buildMockParseContext(mockCtx),
        Collections.emptyList(), conf, null);

    verify(mockCtx).getTempDirForInterimJobPath(DEST_PATH);
    verify(mockCtx, never()).getTempDirForFinalJobPath(any());

    Path dirName = fsd.getDirName();

    assertNotNull("FileSinkDesc.getDirName() must not be null", dirName);
    assertEquals(
        "With scratchdir.for.staging=true, FileSinkDesc.getDirName() must equal "
            + "the value returned by getTempDirForInterimJobPath (INTERIM_JOB_STAGING).",
        INTERIM_JOB_STAGING.toString(), dirName.toString());
    assertNotEquals(
        "With scratchdir.for.staging=true, getDirName() must NOT be the dest-relative staging path.",
        FINAL_JOB_STAGING.toString(), dirName.toString());
  }

  /**
   * Flipping the config flag must produce a different staging path, so that the two paths do not collide.
   */
  @Test
  public void testConfigSwitchChangesStagingLocation() {
    HiveConf confFalse = buildConf();
    confFalse.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, false);
    Context mockCtxFalse = buildMockContext();
    FileSinkDesc fsdFalse = buildFileSinkDesc(DEST_PATH);

    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, buildFileSinkOperator(fsdFalse),
        buildMockParseContext(mockCtxFalse),
        Collections.emptyList(), confFalse, null);

    HiveConf confTrue = buildConf();
    confTrue.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, true);
    Context mockCtxTrue = buildMockContext();
    FileSinkDesc fsdTrue = buildFileSinkDesc(DEST_PATH);

    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, buildFileSinkOperator(fsdTrue),
        buildMockParseContext(mockCtxTrue),
        Collections.emptyList(), confTrue, null);

    assertNotEquals(
        "Flipping hive.use.scratchdir.for.staging must produce a different "
            + "FileSinkDesc.getDirName(). false=" + fsdFalse.getDirName()
            + " true=" + fsdTrue.getDirName(),
        fsdFalse.getDirName().toString(),
        fsdTrue.getDirName().toString());
    assertEquals("false-branch must use FINAL_JOB_STAGING",
        FINAL_JOB_STAGING.toString(), fsdFalse.getDirName().toString());
    assertEquals("true-branch must use INTERIM_JOB_STAGING",
        INTERIM_JOB_STAGING.toString(), fsdTrue.getDirName().toString());
  }

  /**
   * The HIVE-26758 staging path change applies ONLY to:
   * native table, no micro-managed (MM), no direct-insert, no full-ACID
   */
  @Test
  public void testNoStagingForMmTable() {
    HiveConf conf = buildConf();
    conf.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, false);
    Properties mmProps = new Properties();
    mmProps.setProperty(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "insert_only");
    TableDesc mmTableDesc = new TableDesc(null, null, mmProps);

    FileSinkDesc fsd = new FileSinkDesc(
        DEST_PATH, mmTableDesc, false, 0, false, false,
            1, 1, null, null,
        DEST_PATH, false, false, false, false, false,
        AcidUtils.Operation.NOT_ACID, false);

    Context mockCtx = buildMockContext();
    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, buildFileSinkOperator(fsd),
        buildMockParseContext(mockCtx),
        Collections.emptyList(), conf, null);

    // Neither staging method must be called for an MM table.
    verify(mockCtx, never()).getTempDirForFinalJobPath(any());
    verify(mockCtx, never()).getTempDirForInterimJobPath(any());

    // getDirName() must remain at its initial value – no staging redirection.
    assertEquals(
        "MM table: getDirName() must stay at DEST_PATH; staging must be skipped.",
        DEST_PATH.toString(), fsd.getDirName().toString());
  }

  /**
   * Full-ACID tables with direct-insert must bypass the staging path selection;
   */
  @Test
  public void testNoStagingForDirectInsert() {
    HiveConf conf = buildConf();
    conf.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, false);

    FileSinkDesc fsd = new FileSinkDesc(
        DEST_PATH, new TableDesc(null, null, new Properties()),
        false, 0, false, false, 1, 1,
            null, null,
        DEST_PATH, false, false, false, false,
        true,   // isDirectInsert = true
        AcidUtils.Operation.INSERT, false);

    Context mockCtx = buildMockContext();
    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, buildFileSinkOperator(fsd),
        buildMockParseContext(mockCtx),
        Collections.emptyList(), conf, null);

    // Neither staging method must be called for a direct-insert table.
    verify(mockCtx, never()).getTempDirForFinalJobPath(any());
    verify(mockCtx, never()).getTempDirForInterimJobPath(any());

    assertEquals(
        "Direct-insert table: getDirName() must stay at DEST_PATH; staging must be skipped.",
        DEST_PATH.toString(), fsd.getDirName().toString());
  }

  /**
   * With the default config hive.use.scratchdir.for.staging=false;
   * GenMapRedUtils#createMoveTask must produce the new HIVE-26758 layout:
   * <staging_root>/<SPPath> — staging directory appears before the static partition segment.
   * Example (year=2001 is the static partition)
   * table_path/.hive-staging_job0/year=2001
   */
  @Test
  public void testDynamicPartitionStagingLayoutWithDefaultConfig() {
    HiveConf conf = buildConf();
    conf.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, false);
    // Static partition segment: year=2001  (dynamic partition is season)
    String spPath = "year=2001";
    DynamicPartitionCtx dpCtx = new DynamicPartitionCtx();
    dpCtx.setSPPath(spPath);
    FileSinkDesc fsd = buildFileSinkDesc(DEST_PATH);
    fsd.setDynPartCtx(dpCtx);
    Context mockCtx = buildMockContext();
    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, buildFileSinkOperator(fsd),
        buildMockParseContext(mockCtx),
        Collections.emptyList(), conf, null);
    Path expected = new Path(FINAL_JOB_STAGING, spPath);

    assertEquals(
        "With scratchdir.for.staging=false, getDirName() must be <staging_root>/year=2001 "
            + "(staging dir BEFORE static partition).",
        expected.toString(), fsd.getDirName().toString());

    String dirStr  = fsd.getDirName().toString();
    int stagingIdx = dirStr.indexOf(".hive-staging");
    int spIdx      = dirStr.indexOf(spPath);

    assertTrue(
        "Staging dir must precede static partition. stagingIdx=" + stagingIdx
            + " spIdx=" + spIdx + " path=" + dirStr,
        stagingIdx >= 0 && stagingIdx < spIdx);

    // Explicitly confirm that the PRE HIVE-26758 behavior (<dest>/year=2001/<staging>) is NOT produced.
    assertFalse(
        "getDirName() must NOT follow the pre-patch layout (<dest>/year=2001/<staging>). Got: " + dirStr,
        dirStr.contains(DEST_PATH + "/" + spPath));
  }

  /**
   * With hive.use.scratchdir.for.staging=true,
   * GenMapRedUtils#createMoveTask must place staging under the
   * scratch directory and still append the static partition after it:
   * <hive.exec.scratchdir>/<staging_root>/<SPPath>
   * This lets a subsequent MoveTask relocate data from scratchdir to the
   * final table path without any intermediate moves under the table location.
   * Example (year=2001 is the static partition)
   *   expected: hive.exec.scratchdir/.hive-staging_job0/year=2001
   */
  @Test
  public void testDynamicPartitionStagingLayoutWithScratchdirConfig() {
    HiveConf conf = buildConf();
    conf.setBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING, true);
    String spPath = "year=2001";
    DynamicPartitionCtx dpCtx = new DynamicPartitionCtx();
    dpCtx.setSPPath(spPath);
    FileSinkDesc fsd = buildFileSinkDesc(DEST_PATH);
    fsd.setDynPartCtx(dpCtx);
    Context mockCtx = buildMockContext();
    GenMapRedUtils.createMoveTask(
        new MapRedTask(), true, buildFileSinkOperator(fsd),
        buildMockParseContext(mockCtx),
        Collections.emptyList(), conf, null);
    Path expected = new Path(INTERIM_JOB_STAGING, spPath);

    assertEquals(
        "With scratchdir.for.staging=true, getDirName() must be "
            + "<scratchdir_staging_root>/year=2001.",
        expected.toString(), fsd.getDirName().toString());

    String dirStr  = fsd.getDirName().toString();
    int stagingIdx = dirStr.indexOf(".hive-staging");
    int spIdx      = dirStr.indexOf(spPath);

    assertTrue(
        "Staging dir must precede static partition. stagingIdx=" + stagingIdx
            + " spIdx=" + spIdx + " path=" + dirStr,
        stagingIdx >= 0 && stagingIdx < spIdx);
    assertTrue(
        "With config=true, staging must be rooted at INTERIM_JOB_STAGING (scratchdir). Got: " + dirStr,
        dirStr.startsWith(INTERIM_JOB_STAGING.toString()));
    assertFalse(
        "With config=true, staging must NOT be rooted at FINAL_JOB_STAGING (table path). Got: " + dirStr,
        dirStr.startsWith(FINAL_JOB_STAGING.toString()));
  }

  @Test
  public void testDefaultConfigValueIsFalse() {
    assertFalse(
        "hive.use.scratchdir.for.staging must default to false",
        buildConf().getBoolVar(ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING));
  }
}
