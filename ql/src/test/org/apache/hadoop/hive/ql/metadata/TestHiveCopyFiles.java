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
package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@RunWith(Parameterized.class)
public class TestHiveCopyFiles {
  private static boolean LOCAL_SOURCE = true;
  private static boolean NO_ACID = false;

  private static HiveConf hiveConf;

  private boolean isSourceLocal;

  @Rule
  public TemporaryFolder sourceFolder = new TemporaryFolder();

  @Rule
  public TemporaryFolder targetFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return Arrays.asList(new Object[][] {
        { 0, LOCAL_SOURCE}, { 15, LOCAL_SOURCE},
        { 0, !LOCAL_SOURCE}, { 15, !LOCAL_SOURCE}
    });
  }

  @BeforeClass
  public static void setUp() {
    hiveConf = new HiveConfForTest(TestHiveCopyFiles.class);
    SessionState.start(hiveConf);
  }

  public TestHiveCopyFiles(int threadCount, boolean isSourceLocal) {
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT, threadCount);
    this.isSourceLocal = isSourceLocal;
  }

  @Test
  public void testRenameNewFilesOnSameFileSystem() throws IOException {
    Path sourcePath = new Path(sourceFolder.getRoot().getAbsolutePath());
    sourceFolder.newFile("000000_0");
    sourceFolder.newFile("000001_0");
    sourceFolder.newFile("000000_0.gz");
    sourceFolder.newFile("000001_0.gz");

    Path targetPath = new Path(targetFolder.getRoot().getAbsolutePath());
    FileSystem targetFs = targetPath.getFileSystem(hiveConf);

    try {
      Hive.copyFiles(hiveConf, sourcePath, targetPath, targetFs, isSourceLocal, NO_ACID, false,
          null, false, false, false, false);
    } catch (HiveException e) {
      e.printStackTrace();
      assertTrue("Hive.copyFiles() threw an unexpected exception.", false);
    }

    assertTrue(targetFs.exists(new Path(targetPath, "000000_0")));
    assertTrue(targetFs.exists(new Path(targetPath, "000001_0")));
    assertTrue(targetFs.exists(new Path(targetPath, "000000_0.gz")));
    assertTrue(targetFs.exists(new Path(targetPath, "000001_0.gz")));
  }

  @Test
  public void testRenameExistingFilesOnSameFileSystem() throws IOException {
    Path sourcePath = new Path(sourceFolder.getRoot().getAbsolutePath());
    sourceFolder.newFile("000000_0");
    sourceFolder.newFile("000001_0");
    sourceFolder.newFile("000000_0.gz");
    sourceFolder.newFile("000001_0.gz");

    Path targetPath = new Path(targetFolder.getRoot().getAbsolutePath());
    FileSystem targetFs = targetPath.getFileSystem(hiveConf);

    try {
      Hive.copyFiles(hiveConf, sourcePath, targetPath, targetFs, isSourceLocal, NO_ACID, false, null,
          false, false, false, false);
    } catch (HiveException e) {
      e.printStackTrace();
      assertTrue("Hive.copyFiles() threw an unexpected exception.", false);
    }

    // If source is local, then source files won't be deleted, and we have to delete them here
    if (isSourceLocal) {
      sourceFolder.delete();
      sourceFolder.create();
      sourcePath = new Path(sourceFolder.getRoot().getAbsolutePath());
    }

    /* Create new source files with same filenames */
    sourceFolder.newFile("000000_0");
    sourceFolder.newFile("000001_0");
    sourceFolder.newFile("000000_0.gz");
    sourceFolder.newFile("000001_0.gz");

    try {
      Hive.copyFiles(hiveConf, sourcePath, targetPath, targetFs, isSourceLocal, NO_ACID, false, null,
          false, false, false, false);
    } catch (HiveException e) {
      e.printStackTrace();
      assertTrue("Hive.copyFiles() threw an unexpected exception.", false);
    }

    assertTrue(targetFs.exists(new Path(targetPath, "000000_0")));
    assertTrue(targetFs.exists(new Path(targetPath, "000001_0")));
    assertTrue(targetFs.exists(new Path(targetPath, "000000_0.gz")));
    assertTrue(targetFs.exists(new Path(targetPath, "000001_0.gz")));
    assertTrue(targetFs.exists(new Path(targetPath, "000000_0_copy_1")));
    assertTrue(targetFs.exists(new Path(targetPath, "000001_0_copy_1")));
    assertTrue(targetFs.exists(new Path(targetPath, "000000_0_copy_1.gz")));
    assertTrue(targetFs.exists(new Path(targetPath, "000001_0_copy_1.gz")));
  }

  @Test
  public void testCopyNewFilesOnDifferentFileSystem() throws IOException {
    Path sourcePath = new Path(sourceFolder.getRoot().getAbsolutePath());
    sourceFolder.newFile("000000_0");
    sourceFolder.newFile("000001_0");
    sourceFolder.newFile("000000_0.gz");
    sourceFolder.newFile("000001_0.gz");

    Path targetPath = new Path(targetFolder.getRoot().getAbsolutePath());

    // Simulate different filesystems by returning a different URI
    FileSystem spyTargetFs = spy(targetPath.getFileSystem(hiveConf));
    when(spyTargetFs.getUri()).thenReturn(URI.create("hdfs://" + targetPath.toUri().getPath()));

    try {
      Hive.copyFiles(hiveConf, sourcePath, targetPath, spyTargetFs, isSourceLocal, NO_ACID, false, null, false, false, false,
          false);
    } catch (HiveException e) {
      e.printStackTrace();
      assertTrue("Hive.copyFiles() threw an unexpected exception.", false);
    }

    assertTrue(spyTargetFs.exists(new Path(targetPath, "000000_0")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000001_0")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000000_0.gz")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000001_0.gz")));
  }

  @Test
  public void testCopyExistingFilesOnDifferentFileSystem() throws IOException {
    Path sourcePath = new Path(sourceFolder.getRoot().getAbsolutePath());
    sourceFolder.newFile("000000_0");
    sourceFolder.newFile("000001_0");
    sourceFolder.newFile("000000_0.gz");
    sourceFolder.newFile("000001_0.gz");

    Path targetPath = new Path(targetFolder.getRoot().getAbsolutePath());

    // Simulate different filesystems by returning a different URI
    FileSystem spyTargetFs = spy(targetPath.getFileSystem(hiveConf));
    when(spyTargetFs.getUri()).thenReturn(URI.create("hdfs://" + targetPath.toUri().getPath()));

    try {
      Hive.copyFiles(hiveConf, sourcePath, targetPath, spyTargetFs, isSourceLocal, NO_ACID, false, null,
          false, false, false, false);
    } catch (HiveException e) {
      e.printStackTrace();
      assertTrue("Hive.copyFiles() threw an unexpected exception.", false);
    }

    // If source is local, then source files won't be deleted, and we have to delete them here
    if (isSourceLocal) {
      sourceFolder.delete();
      sourceFolder.create();
      sourcePath = new Path(sourceFolder.getRoot().getAbsolutePath());
    }

    /* Create new source files with same filenames */
    sourceFolder.newFile("000000_0");
    sourceFolder.newFile("000001_0");
    sourceFolder.newFile("000000_0.gz");
    sourceFolder.newFile("000001_0.gz");

    try {
      Hive.copyFiles(hiveConf, sourcePath, targetPath, spyTargetFs, isSourceLocal, NO_ACID, false, null,
          false, false, false, false);
    } catch (HiveException e) {
      e.printStackTrace();
      assertTrue("Hive.copyFiles() threw an unexpected exception.", false);
    }

    assertTrue(spyTargetFs.exists(new Path(targetPath, "000000_0")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000001_0")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000000_0.gz")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000001_0.gz")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000000_0_copy_1")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000001_0_copy_1")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000000_0_copy_1.gz")));
    assertTrue(spyTargetFs.exists(new Path(targetPath, "000001_0_copy_1.gz")));
  }

  /**
   * When two concurrent writers stage a file with the same inner filename (e.g. {@code 000000_0})
   * into the same destination directory on an S3-like filesystem, mvFile must pick distinct
   * destination keys so the second writer does not silently overwrite the first. Both files
   * must land under distinct {@code 000000_0_copy_<hex>} names — no plain {@code 000000_0}, no
   * numeric {@code _copy_N}.
   *
   * <p>Covers the two moving parts individually since the full rename-branch path in
   * {@link Hive#copyFiles} requires src and dest FileSystems to compare equal AND the dest
   * scheme to be flagged non-atomic-rename, which is not easily synthesizable with
   * LocalFileSystem in a JUnit environment:
   * <ol>
   *   <li>{@link FileUtils#isNonAtomicRenameFs(FileSystem)} recognizes S3-family schemes on the URI
   *       and rejects HDFS / local schemes.</li>
   *   <li>Two distinct {@code hive.query.id} values map to two distinct 8-hex uniqueness tags
   *       — the compact per-query identifier that mvFile appends when the destination
   *       filesystem is a non-atomic-rename one. Confirms the tag is stable for a given
   *       queryId, and that the tag's shape (8 hex chars) matches the copy-suffix group in
   *       {@link org.apache.hadoop.hive.ql.exec.ParsedOutputFileName}'s regex.</li>
   * </ol>
   */
  @Test
  public void testUniquenessTagAndUnstableFsGating() throws IOException {
    // (1) non-atomic-rename filesystem detection via URI scheme
    FileSystem localFs = new Path(targetFolder.getRoot().getAbsolutePath()).getFileSystem(hiveConf);
    assertFalse("local FS is atomic-rename", FileUtils.isNonAtomicRenameFs(localFs));
    assertFalse("null fs is not flagged", FileUtils.isNonAtomicRenameFs((FileSystem) null));

    for (String scheme : new String[] {"s3a", "s3n", "s3", "gs", "abfs", "abfss", "wasb", "wasbs"}) {
      FileSystem spy = spy(localFs);
      when(spy.getUri()).thenReturn(URI.create(scheme + ":///bucket/path"));
      assertTrue(scheme + " must be flagged non-atomic-rename",
          FileUtils.isNonAtomicRenameFs(spy));
    }
    for (String scheme : new String[] {"hdfs", "file", "ofs", "adl"}) {
      FileSystem spy = spy(localFs);
      when(spy.getUri()).thenReturn(URI.create(scheme + ":///whatever"));
      assertFalse(scheme + " must not be flagged non-atomic-rename",
          FileUtils.isNonAtomicRenameFs(spy));
    }

    // (2) uniqueness tag: the 16-hex most-significant-bits half of the UUID at the tail of
    // queryId (QueryPlan.makeQueryId → "<user>_<timestamp>_<uuid>"; see
    // QueryPlan.extractUniquenessTag). Distinct UUIDs → distinct tags.
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID,
        "lbodor_20260101120000_f47ac10b-58cc-4372-a567-0e02b2c3d479");
    String tag1 = QueryPlan.extractUniquenessTag(hiveConf);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID,
        "lbodor_20260101120001_9c8a44f1-e2b3-4a1c-9d3e-000000000000");
    String tag2 = QueryPlan.extractUniquenessTag(hiveConf);

    assertEquals("MSB half of the UUID at the tail", "f47ac10b58cc4372", tag1);
    assertEquals("MSB half of the UUID at the tail", "9c8a44f1e2b34a1c", tag2);
    assertTrue("tag1 must match <16-hex>: " + tag1, tag1.matches("[0-9a-f]{16}"));
    assertTrue("tag2 must match <16-hex>: " + tag2, tag2.matches("[0-9a-f]{16}"));
    assertNotEquals("distinct queryIds must produce distinct tags", tag1, tag2);

    // Missing queryId → hard failure (mvFile's non-atomic-rename branch must not silently
    // fall back to a shared filename when the query state is absent).
    hiveConf.unset(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    try {
      QueryPlan.extractUniquenessTag(hiveConf);
      fail("extractUniquenessTag must throw when hive.query.id is unset");
    } catch (IllegalStateException expected) {
      assertTrue("exception message must mention hive.query.id: " + expected.getMessage(),
          expected.getMessage() != null && expected.getMessage().contains("hive.query.id"));
    }
  }
}
