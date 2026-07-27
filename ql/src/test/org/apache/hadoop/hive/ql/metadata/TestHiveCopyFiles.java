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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


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
    FileSystem spyTargetFs = Mockito.spy(targetPath.getFileSystem(hiveConf));
    Mockito.when(spyTargetFs.getUri()).thenReturn(URI.create("hdfs://" + targetPath.toUri().getPath()));

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
    FileSystem spyTargetFs = Mockito.spy(targetPath.getFileSystem(hiveConf));
    Mockito.when(spyTargetFs.getUri()).thenReturn(URI.create("hdfs://" + targetPath.toUri().getPath()));

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
   * HIVE-28822 (root-cause fix): when two concurrent writers stage a file with the same
   * inner filename (e.g. {@code 000000_0}) into the same destination directory on an S3-like
   * filesystem, mvFile must pick distinct destination keys so the second writer does not
   * silently overwrite the first. Both files must land under distinct
   * {@code 000000_0_<hex>} suffixed names — no plain {@code 000000_0}, no {@code _copy_N}.
   *
   * <p>Covers the two moving parts individually since the full rename-branch path in
   * {@link Hive#copyFiles} requires src and dest FileSystems to compare equal AND the dest
   * scheme to match {@link UnstableRenameFileSystem}, which is not easily synthesizable with
   * LocalFileSystem in a JUnit environment:
   * <ol>
   *   <li>{@link UnstableRenameFileSystem#matches(String)} recognizes S3-family schemes and
   *       rejects HDFS / local schemes.</li>
   *   <li>Two distinct {@code hive.query.id} values map to two distinct 8-hex uniqueness tags
   *       — the compact per-query identifier that mvFile appends when the destination
   *       filesystem is an unstable-rename one. Confirms the tag is stable for a given
   *       queryId, and that the tag's shape (8 hex chars) matches the extra group in
   *       {@link org.apache.hadoop.hive.ql.exec.ParsedOutputFileName}'s regex.</li>
   * </ol>
   */
  @Test
  public void testUniquenessTagAndUnstableFsGating() {
    // (1) enum gating
    assertTrue(UnstableRenameFileSystem.matches("s3a"));
    assertTrue(UnstableRenameFileSystem.matches("s3n"));
    assertTrue(UnstableRenameFileSystem.matches("s3"));
    assertTrue(UnstableRenameFileSystem.matches("gs"));
    assertFalse("hdfs is atomic-rename",
        UnstableRenameFileSystem.matches("hdfs"));
    assertFalse("local FS is atomic-rename",
        UnstableRenameFileSystem.matches("file"));
    assertFalse(UnstableRenameFileSystem.matches((String) null));
    assertFalse(UnstableRenameFileSystem.matches(""));

    // (2) uniqueness tag: distinct queryIds → distinct 8-hex tags, empty queryId → empty tag
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, "q1_lbodor_20260101_aaaaaaaa");
    String tag1 = Hive.computeUniquenessTag(hiveConf);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, "q2_lbodor_20260101_bbbbbbbb");
    String tag2 = Hive.computeUniquenessTag(hiveConf);
    hiveConf.unset(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    String tagEmpty = Hive.computeUniquenessTag(hiveConf);

    assertTrue("tag1 must match <8-hex>: " + tag1, tag1.matches("[0-9a-f]{8}"));
    assertTrue("tag2 must match <8-hex>: " + tag2, tag2.matches("[0-9a-f]{8}"));
    assertNotEquals("distinct queryIds must produce distinct tags", tag1, tag2);
    assertEquals("empty queryId → empty tag", "", tagEmpty);
  }
}
