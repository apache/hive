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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Two inserts move a staged {@code 000000_0} into the same directory. Both pass the
 * destination {@code exists()} check before either renames, which is the HDFS race
 * {@link Hive#copyFiles} must recover from by publishing the loser as {@code _copy_N}.
 */
public class TestHiveCopyFilesConcurrentInsert {

  private static final int WRITERS = 2;
  private static final String STAGED_FILE_NAME = "000000_0";

  private static HiveConf hiveConf;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  public static void setUpClass() {
    hiveConf = new HiveConfForTest(TestHiveCopyFilesConcurrentInsert.class);
    SessionState.start(hiveConf);
  }

  @Before
  public void setUp() {
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT, 0);
  }

  @Test
  public void testBaseNameToCopy1() throws Exception {
    Set<String> names = moveBoth(STAGED_FILE_NAME);
    assertEquals(names.toString(), WRITERS, names.size());
    assertTrue(names.contains(STAGED_FILE_NAME));
    assertTrue(names.contains(STAGED_FILE_NAME + "_copy_1"));
  }

  /**
   * The destination already holds {@code 000000_0} and {@code 000000_0_copy_1}, so both writers
   * choose {@code 000000_0_copy_2}. The loser of that rename must be published as
   * {@code 000000_0_copy_3}.
   */
  @Test
  public void testAdvancementToNextCopy() throws Exception {
    String copy2 = STAGED_FILE_NAME + "_copy_2";
    Set<String> names = moveBoth(copy2, STAGED_FILE_NAME, STAGED_FILE_NAME + "_copy_1");
    assertEquals(names.toString(),
        new HashSet<>(Arrays.asList(
            STAGED_FILE_NAME, STAGED_FILE_NAME + "_copy_1", copy2, STAGED_FILE_NAME + "_copy_3")),
        names);
  }

  /**
   * Both writers stage {@code 000000_0}. {@code collisionName} is the destination file they both
   * find missing and then rename to. {@code alreadyPublished} are files placed in the table
   * directory first, so the name picker starts at a later {@code _copy_N}.
   */
  private Set<String> moveBoth(String collisionName, String... alreadyPublished) throws Exception {
    Path tableDir = new Path(tmp.newFolder("target").getAbsolutePath());
    FileSystem fs = new HdfsRenameFileSystem(collisionName);
    fs.initialize(URI.create("file:///"), hiveConf);
    assertFalse(FileUtils.isNonAtomicRenameFs(fs));
    // create() calls exists() on the new file. Use a plain local filesystem so that
    // check does not wait for the other writer.
    RawLocalFileSystem local = new RawLocalFileSystem();
    local.initialize(URI.create("file:///"), hiveConf);
    for (String name : alreadyPublished) {
      local.create(new Path(tableDir, name)).close();
    }
    Path[] stagingDirs = new Path[WRITERS];
    for (int i = 0; i < WRITERS; i++) {
      stagingDirs[i] = new Path(tmp.newFolder("staging" + i).getAbsolutePath());
      local.create(new Path(stagingDirs[i], STAGED_FILE_NAME)).close();
    }

    SessionState parentSession = SessionState.get();
    ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
    List<Future<Void>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < WRITERS; i++) {
        final Path stagingDir = stagingDirs[i];
        futures.add(pool.submit(() -> {
          SessionState.setCurrentSessionState(parentSession);
          Hive.copyFiles(hiveConf, stagingDir, tableDir, fs, false, false, false, null,
              false, false, false, false);
          return null;
        }));
      }

      int reportedSuccess = 0;
      List<Integer> silentlyLost = new ArrayList<>();
      for (int i = 0; i < WRITERS; i++) {
        try {
          futures.get(i).get(60, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
          continue;
        }
        reportedSuccess++;
        if (local.exists(new Path(stagingDirs[i], STAGED_FILE_NAME))) {
          silentlyLost.add(i);
        }
      }

      FileStatus[] published = fs.listStatus(tableDir, FileUtils.HIDDEN_FILES_PATH_FILTER);
      Set<String> names = new HashSet<>();
      for (FileStatus status : published) {
        names.add(status.getPath().getName());
      }

      assertEquals(names.toString(), WRITERS, reportedSuccess);
      assertTrue(silentlyLost + " still in staging; published " + names, silentlyLost.isEmpty());
      assertEquals(names.toString(), published.length, names.size());
      return names;
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  /**
   * Local files with HDFS rename: an existing destination returns false and the source stays
   * put. A missing {@code collisionName} waits until both writers have looked.
   */
  private static final class HdfsRenameFileSystem extends RawLocalFileSystem {
    private final String collisionName;
    private final CountDownLatch probed = new CountDownLatch(WRITERS);

    HdfsRenameFileSystem(String collisionName) {
      this.collisionName = collisionName;
    }

    @Override
    public boolean exists(Path path) throws IOException {
      boolean present = super.exists(path);
      if (!present && collisionName.equals(path.getName())) {
        probed.countDown();
        try {
          if (!probed.await(30, TimeUnit.SECONDS)) {
            throw new IOException("writers did not all reach the destination probe");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      return present;
    }

    @Override
    public synchronized boolean rename(Path src, Path dst) throws IOException {
      if (super.exists(dst)) {
        return false;
      }
      return super.rename(src, dst);
    }
  }
}
