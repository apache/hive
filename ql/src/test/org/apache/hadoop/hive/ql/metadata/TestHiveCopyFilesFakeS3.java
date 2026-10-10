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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.exec.ParsedOutputFileName;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.FakeS3FileSystem;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end tests for {@link Hive#copyFiles} through the non-atomic-rename-FS branch of
 * {@link Hive}'s move logic. Registers a synthetic {@code fakes3://} scheme backed by
 * {@link RawLocalFileSystem} (so real files land under a JUnit {@link TemporaryFolder})
 * and appends it directly to {@link FileUtils#NON_ATOMIC_RENAME_SCHEMES} for the duration of
 * this test class so {@link FileUtils#isNonAtomicRenameFs} treats it as a non-atomic-rename
 * FS. The scheme is removed in {@link #tearDownClass()} so no other test sees it.
 *
 * <p>What this covers that the mockito-spy tests in {@link TestHiveCopyFiles} do not:
 * an actual rename() call is made through the tag-suffix branch of
 * {@link Hive#pickDestFilePath}, and the resulting on-disk layout is asserted.
 */
class TestHiveCopyFilesFakeS3 {

  /** Scheme registered as {@code fs.fakes3.impl} for the duration of these tests. */
  private static final String FAKE_SCHEME = FakeS3FileSystem.SCHEME;

  private static HiveConf hiveConf;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  public static void setUpClass() {
    hiveConf = new HiveConfForTest(TestHiveCopyFilesFakeS3.class);
    // Register the fake scheme's FileSystem impl. Cache off so each test gets a fresh
    // instance rooted under its own TemporaryFolder without cross-test leakage.
    hiveConf.setClass("fs." + FAKE_SCHEME + ".impl", FakeS3FileSystem.class, FileSystem.class);
    hiveConf.setBoolean("fs." + FAKE_SCHEME + ".impl.disable.cache", true);
    // Have FileUtils.isNonAtomicRenameFs treat our fake scheme as a non-atomic-rename FS.
    // This is a JVM-global mutation of a production static — we undo it in tearDownClass
    // so no test that runs after this class sees fakes3 in the set.
    FileUtils.NON_ATOMIC_RENAME_SCHEMES.add(FAKE_SCHEME);
    SessionState.start(hiveConf);
  }

  @AfterClass
  public static void tearDownClass() {
    FileUtils.NON_ATOMIC_RENAME_SCHEMES.remove(FAKE_SCHEME);
  }

  @Before
  public void setUp() {
    // Every test needs a fresh hive.query.id so computeUniquenessTag produces a real tag.
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID,
        "test_" + System.nanoTime() + "_f47ac10b-58cc-4372-a567-0e02b2c3d479");
  }

  /**
   * Builds a Path in the {@code fakes3://} namespace that points at the given local
   * subdirectory of the JUnit temp root. We use the local path as the URI path so the
   * underlying RawLocalFileSystem writes/reads real files there.
   */
  private Path fakes3Path(String subdir) throws IOException {
    java.io.File dir = tmp.newFolder(subdir);
    return new Path(URI.create(FAKE_SCHEME + "://" + dir.getAbsolutePath()));
  }

  /**
   * fakes3 must be recognized as a non-atomic-rename FS once
   * {@link #setUpClass()} has appended it to
   * {@link FileUtils#NON_ATOMIC_RENAME_SCHEMES}; a plain {@code file://} filesystem
   * must not be.
   */
  @Test
  public void fakes3IsFlaggedNonAtomicRename() throws IOException {
    Path fakePath = fakes3Path("gate");
    FileSystem fakeFs = fakePath.getFileSystem(hiveConf);

    assertEquals(FAKE_SCHEME, fakeFs.getUri().getScheme());
    assertTrue("fakes3 must be non-atomic-rename", FileUtils.isNonAtomicRenameFs(fakeFs));

    FileSystem localFs = new Path(tmp.getRoot().getAbsolutePath()).getFileSystem(hiveConf);
    assertFalse("local FS must not be flagged", FileUtils.isNonAtomicRenameFs(localFs));
  }

  /**
   * A single-file rename into a fresh destination under {@code fakes3://} must land at
   * {@code <name>_copy_<16-hex>} (uniqueness-tag suffix), not at {@code <name>} — even
   * though the target directory is empty. This is the whole point of the tag branch:
   * skip the exists() probe and stamp the name unconditionally so concurrent writers
   * cannot race on the same key.
   */
  @Test
  public void singleFileRenameUsesUniquenessTagSuffix() throws Exception {
    Path srcDir = fakes3Path("src");
    Path dstDir = fakes3Path("dst");
    FileSystem fs = dstDir.getFileSystem(hiveConf);
    fs.create(new Path(srcDir, "000000_0")).close();

    // fakes3 is flagged non-atomic-rename, so Hive.pickDestFilePath takes the
    // tag branch: append _copy_<16-hex> unconditionally, skipping the exists() probe.
    Hive.copyFiles(hiveConf, srcDir, dstDir, fs, false, false, false, null,
        false, false, false, false);

    FileStatus[] listed = fs.listStatus(dstDir);
    assertEquals("one output file expected", 1, listed.length);
    String name = listed[0].getPath().getName();
    // Reuse the production regexes from AcidUtils so the test tracks whatever
    // the readers on the other end accept, and tighten to require the 16-hex
    // uniqueness tag (not the numeric _copy_N fallback).
    assertHas16HexUniquenessTag(name);
    assertFalse("no plain 000000_N leaf allowed on non-atomic-rename FS: " + name,
        AcidUtils.ORIGINAL_PATTERN.matcher(name).matches());
  }

  /**
   * Two staged files that share an inner filename must land at two distinct
   * {@code _copy_<16-hex>} keys — never at plain {@code 000000_0} vs
   * {@code 000000_0_copy_1}. Each of the two copyFiles calls runs under its own
   * hive.query.id, so the two tags differ and the two on-disk keys must differ
   * too. That's the property that guards against silent overwrite on S3A.
   */
  @Test
  public void twoStagedFilesLandAtDistinctKeys() throws Exception {
    Path srcDir = fakes3Path("src");
    Path dstDir = fakes3Path("dst");
    FileSystem fs = dstDir.getFileSystem(hiveConf);

    // Call #1 — queryId T1 set by @Before.
    fs.create(new Path(srcDir, "000000_0")).close();
    Hive.copyFiles(hiveConf, srcDir, dstDir, fs, false, false, false, null,
        false, false, false, false);

    // Stage a second file with the same base name and copy again — since srcDir was
    // consumed by the previous move, recreate the source folder.
    fs.mkdirs(srcDir);
    fs.create(new Path(srcDir, "000000_0")).close();
    // Call #2 — different queryId → different tag → distinct destination name.
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID,
        "test_" + System.nanoTime() + "_9c8a44f1-e2b3-4a1c-9d3e-000000000000");
    Hive.copyFiles(hiveConf, srcDir, dstDir, fs, false, false, false, null,
        false, false, false, false);

    FileStatus[] listed = fs.listStatus(dstDir);
    assertEquals("two output files expected", 2, listed.length);
    for (FileStatus s : listed) {
      String name = s.getPath().getName();
      // Nothing may be named plain 000000_N — that's the silent-overwrite failure mode.
      assertFalse("no plain 000000_N leaf allowed on non-atomic-rename FS: " + name,
          AcidUtils.ORIGINAL_PATTERN.matcher(name).matches());
      assertHas16HexUniquenessTag(name);
    }
    assertNotEquals("two writers must land at distinct keys",
        listed[0].getPath().getName(), listed[1].getPath().getName());
  }

  /**
   * Ten threads all call {@link Hive#copyFiles} concurrently into the SAME
   * destination directory on {@code fakes3://}, each staging its own
   * {@code 000000_0} under a per-thread source directory. Each thread runs with
   * its own {@link HiveConf} clone and its own {@code hive.query.id}, so
   * {@link Hive#computeUniquenessTag} produces ten distinct 16-hex tags. The
   * invariants:
   * <ol>
   *   <li>Exactly 10 files end up in the destination (nothing was silently
   *       overwritten on rename).</li>
   *   <li>Every filename matches {@link AcidUtils#ORIGINAL_PATTERN_COPY}
   *       (the {@code _copy_<tag>} shape) and none matches
   *       {@link AcidUtils#ORIGINAL_PATTERN} (no plain {@code 000000_N}).</li>
   *   <li>All 10 filenames are distinct.</li>
   * </ol>
   * This is the actual multi-writer race the tag branch of
   * {@link Hive#pickDestFilePath} is designed to protect against on S3A —
   * driven from Java threads directly rather than through the planner.
   */
  @Test
  public void tenConcurrentCopiesLandAtDistinctTaggedKeys() throws Exception {
    final int threads = 10;
    // Shared destination for every writer — this is what makes it a race.
    final Path dstDir = fakes3Path("dst");
    final FileSystem fs = dstDir.getFileSystem(hiveConf);

    // Stage each thread's source in its own directory. All ten source files
    // are named 000000_0 — the collision we're testing is on the DESTINATION
    // side, where the tag branch is expected to make the ten same-named
    // inputs land at ten distinct keys. The per-thread source dir is a
    // mechanical necessity (rename() consumes its source, so multiple threads
    // can't share one physical file), not a collision-avoidance measure.
    final Path[] srcDirs = new Path[threads];
    final HiveConf[] confs = new HiveConf[threads];
    for (int i = 0; i < threads; i++) {
      srcDirs[i] = fakes3Path("src" + i);
      fs.create(new Path(srcDirs[i], "000000_0")).close();

      confs[i] = new HiveConf(hiveConf);
      confs[i].setVar(HiveConf.ConfVars.HIVE_QUERY_ID,
          "test_concurrent_" + i + "_" + java.util.UUID.randomUUID());
    }

    final SessionState parentSession = SessionState.get();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Void>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < threads; i++) {
        final int idx = i;
        futures.add(pool.submit(() -> {
          SessionState.setCurrentSessionState(parentSession);
          Hive.copyFiles(confs[idx], srcDirs[idx], dstDir, fs, false, false, false, null,
              false, false, false, false);
          return null;
        }));
      }
      for (Future<Void> f : futures) {
        // Surface the first exception rather than swallowing it as a timeout.
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdown();
      assertTrue("executor did not terminate", pool.awaitTermination(30, TimeUnit.SECONDS));
    }

    FileStatus[] listed = fs.listStatus(dstDir);
    assertEquals("expected exactly " + threads + " output files, got: "
            + java.util.Arrays.toString(listed), threads, listed.length);
    Set<String> names = new HashSet<>();
    for (FileStatus s : listed) {
      String name = s.getPath().getName();
      assertFalse("no plain 000000_N leaf allowed on non-atomic-rename FS: " + name,
          AcidUtils.ORIGINAL_PATTERN.matcher(name).matches());
      assertHas16HexUniquenessTag(name);
      assertTrue("duplicate destination filename after concurrent copyFiles: " + name,
          names.add(name));
    }
  }

  /**
   * Asserts that {@code name} is a copy-suffixed filename whose suffix is the
   * 16-hex per-query uniqueness tag (as opposed to the numeric {@code _copy_N}
   * fallback that AcidUtils.ORIGINAL_PATTERN_COPY also accepts). Parses via
   * ParsedOutputFileName so we track whatever it accepts.
   */
  private static void assertHas16HexUniquenessTag(String name) {
    // Baseline: must match the copy pattern the readers accept.
    assertTrue("must match _copy_ pattern, got: " + name,
        AcidUtils.ORIGINAL_PATTERN_COPY.matcher(name).matches());
    // Tighten: the suffix must be exactly 16 hex chars, not the numeric fallback.
    ParsedOutputFileName parsed = ParsedOutputFileName.parse(name);
    assertTrue("ParsedOutputFileName should recognize " + name, parsed.matches());
    assertTrue("expected copy suffix on " + name, parsed.isCopyFile());
    String tag = parsed.getCopyIndex();
    assertEquals("copy suffix must be 16 chars long (uniqueness tag, not numeric fallback), got: " + tag,
        16, tag.length());
    assertTrue("copy suffix must be lowercase-or-uppercase hex, got: " + tag,
        tag.matches("[0-9a-fA-F]{16}"));
  }
}
