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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

public class ParsedOutputFileNameTest {
  @Test
  public void testStandardNoAttemptId() {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertNull(p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
  }

  @Test
  public void testStandard() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001_02");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    Assert.assertEquals("00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testStandardPrefix() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("(prefix)00001_02");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("(prefix)", p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("(prefix)00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    Assert.assertEquals("(prefix)00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testStandardSuffix() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001_02.snappy.orc");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertEquals(".snappy.orc", p.getSuffix());
    Assert.assertEquals("00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testPrefixAndSuffix() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("tmp_(prefix)00001_02.snappy.orc");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("(prefix)", p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("(prefix)00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertEquals(".snappy.orc", p.getSuffix());
    Assert.assertEquals("tmp_(prefix)00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testCopy() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001_02_copy_3");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertEquals("3", p.getCopyIndex());
    Assert.assertTrue(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    Assert.assertEquals("00001_02_copy_4", p.makeFilenameWithCopyIndex(4));
  }

  @Test
  public void testCopyAllParts() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("tmp_(prefix)00001_02_copy_3.snappy.orc");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("(prefix)", p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("(prefix)00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertEquals("3", p.getCopyIndex());
    Assert.assertTrue(p.isCopyFile());
    Assert.assertEquals(".snappy.orc", p.getSuffix());
    Assert.assertEquals("tmp_(prefix)00001_02_copy_4", p.makeFilenameWithCopyIndex(4));
  }

  /**
   * On filesystems without atomic rename-if-absent semantics (S3 etc.), the copy suffix
   * carries a 16-hex per-query uniqueness tag instead of the numeric counter, so concurrent
   * writers rename to distinct destination keys.
   */
  @Test
  public void testUniquenessTagAsCopySuffix() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("000001_0_copy_abcd1234deadbeef");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("000001", p.getTaskId());
    Assert.assertEquals("0", p.getAttemptId());
    Assert.assertEquals("abcd1234deadbeef", p.getCopyIndex());
    Assert.assertTrue(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    // Numeric-index renaming (used by legacy code paths) still works and replaces the tag.
    Assert.assertEquals("000001_0_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testUniquenessTagAsCopySuffixWithExtension() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("000001_0_copy_abcd1234deadbeef.snappy.orc");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("000001", p.getTaskId());
    Assert.assertEquals("0", p.getAttemptId());
    Assert.assertEquals("abcd1234deadbeef", p.getCopyIndex());
    Assert.assertTrue(p.isCopyFile());
    Assert.assertEquals(".snappy.orc", p.getSuffix());
    Assert.assertEquals("000001_0_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  /**
   * The copy-index group must reject shapes that are neither a 1..6 digit counter nor an
   * exactly-16-hex tag (e.g. non-hex characters, or a numeric tag longer than 6 digits).
   */
  @Test
  public void testUniquenessTagShapeIsStrict() {
    // 15 chars — matches neither branch.
    Assert.assertNull(ParsedOutputFileName.parse("000001_0_copy_abcd1234deadbee").getCopyIndex());
    // Non-hex character in a 16-char position.
    Assert.assertNull(ParsedOutputFileName.parse("000001_0_copy_abcd1234deadbeez").getCopyIndex());
  }

  /**
   * {@link ParsedOutputFileName#withFoldedSubdirIndex(int)} encodes the
   * {@code HIVE_UNION_SUBDIR_<N>} index into the attempt-id portion of the writer
   * name via {@code newAttempt = subdirIdx * 100000 + originalAttempt}. Called by
   * {@link MoveTask#flattenUnionSubdirectories(org.apache.hadoop.fs.Path)} to hoist
   * union-branch leaves into the parent directory without ever touching the
   * {@code _copy_} suffix — so the result stays in the plain writer-name namespace
   * ({@code [0-9]+_[0-9]+}) and composes cleanly with a subsequent
   * {@code _copy_<HIVE-28822 uniqueness tag>} that {@code Hive.pickDestFilePath}
   * may append on a non-atomic-rename FS.
   */
  @Test
  public void testWithFoldedSubdirIndex() throws Exception {
    // subdirIdx=1, attempt=0 -> 100000. Matches the single-digit-subdir example
    // in the JIRA discussion.
    Assert.assertEquals("000000_100000",
        ParsedOutputFileName.parse("000000_0").withFoldedSubdirIndex(1));
    // subdirIdx=23, attempt=2 -> 2300002. Two-digit subdir; result exceeds the
    // legacy 6-digit attempt-id cap, which is why COPY_FILE_NAME_TO_TASK_ID_REGEX
    // widens the attempt group to 10 digits.
    Assert.assertEquals("000000_2300002",
        ParsedOutputFileName.parse("000000_2").withFoldedSubdirIndex(23));
    // Zero subdirIdx acts as the identity for the attempt id — this branch never
    // fires in practice (HIVE_UNION_SUBDIR_ is 1-indexed) but the arithmetic is
    // still well-defined.
    Assert.assertEquals("000000_42",
        ParsedOutputFileName.parse("000000_42").withFoldedSubdirIndex(0));
  }

  @Test
  public void testWithFoldedSubdirIndexPreservesSuffix() throws Exception {
    // File-extension suffix (single- or multi-part) is preserved verbatim.
    Assert.assertEquals("000000_500003.gz",
        ParsedOutputFileName.parse("000000_3.gz").withFoldedSubdirIndex(5));
    Assert.assertEquals("000000_800001.snappy.orc",
        ParsedOutputFileName.parse("000000_1.snappy.orc").withFoldedSubdirIndex(8));
  }

  @Test
  public void testWithFoldedSubdirIndexPreservesTaskIdPrefix() throws Exception {
    // Any "(prefix)" wrapper on the taskId (used for MoveTask staging names) is
    // preserved so downstream consumers still see the original prefix.
    Assert.assertEquals("(prefix)00001_200002",
        ParsedOutputFileName.parse("(prefix)00001_2").withFoldedSubdirIndex(2));
    Assert.assertEquals("tmp_(prefix)00001_400002",
        ParsedOutputFileName.parse("tmp_(prefix)00001_2").withFoldedSubdirIndex(4));
  }

  @Test
  public void testWithFoldedSubdirIndexOnBareTaskId() throws Exception {
    // A bare taskId (no `_<attemptId>`) is treated as attempt=0.
    Assert.assertEquals("000000_700000",
        ParsedOutputFileName.parse("000000").withFoldedSubdirIndex(7));
  }

  @Test
  public void testWithFoldedSubdirIndexDropsCopySuffix() throws Exception {
    // A pre-existing `_copy_<N>` on the source is dropped: flatten is not a copy,
    // and the flattened name must stay in the plain-writer-name namespace so a
    // subsequent Hive.pickDestFilePath can append its own uniqueness tag on top.
    Assert.assertEquals("000000_100002",
        ParsedOutputFileName.parse("000000_2_copy_9").withFoldedSubdirIndex(1));
  }

  @Test
  public void testWithFoldedSubdirIndexOnUnparseableThrows() {
    ParsedOutputFileName p = ParsedOutputFileName.parse("ZfsLke");
    Assert.assertFalse(p.matches());
    try {
      p.withFoldedSubdirIndex(1);
      Assert.fail("Expected HiveException on unparseable filename");
    } catch (HiveException expected) {
    }
  }

  /**
   * The attempt-id group was widened from {@code \d{1,6}} to {@code \d{1,10}} so
   * a flattened name produced by {@link ParsedOutputFileName#withFoldedSubdirIndex(int)}
   * — up to 10 digits for subdir indices in the hundreds — still parses back.
   */
  @Test
  public void testWideAttemptIdParses() {
    ParsedOutputFileName p = ParsedOutputFileName.parse("000000_2300002");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("000000", p.getTaskId());
    Assert.assertEquals("2300002", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
  }

  @Test
  public void testNoMatch() {
    ParsedOutputFileName p = ParsedOutputFileName.parse("ZfsLke");
    Assert.assertFalse(p.matches());
    Assert.assertNull(p.getTaskId());
    Assert.assertNull(p.getPrefixedTaskId());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertNull(p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    try {
      p.makeFilenameWithCopyIndex(1);
      Assert.fail("Expected HiveException");
    } catch(HiveException e) {
    }
  }
}
