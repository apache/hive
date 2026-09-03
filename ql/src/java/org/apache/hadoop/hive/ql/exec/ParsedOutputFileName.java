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

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Helper class to match hive filenames and extract taskId, taskAttemptId, copyIndex.
 *
 * Matches following:
 * 00001_02
 * 00001_02.gz
 * 00001_02.zlib.gz
 * 00001_02_copy_1                            (numeric copy suffix, HDFS-style)
 * 00001_02_copy_1.gz
 * 00001_02_copy_abcd1234deadbeef             (per-query uniqueness tag as copy suffix,
 *                                             used on non-atomic-rename filesystems)
 * 00001_02_copy_abcd1234deadbeef.gz
 * <p>
 * All the components are here:
 * tmp_(taskPrefix)00001_02_copy_1.zlib.gz
 */
public class ParsedOutputFileName {
  private static final Pattern COPY_FILE_NAME_TO_TASK_ID_REGEX = Pattern.compile(
      "^(.*?)?" + // any prefix
      "(\\(.*\\))?" + // taskId prefix
      "(\\d+)" + // taskId
      "(?:_(\\d{1,10}))?" + // _<attemptId>
      // Cap raised from 6 to 10 digits so MoveTask.foldSubdirIntoAttemptId can
      // encode a HIVE_UNION_SUBDIR_<N> index in the attempt-id namespace as
      // `subdirIdx * 100000 + originalAttempt` without needing a copy suffix.
      "(?:_copy_(\\d{1,6}|[0-9a-fA-F]{16}))?" + // copy suffix: numeric counter, or 16-hex uniqueness tag
      "(\\..*)?$"); // any suffix/file extension

  public static ParsedOutputFileName parse(String fileName) {
    return new ParsedOutputFileName(fileName);
  }

  private final boolean matches;
  private final String taskIdPrefix;
  private final String taskId;
  private final String attemptId;
  private final String copyIndex;
  private final String suffix;
  private final CharSequence filePrefixForCopy;
  // Everything before the taskId (group 1 + group 2, if any) — includes any
  // "tmp_" style leading prefix that taskIdPrefix on its own would drop.
  private final CharSequence preTaskIdPrefix;

  private ParsedOutputFileName(CharSequence fileName) {
    Matcher m = COPY_FILE_NAME_TO_TASK_ID_REGEX.matcher(fileName);
    matches = m.matches();
    if (matches) {
      taskIdPrefix = m.group(2);
      taskId = m.group(3);
      attemptId = m.group(4);
      copyIndex = m.group(5);
      suffix = m.group(6);
      filePrefixForCopy = m.end(4) >= 0 ? fileName.subSequence(0, m.end(4)) : null;
      preTaskIdPrefix = fileName.subSequence(0, m.start(3));
    } else {
      taskIdPrefix = null;
      taskId = null;
      attemptId = null;
      copyIndex = null;
      suffix = null;
      filePrefixForCopy = null;
      preTaskIdPrefix = null;
    }
  }

  public boolean matches() {
    return matches;
  }

  public String getTaskIdPrefix() {
    return taskIdPrefix;
  }

  public String getTaskId() {
    return taskId;
  }

  public String getPrefixedTaskId() {
    String prefix = getTaskIdPrefix();
    String taskId = getTaskId();
    if (prefix != null && taskId != null) {
      return prefix + taskId;
    } else {
      return taskId;
    }
  }

  public String getAttemptId() {
    return attemptId;
  }

  public boolean isCopyFile() {
    return copyIndex != null;
  }

  /**
   * @return the copy suffix: either a numeric counter (HDFS-style) or an 8-hex per-query
   *         uniqueness tag (used on non-atomic-rename filesystems), or {@code null} when the
   *         filename has no copy suffix.
   */
  public String getCopyIndex() {
    return copyIndex;
  }

  public String getSuffix() {
    return suffix;
  }

  /**
   * Create a copy file using the same file name as this and the given index. It will keep the prefixes but drop any
   * suffixes.
   * Ex: 00001_02 will be converted to 00001_02_copy_3 for idx = 3.
   * tmp_(prefix)00001_02_copy_1.snappy.orc will be converted to tmp_(prefix)00001_02_copy_3 for idx = 3
   * @param idx The index required.
   * @return
   */
  public String makeFilenameWithCopyIndex(int idx) throws HiveException {
    if (filePrefixForCopy == null) {
      throw new HiveException("Not expected format for copying files.");
    }
    return filePrefixForCopy + "_copy_" + idx;
  }

  /**
   * Return a new filename with the given {@code subdirIdx} folded into the attempt-id
   * portion: {@code newAttempt = subdirIdx * 100000 + originalAttempt}. TaskId prefix
   * and file-extension suffix are preserved; any {@code _copy_} suffix on the source
   * is dropped (this is a flatten operation, not a copy).
   *
   * <p>Called by {@link org.apache.hadoop.hive.ql.exec.MoveTask#flattenUnionSubdirectories(org.apache.hadoop.fs.Path)}
   * when hoisting leaves out of {@code HIVE_UNION_SUBDIR_<subdirIdx>/} into the parent
   * directory. Folding into the writer-name namespace ({@code [0-9]+_[0-9]+}) keeps
   * the flattened name out of the {@code _copy_<HIVE-28822 uniqueness tag>} namespace
   * that {@link org.apache.hadoop.hive.ql.metadata.Hive#pickDestFilePath} may later
   * add on a non-atomic-rename FS — so the two mechanisms compose cleanly.
   *
   * <p>Examples:
   * <pre>
   *   000000_0,      subdirIdx=1  -> 000000_100000
   *   000000_2,      subdirIdx=23 -> 000000_2300002
   *   000000_3.gz,   subdirIdx=5  -> 000000_500003.gz
   *   000000,        subdirIdx=7  -> 000000_700000
   * </pre>
   *
   * @param subdirIdx the {@code HIVE_UNION_SUBDIR_<N>} index to fold in
   * @return the flattened name
   * @throws HiveException if the source name is not in a recognized writer shape
   */
  public String withFoldedSubdirIndex(int subdirIdx) throws HiveException {
    if (!matches || taskId == null) {
      throw new HiveException("Cannot fold subdir into attempt id; unexpected filename shape.");
    }
    long originalAttempt = attemptId != null ? Long.parseLong(attemptId) : 0L;
    long newAttempt = ((long) subdirIdx) * 100_000L + originalAttempt;
    String s = suffix != null ? suffix : "";
    // preTaskIdPrefix includes any leading "tmp_" AND the "(prefix)" wrapper, so the
    // rebuilt name matches what makeFilenameWithCopyIndex preserves.
    return preTaskIdPrefix + taskId + "_" + newAttempt + s;
  }

  public String toString() {
    return "[taskId: " + getPrefixedTaskId() + ", taskAttemptId: " + getAttemptId() +
        ", copyIndex: " + getCopyIndex() + "]";
  }
}
