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

package org.apache.hadoop.hive.ql.exec;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Helper class to match hive filenames and extract taskId, taskAttemptId, copyIndex.
 *
 * Matches following:
 * 00001_02
 * 00001_02.gz
 * 00001_02.zlib.gz
 * 00001_02_copy_1
 * 00001_02_copy_1.gz
 * <p>
 * All the components are here:
 * tmp_(taskPrefix)00001_02_copy_1.zlib.gz
 */
public class ParsedOutputFileName {
  private static final Pattern COPY_FILE_NAME_TO_TASK_ID_REGEX = Pattern.compile(
      "^(.*?)?" + // any prefix
      "(\\(.*\\))?" + // taskId prefix
      "([0-9]+)" + // taskId
      "(?:_([0-9]{1,6}))?" + // _<attemptId> (limited to 6 digits)
      "(?:_copy_([0-9]{1,6}))?" + // copy file index
      "(\\..*)?$"); // any suffix/file extension

  public static ParsedOutputFileName parse(String fileName) {
    return new ParsedOutputFileName(fileName);
  }

  private final Matcher m;
  private final boolean matches;

  private ParsedOutputFileName(CharSequence fileName) {
    m = COPY_FILE_NAME_TO_TASK_ID_REGEX.matcher(fileName);
    matches = m.matches();
  }

  public boolean matches() {
    return matches;
  }

  public String getTaskIdPrefix() {
    return m.group(2);
  }

  public String getTaskId() {
    return m.group(3);
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
    return m.group(4);
  }

  public boolean isCopyFile() {
    return m.group(5) != null;
  }

  public String getCopyIndex() {
    return m.group(5);
  }

  public String getSuffix() {
    return m.group(6);
  }

  /**
   * Create a copy file using the same file name as this and the given index. It will keep the prefixes but drop any
   * suffixes.
   * Ex: 00001_02 will be converted to 00001_02_copy_3 for idx = 3.
   * tmp_(prefix)00001_02_copy_1.snappy.orc will be converted to tmp_(prefix)00001_02_copy_3 for idx = 3
   * @param idx The index required.
   * @return
   */
  public String makeFilenameWithCopyIndex(int idx) {
    String orig = m.group(0);
    return orig.substring(0, m.end(4)) + "_copy_" + idx;
  }

  public String toString() {
    return "[taskId: " + getPrefixedTaskId() + ", taskAttemptId: " + getAttemptId() + ", copyIndex: " + getCopyIndex() + "]";
  }
}
