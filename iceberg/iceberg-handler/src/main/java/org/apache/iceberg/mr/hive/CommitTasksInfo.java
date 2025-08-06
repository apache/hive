/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class CommitTasksInfo implements Serializable {

  private List<String> commitTaskFiles;

  public static final String COMMIT_TASKS_INFO_FILE_EXTENSION = ".forCommitTasksInfo";

  public CommitTasksInfo(List<String> commitTaskFiles) {
    this.commitTaskFiles = commitTaskFiles;
  }

  public void addFile(String commitTaskFile) {
    commitTaskFiles.add(commitTaskFile);
  }

  public void removeFile(String commitTaskFile) {
    commitTaskFiles.remove(commitTaskFile);
  }

  public List<String> getCommitTaskFiles() {
    return commitTaskFiles;
  }

  public boolean isEmpty() {
    return commitTaskFiles.isEmpty();
  }

  public boolean containsCommitTaskFile(String commitTaskFile) {
    return commitTaskFiles.contains(commitTaskFile);
  }

  public boolean containsCommitTaskDirectory(String commitTaskDirectory) {
    for (String commitTaskFile : commitTaskFiles) {
      if (commitTaskFile.startsWith(commitTaskDirectory)) {
        return true;
      }
    }
    return false;
  }

  public void removeFilesWithDirectory(String commitTaskDirectory) {
    commitTaskFiles.removeIf(commitFile -> commitFile.startsWith(commitTaskDirectory));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("commitTaskFiles", commitTaskFiles.toString())
        .toString();
  }

}
