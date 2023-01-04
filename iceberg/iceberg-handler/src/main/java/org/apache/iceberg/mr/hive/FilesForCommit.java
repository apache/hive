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
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class FilesForCommit implements Serializable {

  private final Collection<DataFile> dataFiles;
  private final Collection<DeleteFile> deleteFiles;

  public FilesForCommit(Collection<DataFile> dataFiles, Collection<DeleteFile> deleteFiles) {
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
  }

  public static FilesForCommit onlyDelete(Collection<DeleteFile> deleteFiles) {
    return new FilesForCommit(Collections.emptyList(), deleteFiles);
  }

  public static FilesForCommit onlyData(Collection<DataFile> dataFiles) {
    return new FilesForCommit(dataFiles, Collections.emptyList());
  }

  public static FilesForCommit empty() {
    return new FilesForCommit(Collections.emptyList(), Collections.emptyList());
  }

  public Collection<DataFile> dataFiles() {
    return dataFiles;
  }

  public Collection<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  public Collection<? extends ContentFile> allFiles() {
    return Stream.concat(dataFiles.stream(), deleteFiles.stream()).collect(Collectors.toList());
  }

  public boolean isEmpty() {
    return dataFiles.isEmpty() && deleteFiles.isEmpty();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataFiles", dataFiles.toString())
        .add("deleteFiles", deleteFiles.toString())
        .toString();
  }
}
