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
package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Basic implementation of AcidUtils.Directory.
 * All files in the given path are original files and should be read by the queries.
 */
public final class OriginalDirectory implements  AcidUtils.Directory {

  List<AcidUtils.FileInfo> files;
  FileSystem fs;
  Path path;

  public OriginalDirectory(List<AcidUtils.FileInfo> files, FileSystem fs, Path path) {
    this.files = files;
    this.fs = fs;
    this.path = path;
  }

  @Override
  public List<AcidUtils.FileInfo> getFiles() throws IOException {
    return files;
  }

  @Override
  public FileSystem getFs() {
    return fs;
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public List<AcidUtils.ParsedDelta> getDeleteDeltas() {
    return Collections.EMPTY_LIST;
  }
}