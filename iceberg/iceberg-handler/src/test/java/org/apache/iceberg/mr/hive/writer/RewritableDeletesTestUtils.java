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

package org.apache.iceberg.mr.hive.writer;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;

/**
 * Reads the files that the collectors of a query's scans wrote, for tests.
 */
public final class RewritableDeletesTestUtils {

  private RewritableDeletesTestUtils() {
  }

  /**
   * Returns the files that collectors wrote under the root dir, with the deletes in each.
   */
  public static Map<Path, Map<String, DeleteFileSet>> collectorFiles(Path root, Configuration conf)
      throws IOException {
    Map<Path, Map<String, DeleteFileSet>> collectorFiles = Maps.newHashMap();
    FileSystem fs = root.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
    while (files.hasNext()) {
      Path file = files.next().getPath();
      if (isCollectorFile(file)) {
        collectorFiles.put(file, RewritableDeletes.read(fs, file));
      }
    }
    return collectorFiles;
  }

  // a Collector file: <table name>-rewritable-deletes/<snapshot id>/<uuid>.gz
  private static boolean isCollectorFile(Path file) {
    Path tableDir = file.getParent().getParent();
    return tableDir != null && tableDir.getName().endsWith(RewritableDeletes.DIR_SUFFIX);
  }
}
