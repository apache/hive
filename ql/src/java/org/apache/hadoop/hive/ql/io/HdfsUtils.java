/**
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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;

public class HdfsUtils {
  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);

  public static Long getFileId(
      FileSystem fileSystem, Path path, boolean allowSynthetic) throws IOException {
    String pathStr = path.toUri().getPath();
    if (fileSystem instanceof DistributedFileSystem) {
      return SHIMS.getFileId(fileSystem, pathStr);
    }
    if (!allowSynthetic) {
      LOG.warn("Cannot get unique file ID from "
        + fileSystem.getClass().getSimpleName() + "; returning null");
      return null;
    }
    // If we are not on DFS, we just hash the file name + size and hope for the best.
    // TODO: we assume it only happens in tests. Fix?
    int nameHash = pathStr.hashCode();
    FileStatus fs = fileSystem.getFileStatus(path);
    long fileSize = fs.getLen(), modTime = fs.getModificationTime();
    int fileSizeHash = (int)(fileSize ^ (fileSize >>> 32)),
        modTimeHash = (int)(modTime ^ (modTime >>> 32)),
        combinedHash = modTimeHash ^ fileSizeHash;
    long id = (((long)nameHash & 0xffffffffL) << 32) | ((long)combinedHash & 0xffffffffL);
    LOG.warn("Cannot get unique file ID from "
        + fileSystem.getClass().getSimpleName() + "; using " + id + " (" + pathStr
        + "," + nameHash + "," + fileSize + ")");
    return id;
  }

  // TODO: this relies on HDFS not changing the format; we assume if we could get inode ID, this
  //       is still going to work. Otherwise, file IDs can be turned off. Later, we should use
  //       as public utility method in HDFS to obtain the inode-based path.
  private static String HDFS_ID_PATH_PREFIX = "/.reserved/.inodes/";

  public static Path getFileIdPath(
      FileSystem fileSystem, Path path, long fileId) {
    return ((fileSystem instanceof DistributedFileSystem))
        ? new Path(HDFS_ID_PATH_PREFIX + fileId) : path;
  }
}
