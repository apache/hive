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

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.net.URI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;

public class HdfsUtils {
  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);

  public static Object getFileId(FileSystem fileSystem, Path path,
      boolean allowSynthetic, boolean checkDefaultFs, boolean forceSyntheticIds) throws IOException {
    if (forceSyntheticIds == false && fileSystem instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
      if ((!checkDefaultFs) || isDefaultFs(dfs)) {
        Object result = SHIMS.getFileId(dfs, path.toUri().getPath());
        if (result != null) return result;
      }
    }
    if (!allowSynthetic) {
      LOG.warn("Cannot get unique file ID from "
        + fileSystem.getClass().getSimpleName() + "; returning null");
      return null;
    }
    FileStatus fs = fileSystem.getFileStatus(path);
    return new SyntheticFileId(path, fs.getLen(), fs.getModificationTime());
  }

  // This is not actually used for production.
  @VisibleForTesting
  public static long createTestFileId(
      String pathStr, FileStatus fs, boolean doLog, String fsName) {
    int nameHash = pathStr.hashCode();
    long fileSize = fs.getLen(), modTime = fs.getModificationTime();
    int fileSizeHash = (int)(fileSize ^ (fileSize >>> 32)),
        modTimeHash = (int)(modTime ^ (modTime >>> 32)),
        combinedHash = modTimeHash ^ fileSizeHash;
    long id = ((nameHash & 0xffffffffL) << 32) | (combinedHash & 0xffffffffL);
    if (doLog) {
      LOG.warn("Cannot get unique file ID from " + fsName + "; using " + id
          + " (" + pathStr + "," + nameHash + "," + fileSize + ")");
    }
    return id;
  }

  public static List<FileStatus> listLocatedStatus(final FileSystem fs,
      final Path path,
      final PathFilter filter
      ) throws IOException {
    RemoteIterator<LocatedFileStatus> itr = fs.listLocatedStatus(path);
    List<FileStatus> result = new ArrayList<FileStatus>();
    while(itr.hasNext()) {
      FileStatus stat = itr.next();
      if (filter == null || filter.accept(stat.getPath())) {
        result.add(stat);
      }
    }
    return result;
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

  public static boolean isDefaultFs(DistributedFileSystem fs) {
    URI uri = fs.getUri();

    String scheme = uri.getScheme();
    if (scheme == null) return true; // Assume that relative URI resolves to default FS.
    URI defaultUri = FileSystem.getDefaultUri(fs.getConf());
    if (!defaultUri.getScheme().equalsIgnoreCase(scheme)) return false; // Mismatch.
 
    String defaultAuthority = defaultUri.getAuthority(), authority = uri.getAuthority();
    if (authority == null) return true; // Schemes match, no authority - assume default.
    if (defaultAuthority == null) return false; // TODO: What does this even mean?
    if (!defaultUri.getHost().equalsIgnoreCase(uri.getHost())) return false; // Mismatch.

    int defaultPort = defaultUri.getPort(), port = uri.getPort();
    if (port == -1) return true; // No port, assume default.
    // Note - this makes assumptions that are DFS-specific; DFS::getDefaultPort is not visible.
    return (defaultPort == -1) ? (port == NameNode.DEFAULT_PORT) : (port == defaultPort);
  }
}
