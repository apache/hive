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
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * Common FileSystem utilities around FileId.
 */
public class HdfsUtils {
  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);

  public static Object getFileId(FileSystem fileSystem, Path path,
      boolean allowSynthetic, boolean checkDefaultFs, boolean forceSyntheticIds) throws IOException {
    if (!forceSyntheticIds && fileSystem instanceof DistributedFileSystem) {
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

  /**
   * List filestatuses in the given directory.
   * In recursive mode all files are returned, otherwise directories are skipped.
   * @param fs FileSystem
   * @param path directory to list
   * @param filter optional filter
   * @param recursive do recursive listing
   * @return
   * @throws IOException
   */
  public static List<FileStatus> listLocatedFileStatus(final FileSystem fs, final Path path, final PathFilter filter,
      final boolean recursive) throws IOException {
    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, recursive);
    List<FileStatus> result = new ArrayList<>();
    while (itr.hasNext()) {
      FileStatus stat = itr.next();
      // Recursive listing might list files inside hidden directories, we need to filter those out
      if (!stat.getPath().toString().equals(path.toString())) {
        Path relativePath = new Path(stat.getPath().toString().replace(path.toString(), ""));
        if (!org.apache.hadoop.hive.metastore.utils.FileUtils.RemoteIteratorWithFilter.HIDDEN_FILES_FULL_PATH_FILTER.accept(relativePath)) {
          continue;
        }
      }
      // Apply the user provided filter
      if (filter == null || filter.accept(stat.getPath())) {
        result.add(stat);
      }

    }
    return result;
  }
  public static List<Path> listPath(final FileSystem fs, final Path path, final PathFilter filter,
      final boolean recursive) throws IOException {
    return listLocatedFileStatus(fs, path, filter, recursive).stream()
        .map(FileStatus::getPath)
        .collect(Collectors.toList());
  }

  // TODO: this relies on HDFS not changing the format; we assume if we could get inode ID, this
  //       is still going to work. Otherwise, file IDs can be turned off. Later, we should use
  //       as public utility method in HDFS to obtain the inode-based path.
  private static String HDFS_ID_PATH_PREFIX = "/.reserved/.inodes/";

  public static Path getFileIdPath(Path path, long fileId) {
    // BI/ETL split strategies set fileId correctly when HDFS is used.
    return (fileId > 0) ? new Path(HDFS_ID_PATH_PREFIX + fileId) : path;
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

  public static boolean pathExists(Path p, Configuration conf) throws HiveException {
    try {
      FileSystem fs = p.getFileSystem(conf);
      return fs.exists(p);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }


  public static class HdfsFileStatusWithoutId implements HadoopShims.HdfsFileStatusWithId {
    private final FileStatus fs;

    public HdfsFileStatusWithoutId(FileStatus fs) {
      this.fs = fs;
    }

    @Override
    public FileStatus getFileStatus() {
      return fs;
    }

    @Override
    public Long getFileId() {
      return null;
    }
  }

  /**
   * List files recursively in the given directory.
   * If the filesystem supports it will use file listing with FileIds.
   * @param fs the filesystem
   * @param dir the directory to list
   * @param useFileIds use fileId if possible
   * @param recursive do recursive listing
   * @param filter filter to apply
   * @return List of files
   * @throws IOException ex
   */
  public static List<HadoopShims.HdfsFileStatusWithId> listFileStatusWithId(FileSystem fs, Path dir, Ref<Boolean> useFileIds,
      boolean recursive, PathFilter filter) throws IOException {

    List<HadoopShims.HdfsFileStatusWithId> originals = new ArrayList<>();
    List<HadoopShims.HdfsFileStatusWithId> childrenWithId = tryListLocatedHdfsStatus(useFileIds, fs, dir, filter);
    if (childrenWithId != null) {
      for (HadoopShims.HdfsFileStatusWithId child : childrenWithId) {
        if (child.getFileStatus().isDirectory()) {
          if (recursive) {
            originals.addAll(listFileStatusWithId(fs, child.getFileStatus().getPath(), useFileIds,
                true, filter));
          }
        } else {
          if (child.getFileStatus().getLen() > 0) {
            originals.add(child);
          }
        }
      }
    } else {
      List<FileStatus> children = listLocatedFileStatus(fs, dir, filter, recursive);
      originals = children.stream().map(HdfsFileStatusWithoutId::new).collect(Collectors.toList());
    }
    return originals;
  }

  public static List<HadoopShims.HdfsFileStatusWithId> tryListLocatedHdfsStatus(Ref<Boolean> useFileIds, FileSystem fs,
      Path directory, PathFilter filter) {
    if (useFileIds == null) {
      return null;
    }
    if (filter == null) {
      filter = FileUtils.HIDDEN_FILES_PATH_FILTER;
    }

    List<HadoopShims.HdfsFileStatusWithId> childrenWithId = null;
    final Boolean val = useFileIds.value;
    if (val == null || val) {
      try {
        childrenWithId = SHIMS.listLocatedHdfsStatus(fs, directory, filter);
        if (val == null) {
          useFileIds.value = true;
        }
      } catch (UnsupportedOperationException uoe) {
        LOG.info("Failed to get files with ID; using regular API: " + uoe.getMessage());
        if (val == null) {
          useFileIds.value = false;
        }
      } catch (IOException ioe) {
        LOG.info("Failed to get files with ID; using regular API: " + ioe.getMessage());
        LOG.debug("Failed to get files with ID", ioe);
      }
    }
    return childrenWithId;
  }
}
