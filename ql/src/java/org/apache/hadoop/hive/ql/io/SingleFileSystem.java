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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Implements an abstraction layer to show files in a single directory.
 *
 * Suppose the filesystem has a directory in which there are multiple files:
 * file://somedir/f1.txt
 * file://somedir/f2.txt
 *
 * In case of Hive the content of a directory may be inside a table.
 * To give a way to show a single file as a single file in a directory it could be specified:
 *
 * sfs+file://somedir/f1.txt/#SINGLEFILE#
 *
 * This will be a directory containing only the f1.txt and nothing else.
 *
 */
/*
 * Thru out this file there are paths of both the overlay filesystem and the underlying fs.
 * To avoid confusion between these path types - all paths which are in the overlay fs are refered
 * with the upper keyword - and paths on the underlying fs are identified with the lower keyword.
 *
 *  For example:
 *    'sfs+file:///foo/bar/#SINGLEFILE#/bar' is an upper path
 *    'file:///foo/bar' is a lower path
 */
public abstract class SingleFileSystem extends FileSystem {

  public static class HDFS extends SingleFileSystem {
  }

  public static class S3A extends SingleFileSystem {
  }

  public static class ABFS extends SingleFileSystem {
  }

  public static class ABFSS extends SingleFileSystem {
  }

  public static class ADL extends SingleFileSystem {
  }

  public static class GS extends SingleFileSystem {
  }

  public static class O3FS extends SingleFileSystem {
  }

  public static class OFS extends SingleFileSystem {
  }

  public static class PFILE extends SingleFileSystem {
  }

  public static class FILE extends SingleFileSystem {
  }

  private static final String SINGLEFILE = "#SINGLEFILE#";

  private URI uri;
  private Configuration conf;
  private Path workDir;

  public String getScheme() {
    return "sfs+" + getClass().getSimpleName().toLowerCase();
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    this.uri = uri;
    this.conf = conf;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path upperPath, int bufferSize) throws IOException {
    SfsInfo info = new SfsInfo(upperPath);
    switch (info.type) {
    case LEAF_FILE:
      return info.lowerTargetPath.getFileSystem(conf).open(info.lowerTargetPath, bufferSize);
    case NONEXISTENT:
      throw newFileNotFoundException(upperPath.toString());
    default:
      throw unsupported("open:" + upperPath);
    }
  }

  @Override
  public FileStatus getFileStatus(Path upperPath) throws IOException {
    SfsInfo info = new SfsInfo(upperPath);
    switch (info.type) {
    case LEAF_FILE:
      return makeFileStatus(info.upperTargetPath, info.lowerTargetPath);
    case DIR_MODE:
      return makeDirFileStatus(upperPath, removeSfsScheme(upperPath));
    case SINGLEFILE_DIR:
      return makeDirFileStatus(upperPath, info.lowerTargetPath);
    case NONEXISTENT:
      throw newFileNotFoundException(upperPath.toString());
    default:
      throw unsupported("fileStatus:" + upperPath);
    }
  }

  @Override
  public FileStatus[] listStatus(Path upperPath) throws FileNotFoundException, IOException {
    SfsInfo info = new SfsInfo(upperPath);
    switch (info.type) {
    case DIR_MODE:
      return dirModeListStatus(upperPath);
    case LEAF_FILE:
    case SINGLEFILE_DIR:
      return new FileStatus[] { makeFileStatus(info.upperTargetPath, info.lowerTargetPath) };
    case NONEXISTENT:
      throw newFileNotFoundException(upperPath.toString());
    default:
      throw unsupported("listStatus: " + upperPath);
    }
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    workDir = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workDir;
  }

  @Override
  public FSDataOutputStream create(Path upperPath, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    throw unsupportedReadOnly("create", upperPath);
  }

  @Override
  public FSDataOutputStream append(Path upperPath, int bufferSize, Progressable progress) throws IOException {
    throw unsupportedReadOnly("append", upperPath);

  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw unsupportedReadOnly("rename", src);
  }

  @Override
  public boolean delete(Path upperPath, boolean recursive) throws IOException {
    throw unsupportedReadOnly("delete", upperPath);
  }

  @Override
  public boolean mkdirs(Path upperPath, FsPermission permission) throws IOException {
    throw unsupportedReadOnly("mkdirs", upperPath);
  }

  @Override
  public String getCanonicalServiceName() {
    return null;
  }

  /**
   * Represents what kind of path we are at.
   *
   * For every state I will give the path for the following path:
   *
   * sfs+file:///foo/bar/#SINGLEFILE#/bar
   */
  enum SfsInodeType {
    /**
     * Represents the final leaf file.
     *
     * sfs+file:///foo/bar/#SINGLEFILE#/bar
     */
    LEAF_FILE,
    /**
     * We are at a SINGLEFILE directory node.
     *
     * sfs+file:///foo/bar/#SINGLEFILE#
     */
    SINGLEFILE_DIR,
    /**
     * We are on the covered filesystem in directory mode.
     *
     * In this mode all files and directories of the underlying fs is shown as directories.
     *
     * sfs+file:///foo/bar
     * sfs+file:///foo/
     *
     */
    DIR_MODE,
    /**
     * We are at a path which doesnt exists.
     *
     * sfs+file:///foo/bar/#SINGLEFILE#/invalid
     */
    NONEXISTENT,
  }

  /**
   * Identifies and collects basic infos about the current path.
   *
   * TargetPath is also identified for both lower/upper if its available.
   */
  class SfsInfo {

    final private URI uri;
    final private SfsInodeType type;
    final private Path lowerTargetPath;
    final private Path upperTargetPath;

    public SfsInfo(Path upperPath) {
      uri = upperPath.toUri();
      String[] parts = uri.getPath().split(Path.SEPARATOR);

      int n = parts.length;
      if (n >= 1 && parts[n - 1].equals(SINGLEFILE)) {
        type = SfsInodeType.SINGLEFILE_DIR;
        lowerTargetPath = removeSfsScheme(upperPath.getParent());
        upperTargetPath = new Path(uri.getScheme(), uri.getAuthority(), uri.getPath() + "/" + parts[n - 2]);
      } else {
        if (n >= 2 && parts[n - 2].equals(SINGLEFILE)) {
          if (n >= 3 && !parts[n - 3].equals(parts[n - 1])) {
            type = SfsInodeType.NONEXISTENT;
            lowerTargetPath = null;
            upperTargetPath = null;
          } else {
            type = SfsInodeType.LEAF_FILE;
            lowerTargetPath = removeSfsScheme(upperPath.getParent().getParent());
            upperTargetPath = upperPath;
          }
        } else {
          type = SfsInodeType.DIR_MODE;
          lowerTargetPath = null;
          upperTargetPath = null;
        }
      }
    }
  }

  /**
   * Implements listing for {@link SfsInodeType#DIR_MODE}.
   */
  public FileStatus[] dirModeListStatus(Path upperPath) throws IOException {
    Path lowerPath = removeSfsScheme(upperPath);
    FileSystem fs = lowerPath.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(lowerPath);
    List<FileStatus> ret = new ArrayList<>();
    if (status.isDirectory()) {
      FileStatus[] statusList = fs.listStatus(lowerPath);
      for (FileStatus fileStatus : statusList) {
        ret.add(makeDirFileStatus(fileStatus));
      }
    } else {
      FileStatus dirStat = makeDirFileStatus(new Path(upperPath, SINGLEFILE), lowerPath);
      ret.add(dirStat);
    }
    return ret.toArray(new FileStatus[0]);
  }

  public FileStatus makeFileStatus(Path upperPath, Path lowerPath) throws IOException {
    FileStatus status = lowerPath.getFileSystem(conf).getFileStatus(lowerPath);
    status = new FileStatus(status);
    status.setPath(upperPath);
    return status;
  }

  private static FileStatus makeDirFileStatus(FileStatus lowerStatus) throws IOException {
    return makeDirFileStatus(makeSfsPath(lowerStatus.getPath()), lowerStatus);
  }

  private Path removeSfsScheme(Path lowerTargetPath0) {
    URI u = lowerTargetPath0.toUri();
    return new Path(removeSfsScheme(u.getScheme()), u.getAuthority(), u.getPath());
  }

  private String removeSfsScheme(String scheme) {
    if (scheme.startsWith("sfs+")) {
      return scheme.substring(4);
    }
    if (scheme.equals("sfs")) {
      return null;
    }
    throw new RuntimeException("Unexpected scheme: " + scheme);
  }

  private static Path makeSfsPath(Path path) throws IOException {
    URI oldUri = path.toUri();
    if (oldUri.getScheme().startsWith("sfs+")) {
      throw new IOException("unexpected path");
    }
    return new Path("sfs+" + oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath());
  }

  public FileStatus makeDirFileStatus(Path upperPath, Path lowerPath) throws IOException {
    FileStatus status = lowerPath.getFileSystem(conf).getFileStatus(lowerPath);
    return makeDirFileStatus(upperPath, status);
  }

  public static FileStatus makeDirFileStatus(Path upperPath, FileStatus status) throws IOException {
    FileStatus newStatus = new FileStatus(status.getLen(), true, status.getReplication(), status.getBlockSize(),
        status.getModificationTime(), status.getAccessTime(), addExecute(status.getPermission()), status.getOwner(),
        status.getGroup(), (status.isSymlink() ? status.getSymlink() : null), status.getPath());
    newStatus.setPath(upperPath);
    return newStatus;
  }

  private static FsPermission addExecute(FsPermission permission) {
    short mode = (short) (permission.toShort() | 1 | (1 << 3) | (1 << 6));
    return new FsPermission(mode);
  }

  private IOException unsupportedReadOnly(String opName, Path path) throws IOException {
    SfsInfo sfsInfo = new SfsInfo(path);
    if (sfsInfo.type == SfsInodeType.SINGLEFILE_DIR || sfsInfo.type == SfsInodeType.LEAF_FILE) {
      // Try to access the the underlying file if possible; as the lower fs may provide a more
      // specific exception (like: FileNotFoundException)
      FileSystem fs = sfsInfo.lowerTargetPath.getFileSystem(conf);
      fs.getFileStatus(sfsInfo.lowerTargetPath);
    }
    return new IOException("SFS is readonly hence " + opName + " is not supported! (" + path + ")");
  }

  private IOException unsupported(String str) {
    return new IOException("Unsupported SFS filesystem operation! (" + str + ")");
  }

  private IOException newFileNotFoundException(String path) {
    return new FileNotFoundException("File " + path + " does not exists!");
  }

}
