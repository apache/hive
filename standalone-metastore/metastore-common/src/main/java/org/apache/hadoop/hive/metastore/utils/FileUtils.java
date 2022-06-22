/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
  private static final PathFilter SNAPSHOT_DIR_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return ".snapshot".equalsIgnoreCase(p.getName());
    }
  };
  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  public static final PathFilter HIDDEN_FILES_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };
  /**
   * Filter that filters out hidden files
   */
  private static final PathFilter hiddenFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Move a particular file or directory to the trash.
   * @param fs FileSystem to use
   * @param f path of file or directory to move to trash.
   * @param conf configuration object
   * @return true if move successful
   * @throws IOException
   */
  public static boolean moveToTrash(FileSystem fs, Path f, Configuration conf, boolean purge)
      throws IOException {
    LOG.debug("deleting  " + f);
    boolean result;
    try {
      if(purge) {
        LOG.debug("purge is set to true. Not moving to Trash " + f);
      } else {
        result = Trash.moveToAppropriateTrash(fs, f, conf);
        if (result) {
          LOG.trace("Moved to trash: " + f);
          return true;
        }
      }
    } catch (IOException ioe) {
      // for whatever failure reason including that trash has lower encryption zone
      // retry with force delete
      LOG.warn(ioe.getMessage() + "; Force to delete it.");
    }

    try {
      result = fs.delete(f, true);

    } catch (RemoteException | SnapshotException se) {
      // If this is snapshot exception or the cause is snapshot replication from HDFS, could be the case where the
      // snapshots were created by replication, so in that case attempt to delete the replication related snapshots,
      // if the exists and then re attempt delete.
      if (se instanceof SnapshotException || se.getCause() instanceof SnapshotException || se.getMessage()
          .contains("Snapshot"))
        deleteReplRelatedSnapshots(fs, f);
      // retry delete after attempting to delete replication related snapshots
      result = fs.delete(f, true);
    }
    if (!result) {
      LOG.error("Failed to delete " + f);
    }
    return result;
  }

  /**
   * Attempts to delete the replication related snapshots
   * @param fs the filesystem
   * @param path path where the snapshots are supposed to exists.
   */
  private static void deleteReplRelatedSnapshots(FileSystem fs, Path path) {
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      // List the snapshot directory.
      FileStatus[] listing = fs.listStatus(new Path(path, ".snapshot"));
      for (FileStatus elem : listing) {
        // if the snapshot name has replication related suffix, then delete that snapshot.
        if (elem.getPath().getName().endsWith("replOld") || elem.getPath().getName().endsWith("replNew")) {
          dfs.deleteSnapshot(path, elem.getPath().getName());
        }
      }
    } catch (Exception ioe) {
      // Ignore since this method is used as part of purge which actually ignores all exception, if the directory can
      // not be deleted, so preserve the same behaviour.
      LOG.warn("Couldn't clean up replication related snapshots", ioe);
    }
  }

  /**
   * Copies files between filesystems.
   */
  public static boolean copy(FileSystem srcFS, Path src,
      FileSystem dstFS, Path dst,
      boolean deleteSource,
      boolean overwrite,
      Configuration conf) throws IOException {
    boolean copied = false;
    boolean triedDistcp = false;

    /* Run distcp if source file/dir is too big */
    if (srcFS.getUri().getScheme().equals("hdfs")) {
      ContentSummary srcContentSummary = srcFS.getContentSummary(src);
      if (srcContentSummary.getFileCount() >
            MetastoreConf.getLongVar(conf, ConfVars.REPL_COPYFILE_MAXNUMFILES)
          && srcContentSummary.getLength() >
            MetastoreConf.getLongVar(conf,ConfVars.REPL_COPYFILE_MAXSIZE)) {

        LOG.info("Source is " + srcContentSummary.getLength() + " bytes. (MAX: " +
            MetastoreConf.getLongVar(conf, ConfVars.REPL_COPYFILE_MAXSIZE) + ")");
        LOG.info("Source is " + srcContentSummary.getFileCount() + " files. (MAX: " +
            MetastoreConf.getLongVar(conf, ConfVars.REPL_COPYFILE_MAXNUMFILES) + ")");
        LOG.info("Launch distributed copy (distcp) job.");
        triedDistcp = true;
        copied = distCp(srcFS, Collections.singletonList(src), dst, deleteSource, null, conf);
      }
    }
    if (!triedDistcp) {
      // Note : Currently, this implementation does not "fall back" to regular copy if distcp
      // is tried and it fails. We depend upon that behaviour in cases like replication,
      // wherein if distcp fails, there is good reason to not plod along with a trivial
      // implementation, and fail instead.
      copied = FileUtil.copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf);
    }
    return copied;
  }

  private static boolean distCp(FileSystem srcFS, List<Path> srcPaths, Path dst,
                                boolean deleteSource, String doAsUser,
                                Configuration conf) throws IOException {
    boolean copied;
    if (doAsUser == null){
      copied = HdfsUtils.runDistCp(srcPaths, dst, conf);
    } else {
      copied = HdfsUtils.runDistCpAs(srcPaths, dst, conf, doAsUser);
    }
    if (copied && deleteSource) {
      for (Path path : srcPaths) {
        srcFS.delete(path, true);
      }
    }
    return copied;
  }

  /**
   * Creates the directory and all necessary parent directories.
   * @param fs FileSystem to use
   * @param f path to create.
   * @return true if directory created successfully.  False otherwise, including if it exists.
   * @throws IOException exception in creating the directory
   */
  public static boolean mkdir(FileSystem fs, Path f) throws IOException {
    LOG.info("Creating directory if it doesn't exist: " + f);
    return fs.mkdirs(f);
  }

  /**
   * Rename a file.  Unlike {@link FileSystem#rename(Path, Path)}, if the destPath already exists
   * and is a directory, this will NOT move the sourcePath into it.  It will throw an IOException
   * instead.
   * @param srcFs file system src paths are on
   * @param destFs file system dest paths are on
   * @param srcPath source file or directory to move
   * @param destPath destination file name.  This must be a file and not an existing directory.
   * @return result of fs.rename.
   * @throws IOException if fs.rename throws it, or if destPath already exists.
   */
   public static boolean rename(FileSystem srcFs, FileSystem destFs, Path srcPath,
                               Path destPath) throws IOException {
   LOG.info("Renaming " + srcPath + " to " + destPath);

   // If destPath directory exists, rename call will move the srcPath
   // into destPath without failing. So check it before renaming.
   if(destFs.exists(destPath)) {
     throw new IOException("Cannot rename the source path. The destination "
         + "path already exists.");
   }

   if (equalsFileSystem(srcFs, destFs)) {
       //just rename the directory
       return srcFs.rename(srcPath, destPath);
     } else {
         Configuration conf = new Configuration();
         return copy(srcFs, srcPath, destFs, destPath,
         true,    // delete source
         false, // overwrite destination
         conf);
     }
   }

  // NOTE: This is for generating the internal path name for partitions. Users
  // should always use the MetaStore API to get the path name for a partition.
  // Users should not directly take partition values and turn it into a path
  // name by themselves, because the logic below may change in the future.
  //
  // In the future, it's OK to add new chars to the escape list, and old data
  // won't be corrupt, because the full path name in metastore is stored.
  // In that case, Hive will continue to read the old data, but when it creates
  // new partitions, it will use new names.
  // edit : There are some use cases for which adding new chars does not seem
  // to be backward compatible - Eg. if partition was created with name having
  // a special char that you want to start escaping, and then you try dropping
  // the partition with a hive version that now escapes the special char using
  // the list below, then the drop partition fails to work.

  private static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }

    /*
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    char[] clist = new char[] {'\u0001', '\u0002', '\u0003', '\u0004',
                               '\u0005', '\u0006', '\u0007', '\u0008', '\u0009', '\n', '\u000B',
                               '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012',
                               '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019',
                               '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F',
                               '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F', '{',
                               '[', ']', '^'};

    for (char c : clist) {
      charToEscape.set(c);
    }
  }

  private static boolean needsEscaping(char c) {
    return c < charToEscape.size() && charToEscape.get(c);
  }

  public static String escapePathName(String path) {
    return escapePathName(path, null);
  }

  /**
   * Escapes a path name.
   * @param path The path to escape.
   * @param defaultPath
   *          The default name for the path, if the given path is empty or null.
   * @return An escaped path name.
   */
  public static String escapePathName(String path, String defaultPath) {

    // __HIVE_DEFAULT_NULL__ is the system default value for null and empty string.
    // TODO: we should allow user to specify default partition or HDFS file location.
    if (path == null || path.length() == 0) {
      if (defaultPath == null) {
        //previously, when path is empty or null and no default path is specified,
        // __HIVE_DEFAULT_PARTITION__ was the return value for escapePathName
        return "__HIVE_DEFAULT_PARTITION__";
      } else {
        return defaultPath;
      }
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  public static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (Exception e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Get all file status from a root path and recursively go deep into certain levels.
   *
   * @param base
   *          the root path
   * @param fs
   *          the file system
   * @return array of FileStatus
   */
  public static List<FileStatus> getFileStatusRecurse(Path base, FileSystem fs) {
    try {
      List<FileStatus> results = new ArrayList<>();
      if (isS3a(fs)) {
        // S3A file system has an optimized recursive directory listing implementation however it doesn't support filtering.
        // Therefore we filter the result set afterwards. This might be not so optimal in HDFS case (which does a tree walking) where a filter could have been used.
        listS3FilesRecursive(base, fs, results);
      } else {
        listStatusRecursively(fs, fs.getFileStatus(base), results);
      }
      return results;
    } catch (IOException e) {
      // globStatus() API returns empty FileStatus[] when the specified path
      // does not exist. But getFileStatus() throw IOException. To mimic the
      // similar behavior we will return empty array on exception. For external
      // tables, the path of the table will not exists during table creation
      return Collections.emptyList();
    }
  }

  /**
   * Recursively lists status for all files starting from a particular directory (or individual file
   * as base case).
   *
   * @param fs
   *          file system
   *
   * @param fileStatus
   *          starting point in file system
   *
   * @param results
   *          receives enumeration of all files found
   */
  private static void listStatusRecursively(FileSystem fs, FileStatus fileStatus,
                                           List<FileStatus> results) throws IOException {

    if (fileStatus.isDir()) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath(), HIDDEN_FILES_PATH_FILTER)) {
        listStatusRecursively(fs, stat, results);
      }
    } else {
      results.add(fileStatus);
    }
  }

  private static void listS3FilesRecursive(Path base, FileSystem fs, List<FileStatus> results) throws IOException {
    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(base, true);
    while (remoteIterator.hasNext()) {
      LocatedFileStatus each = remoteIterator.next();
      Path relativePath = makeRelative(base, each.getPath());
      if (RemoteIteratorWithFilter.HIDDEN_FILES_FULL_PATH_FILTER.accept(relativePath)) {
        results.add(each);
      }
    }
  }

  /**
   * Returns a relative path wrt the parent path.
   * @param parentPath the parent path.
   * @param childPath the child path.
   * @return childPath relative to parent path.
   */
  public static Path makeRelative(Path parentPath, Path childPath) {
    String parentString =
        parentPath.toString().endsWith(Path.SEPARATOR) ? parentPath.toString() : parentPath.toString() + Path.SEPARATOR;
    String childString =
        childPath.toString().endsWith(Path.SEPARATOR) ? childPath.toString() : childPath.toString() + Path.SEPARATOR;
    return new Path(childString.replaceFirst(parentString, ""));
  }

  public static boolean isS3a(FileSystem fs) {
    try {
      return "s3a".equalsIgnoreCase(fs.getScheme());
    } catch (UnsupportedOperationException ex) {
      return false;
    }
  }

  public static String makePartName(List<String> partCols, List<String> vals) {
    return makePartName(partCols, vals, null);
  }

  /**
   * Makes a valid partition name.
   * @param partCols The partition keys' names
   * @param vals The partition values
   * @param defaultStr
   *         The default name given to a partition value if the respective value is empty or null.
   * @return An escaped, valid partition name.
   */
  public static String makePartName(List<String> partCols, List<String> vals,
                                    String defaultStr) {
    StringBuilder name = new StringBuilder();
    for (int i = 0; i < partCols.size(); i++) {
      if (i > 0) {
        name.append(Path.SEPARATOR);
      }
      name.append(escapePathName((partCols.get(i)).toLowerCase(), defaultStr));
      name.append('=');
      name.append(escapePathName(vals.get(i), defaultStr));
    }
    return name.toString();
  }

  /**
   * Determine if two objects reference the same file system.
   * @param fs1 first file system
   * @param fs2 second file system
   * @return return true if both file system arguments point to same file system
   */
  public static boolean equalsFileSystem(FileSystem fs1, FileSystem fs2) {
    //When file system cache is disabled, you get different FileSystem objects
    // for same file system, so '==' can't be used in such cases
    //FileSystem api doesn't have a .equals() function implemented, so using
    //the uri for comparison. FileSystem already uses uri+Configuration for
    //equality in its CACHE .
    //Once equality has been added in HDFS-9159, we should make use of it
    return fs1.getUri().equals(fs2.getUri());
  }

  /**
   * Check if the path contains a subdirectory named '.snapshot'
   * @param p path to check
   * @param fs filesystem of the path
   * @return true if p contains a subdirectory named '.snapshot'
   * @throws IOException
   */
  public static boolean pathHasSnapshotSubDir(Path p, FileSystem fs) throws IOException {
    // Hadoop is missing a public API to check for snapshotable directories. Check with the directory name
    // until a more appropriate API is provided by HDFS-12257.
    final FileStatus[] statuses = fs.listStatus(p, FileUtils.SNAPSHOT_DIR_PATH_FILTER);
    return statuses != null && statuses.length != 0;
  }

  public static void makeDir(Path path, Configuration conf) throws MetaException {
    FileSystem fs;
    try {
      fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }
    } catch (IOException e) {
      throw new MetaException("Unable to : " + path);
    }
  }

  /**
   * Utility method that determines if a specified directory already has
   * contents (non-hidden files) or not - useful to determine if an
   * immutable table already has contents, for example.
   * @param fs
   * @param path
   * @throws IOException
   */
  public static boolean isDirEmpty(FileSystem fs, Path path) throws IOException {

    if (fs.exists(path)) {
      FileStatus[] status = fs.globStatus(new Path(path, "*"), hiddenFileFilter);
      if (status.length > 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Variant of Path.makeQualified that qualifies the input path against the default file system
   * indicated by the configuration
   *
   * This does not require a FileSystem handle in most cases - only requires the Filesystem URI.
   * This saves the cost of opening the Filesystem - which can involve RPCs - as well as cause
   * errors
   *
   * @param path
   *          path to be fully qualified
   * @param conf
   *          Configuration file
   * @return path qualified relative to default file system
   */
  public static Path makeQualified(Path path, Configuration conf) throws IOException {

    if (!path.isAbsolute()) {
      // in this case we need to get the working directory
      // and this requires a FileSystem handle. So revert to
      // original method.
      FileSystem fs = FileSystem.get(conf);
      return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    }

    URI fsUri = FileSystem.getDefaultUri(conf);
    URI pathUri = path.toUri();

    String scheme = pathUri.getScheme();
    String authority = pathUri.getAuthority();

    // validate/fill-in scheme and authority. this follows logic
    // identical to FileSystem.get(URI, conf) - but doesn't actually
    // obtain a file system handle

    if (scheme == null) {
      // no scheme - use default file system uri
      scheme = fsUri.getScheme();
      authority = fsUri.getAuthority();
      if (authority == null) {
        authority = "";
      }
    } else {
      if (authority == null) {
        // no authority - use default one if it applies
        if (scheme.equals(fsUri.getScheme()) && fsUri.getAuthority() != null) {
          authority = fsUri.getAuthority();
        } else {
          authority = "";
        }
      }
    }

    return new Path(scheme, authority, pathUri.getPath());
  }

  /**
   * Returns a BEST GUESS as to whether or not other is a subdirectory of parent. It does not
   * take into account any intricacies of the underlying file system, which is assumed to be
   * HDFS. This should not return any false positives, but may return false negatives.
   *
   * @param parent
   * @param other Directory to check if it is a subdirectory of parent
   * @return True, if other is subdirectory of parent
   */
  public static boolean isSubdirectory(String parent, String other) {
    return other.startsWith(parent.endsWith(Path.SEPARATOR) ? parent : parent + Path.SEPARATOR);
  }

  public static Path getTransformedPath(String name, String subDir, String root) {
    if (root != null) {
      Path newPath = new Path(root);
      if (subDir != null) {
        newPath = new Path(newPath, subDir);
      }
      return new Path(newPath, name);
    }
    return null;
  }
  public static class RemoteIteratorWithFilter implements RemoteIterator<LocatedFileStatus> {
    /**
     * This works with {@link RemoteIterator} which (potentially) produces all files recursively
     * so looking for hidden folders must look at whole path, not just the the last part of it as
     * would be appropriate w/o recursive listing.
     */
    public static final PathFilter HIDDEN_FILES_FULL_PATH_FILTER = new PathFilter() {
      @Override
      public boolean accept(Path p) {
        do {
          String name = p.getName();
          if (name.startsWith("_") || name.startsWith(".")) {
            return false;
          }
        } while ((p = p.getParent()) != null);
        return true;
      }
    };
    private final RemoteIterator<LocatedFileStatus> iter;
    private final PathFilter filter;
    private LocatedFileStatus nextFile;

    public RemoteIteratorWithFilter(RemoteIterator<LocatedFileStatus> iter, PathFilter filter)
        throws IOException {
      this.iter = iter;
      this.filter = filter;
      findNext();
    }

    @Override
    public boolean hasNext() throws IOException {
      return nextFile != null;
    }

    @Override
    public LocatedFileStatus next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      LocatedFileStatus result = nextFile;
      findNext();
      return result;
    }

    void findNext() throws IOException {
      while (iter.hasNext()) {
        LocatedFileStatus status = iter.next();
        if (filter.accept(status.getPath())) {
          nextFile = status;
          return;
        }
      }

      // No more matching files in the iterator
      nextFile = null;
    }
  }
}
