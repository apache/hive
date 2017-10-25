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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public class FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  public static final PathFilter HIDDEN_FILES_PATH_FILTER = new PathFilter() {
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

    result = fs.delete(f, true);
    if (!result) {
      LOG.error("Failed to delete " + f);
    }
    return result;
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
   * @param fs file system paths are on
   * @param sourcePath source file or directory to move
   * @param destPath destination file name.  This must be a file and not an existing directory.
   * @return result of fs.rename.
   * @throws IOException if fs.rename throws it, or if destPath already exists.
   */
  public static boolean rename(FileSystem fs, Path sourcePath, Path destPath) throws IOException {
    LOG.info("Renaming " + sourcePath + " to " + destPath);

    // If destPath directory exists, rename call will move the sourcePath
    // into destPath without failing. So check it before renaming.
    if (fs.exists(destPath)) {
      throw new IOException("Cannot rename the source path. The destination "
          + "path already exists.");
    }
    return fs.rename(sourcePath, destPath);
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
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
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
   * @param path
   *          the root path
   * @param level
   *          the depth of directory to explore
   * @param fs
   *          the file system
   * @return array of FileStatus
   * @throws IOException
   */
  public static FileStatus[] getFileStatusRecurse(Path path, int level, FileSystem fs)
      throws IOException {

    // if level is <0, the return all files/directories under the specified path
    if ( level < 0) {
      List<FileStatus> result = new ArrayList<>();
      try {
        FileStatus fileStatus = fs.getFileStatus(path);
        FileUtils.listStatusRecursively(fs, fileStatus, result);
      } catch (IOException e) {
        // globStatus() API returns empty FileStatus[] when the specified path
        // does not exist. But getFileStatus() throw IOException. To mimic the
        // similar behavior we will return empty array on exception. For external
        // tables, the path of the table will not exists during table creation
        return new FileStatus[0];
      }
      return result.toArray(new FileStatus[result.size()]);
    }

    // construct a path pattern (e.g., /*/*) to find all dynamically generated paths
    StringBuilder sb = new StringBuilder(path.toUri().getPath());
    for (int i = 0; i < level; i++) {
      sb.append(Path.SEPARATOR).append("*");
    }
    Path pathPattern = new Path(path, sb.toString());
    return fs.globStatus(pathPattern, FileUtils.HIDDEN_FILES_PATH_FILTER);
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
  public static void listStatusRecursively(FileSystem fs, FileStatus fileStatus,
                                           List<FileStatus> results) throws IOException {

    if (fileStatus.isDir()) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath(), HIDDEN_FILES_PATH_FILTER)) {
        listStatusRecursively(fs, stat, results);
      }
    } else {
      results.add(fileStatus);
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
}
