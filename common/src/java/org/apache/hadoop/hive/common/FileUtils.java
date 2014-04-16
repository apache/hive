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

package org.apache.hadoop.hive.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;


/**
 * Collection of file manipulation utilities common across Hive.
 */
public final class FileUtils {
  private static final Log LOG = LogFactory.getLog(FileUtils.class.getName());
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
      return path.makeQualified(FileSystem.get(conf));
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

  private FileUtils() {
    // prevent instantiation
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
   * default directory will have the same depth as number of skewed columns
   * this will make future operation easy like DML merge, concatenate merge
   * @param skewedCols
   * @param name
   * @return
   */
  public static String makeDefaultListBucketingDirName(List<String> skewedCols,
      String name) {
    String lbDirName;
    String defaultDir = FileUtils.escapePathName(name);
    StringBuilder defaultDirPath = new StringBuilder();
    for (int i = 0; i < skewedCols.size(); i++) {
      if (i > 0) {
        defaultDirPath.append(Path.SEPARATOR);
      }
      defaultDirPath.append(defaultDir);
    }
    lbDirName = defaultDirPath.toString();
    return lbDirName;
  }

  /**
   * Makes a valid list bucketing directory name.
   * @param lbCols The skewed keys' names
   * @param vals The skewed values
   * @return An escaped, valid list bucketing directory name.
   */
  public static String makeListBucketingDirName(List<String> lbCols, List<String> vals) {
    StringBuilder name = new StringBuilder();
    for (int i = 0; i < lbCols.size(); i++) {
      if (i > 0) {
        name.append(Path.SEPARATOR);
      }
      name.append(escapePathName((lbCols.get(i)).toLowerCase()));
      name.append('=');
      name.append(escapePathName(vals.get(i)));
    }
    return name.toString();
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

  static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }

    /**
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

    if(Shell.WINDOWS){
      //On windows, following chars need to be escaped as well
      char [] winClist = {' ', '<','>','|'};
      for (char c : winClist) {
        charToEscape.set(c);
      }
    }

  }

  static boolean needsEscaping(char c) {
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
          code = Integer.valueOf(path.substring(i + 1, i + 3), 16);
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
      for (FileStatus stat : fs.listStatus(fileStatus.getPath(), new PathFilter() {

        @Override
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      })) {
        listStatusRecursively(fs, stat, results);
      }
    } else {
      results.add(fileStatus);
    }
  }

  /**
   * Find the parent of path that exists, if path does not exist
   *
   * @param fs
   *          file system
   * @param path
   * @return the argument path if it exists or a parent path exists. Returns
   *         NULL root is only parent that exists
   * @throws IOException
   */
  public static Path getPathOrParentThatExists(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      Path parentPath = path.getParent();
      return getPathOrParentThatExists(fs, parentPath);
    }
    return path;
  }

  /**
   * Check if the given FileStatus indicates that the action is allowed for
   * userName. It checks the group and other permissions also to determine this.
   *
   * @param userName
   * @param fsStatus
   * @param action
   * @return true if it is writable for userName
   */
  public static boolean isActionPermittedForUser(String userName, FileStatus fsStatus, FsAction action) {
    FsPermission permissions = fsStatus.getPermission();
    // check user perm
    if (fsStatus.getOwner().equals(userName)
        && permissions.getUserAction().implies(action)) {
      return true;
    }
    // check other perm
    if (permissions.getOtherAction().implies(action)) {
      return true;
    }
    // check group perm after ensuring user belongs to the file owner group
    String fileGroup = fsStatus.getGroup();
    String[] userGroups = UserGroupInformation.createRemoteUser(userName).getGroupNames();
    for (String group : userGroups) {
      if (group.equals(fileGroup)) {
        // user belongs to the file group
        return permissions.getGroupAction().implies(action);
      }
    }
    return false;
  }

  /**
   * Check if user userName has permissions to perform the given FsAction action
   * on all files under the file whose FileStatus fileStatus is provided
   *
   * @param fs
   * @param fileStatus
   * @param userName
   * @param action
   * @return
   * @throws IOException
   */
  public static boolean isActionPermittedForFileHierarchy(FileSystem fs, FileStatus fileStatus,
      String userName, FsAction action) throws IOException {
    boolean isDir = fileStatus.isDir();

    FsAction dirActionNeeded = action;
    if (isDir) {
      // for dirs user needs execute privileges as well
      dirActionNeeded.and(FsAction.EXECUTE);
    }
    if (!isActionPermittedForUser(userName, fileStatus, dirActionNeeded)) {
      return false;
    }

    if (!isDir) {
      // no sub dirs to be checked
      return true;
    }
    // check all children
    FileStatus[] childStatuses = fs.listStatus(fileStatus.getPath());
    for (FileStatus childStatus : childStatuses) {
      // check children recursively
      if (!isActionPermittedForFileHierarchy(fs, childStatus, userName, action)) {
        return false;
      }
    }
    return true;
  }

  /**
   * A best effort attempt to determine if if the file is a local file
   * @param conf
   * @param fileName
   * @return true if it was successfully able to determine that it is a local file
   */
  public static boolean isLocalFile(HiveConf conf, String fileName) {
    try {
      // do best effor to determine if this is a local file
      FileSystem fsForFile = FileSystem.get(new URI(fileName), conf);
      return LocalFileSystem.class.isInstance(fsForFile);
    } catch (URISyntaxException e) {
      LOG.warn("Unable to create URI from " + fileName, e);
    } catch (IOException e) {
      LOG.warn("Unable to get FileSystem for " + fileName, e);
    }
    return false;
  }

  public static boolean isOwnerOfFileHierarchy(FileSystem fs, FileStatus fileStatus, String userName)
      throws IOException {
    if (!fileStatus.getOwner().equals(userName)) {
      return false;
    }

    if (!fileStatus.isDir()) {
      // no sub dirs to be checked
      return true;
    }
    // check all children
    FileStatus[] childStatuses = fs.listStatus(fileStatus.getPath());
    for (FileStatus childStatus : childStatuses) {
      // check children recursively
      if (!isOwnerOfFileHierarchy(fs, childStatus, userName)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Creates the directory and all necessary parent directories.
   * @param fs FileSystem to use
   * @param f path to create.
   * @param inheritPerms whether directory inherits the permission of the last-existing parent path
   * @return true if directory created successfully.  False otherwise, including if it exists.
   * @throws IOException exception in creating the directory
   */
  public static boolean mkdir(FileSystem fs, Path f, boolean inheritPerms) throws IOException {
    LOG.info("Creating directory if it doesn't exist: " + f);
    if (!inheritPerms) {
      //just create the directory
      return fs.mkdirs(f);
    } else {
      //Check if the directory already exists. We want to change the permission
      //to that of the parent directory only for newly created directories.
      try {
        return fs.getFileStatus(f).isDir();
      } catch (FileNotFoundException ignore) {
      }
      //inherit perms: need to find last existing parent path, and apply its permission on entire subtree.
      Path path = f;
      List<Path> pathsToSet = new ArrayList<Path>();
      while (!fs.exists(path)) {
        pathsToSet.add(path);
        path = path.getParent();
      }
      //at the end of this loop, path is the last-existing parent path.
      boolean success = fs.mkdirs(f);
      if (!success) {
        return false;
      } else {
        FsPermission parentPerm = fs.getFileStatus(path).getPermission();
        String parentGroup = fs.getFileStatus(path).getGroup();
        for (Path pathToSet : pathsToSet) {
          String currOwner = fs.getFileStatus(pathToSet).getOwner();
          LOG.info("Setting permission and group of parent directory: " + path.toString() +
            " on new directory: " + pathToSet.toString());
          fs.setPermission(pathToSet, parentPerm);
          fs.setOwner(pathToSet, currOwner, parentGroup);
        }
        return true;
      }
    }
  }
}
