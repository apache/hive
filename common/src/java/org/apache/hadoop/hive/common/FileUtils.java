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
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatus;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;


/**
 * Collection of file manipulation utilities common across Hive.
 */
public final class FileUtils {
  private static final Log LOG = LogFactory.getLog(FileUtils.class.getName());

  public static final PathFilter HIDDEN_FILES_PATH_FILTER = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  public static final PathFilter STAGING_DIR_PATH_FILTER = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith(".");
    }
  };

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
      for (FileStatus stat : fs.listStatus(fileStatus.getPath(), HIDDEN_FILES_PATH_FILTER)) {
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
   * @return FileStatus for argument path if it exists or the first ancestor in the path that exists
   * @throws IOException
   */
  public static FileStatus getPathOrParentThatExists(FileSystem fs, Path path) throws IOException {
    FileStatus stat = FileUtils.getFileStatusOrNull(fs, path);
    if (stat != null) {
      return stat;
    }
    Path parentPath = path.getParent();
    return getPathOrParentThatExists(fs, parentPath);
  }

  /**
   * Perform a check to determine if the user is able to access the file passed in.
   * If the user name passed in is different from the current user, this method will
   * attempt to do impersonate the user to do the check; the current user should be
   * able to create proxy users in this case.
   * @param fs   FileSystem of the path to check
   * @param stat FileStatus representing the file
   * @param action FsAction that will be checked
   * @param user User name of the user that will be checked for access.  If the user name
   *             is null or the same as the current user, no user impersonation will be done
   *             and the check will be done as the current user. Otherwise the file access
   *             check will be performed within a doAs() block to use the access privileges
   *             of this user. In this case the user must be configured to impersonate other
   *             users, otherwise this check will fail with error.
   * @throws IOException
   * @throws AccessControlException
   * @throws InterruptedException
   * @throws Exception
   */
  public static void checkFileAccessWithImpersonation(final FileSystem fs,
      final FileStatus stat, final FsAction action, final String user)
          throws IOException, AccessControlException, InterruptedException, Exception {
    checkFileAccessWithImpersonation(fs,
                                     Iterators.singletonIterator(stat),
                                     EnumSet.of(action),
                                     user);
  }

  /**
   * Perform a check to determine if the user is able to access the file passed in.
   * If the user name passed in is different from the current user, this method will
   * attempt to do impersonate the user to do the check; the current user should be
   * able to create proxy users in this case.
   * @param fs   FileSystem of the path to check
   * @param statuses FileStatus instances representing the file
   * @param actions The FsActions that will be checked
   * @param user User name of the user that will be checked for access.  If the user name
   *             is null or the same as the current user, no user impersonation will be done
   *             and the check will be done as the current user. Otherwise the file access
   *             check will be performed within a doAs() block to use the access privileges
   *             of this user. In this case the user must be configured to impersonate other
   *             users, otherwise this check will fail with error.
   * @throws IOException
   * @throws AccessControlException
   * @throws InterruptedException
   * @throws Exception
   */
  public static void checkFileAccessWithImpersonation(final FileSystem fs,
      final Iterator<FileStatus> statuses, final EnumSet<FsAction> actions, final String user)
          throws IOException, AccessControlException, InterruptedException, Exception {
    UserGroupInformation ugi = Utils.getUGI();
    String currentUser = ugi.getShortUserName();

    if (user == null || currentUser.equals(user)) {
      // No need to impersonate user, do the checks as the currently configured user.
      ShimLoader.getHadoopShims().checkFileAccess(fs, statuses, actions);
    }
    else {
      // Otherwise, try user impersonation. Current user must be configured to do user impersonation.
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
          user, UserGroupInformation.getLoginUser());
      proxyUser.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          FileSystem fsAsUser = FileSystem.get(fs.getUri(), fs.getConf());
          ShimLoader.getHadoopShims().checkFileAccess(fsAsUser, statuses, actions);
          return null;
        }
      });
    }
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
      String userName, FsAction action) throws Exception {
    boolean isDir = fileStatus.isDir();

    FsAction dirActionNeeded = action;
    if (isDir) {
      // for dirs user needs execute privileges as well
      dirActionNeeded.and(FsAction.EXECUTE);
    }

    try {
      checkFileAccessWithImpersonation(fs, fileStatus, action, userName);
    } catch (AccessControlException err) {
      // Action not permitted for user
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
      // do best effort to determine if this is a local file
      return isLocalFile(conf, new URI(fileName));
    } catch (URISyntaxException e) {
      LOG.warn("Unable to create URI from " + fileName, e);
    }
    return false;
  }

  /**
   * A best effort attempt to determine if if the file is a local file
   * @param conf
   * @param fileUri
   * @return true if it was successfully able to determine that it is a local file
   */
  public static boolean isLocalFile(HiveConf conf, URI fileUri) {
    try {
      // do best effort to determine if this is a local file
      FileSystem fsForFile = FileSystem.get(fileUri, conf);
      return LocalFileSystem.class.isInstance(fsForFile);
    } catch (IOException e) {
      LOG.warn("Unable to get FileSystem for " + fileUri, e);
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
   * @param conf Hive configuration
   * @return true if directory created successfully.  False otherwise, including if it exists.
   * @throws IOException exception in creating the directory
   */
  public static boolean mkdir(FileSystem fs, Path f, boolean inheritPerms, Configuration conf) throws IOException {
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
      Path lastExistingParent = f;
      Path firstNonExistentParent = null;
      while (!fs.exists(lastExistingParent)) {
        firstNonExistentParent = lastExistingParent;
        lastExistingParent = lastExistingParent.getParent();
      }
      boolean success = fs.mkdirs(f);
      if (!success) {
        return false;
      } else {
        HadoopShims shim = ShimLoader.getHadoopShims();
        HdfsFileStatus fullFileStatus = shim.getFullFileStatus(conf, fs, lastExistingParent);
        try {
          //set on the entire subtree
          shim.setFullFileStatus(conf, fullFileStatus, fs, firstNonExistentParent);
        } catch (Exception e) {
          LOG.warn("Error setting permissions of " + firstNonExistentParent, e);
        }
        return true;
      }
    }
  }

  /**
   * Copies files between filesystems.
   */
  public static boolean copy(FileSystem srcFS, Path src,
    FileSystem dstFS, Path dst,
    boolean deleteSource,
    boolean overwrite,
    HiveConf conf) throws IOException {

    HadoopShims shims = ShimLoader.getHadoopShims();
    boolean copied;

    /* Run distcp if source file/dir is too big */
    if (srcFS.getUri().getScheme().equals("hdfs") &&
        srcFS.getFileStatus(src).getLen() > conf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE)) {
      LOG.info("Source is " + srcFS.getFileStatus(src).getLen() + " bytes. (MAX: " + conf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE) + ")");
      LOG.info("Launch distributed copy (distcp) job.");
      copied = shims.runDistCp(src, dst, conf);
      if (copied && deleteSource) {
        srcFS.delete(src, true);
      }
    } else {
      copied = FileUtil.copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf);
    }

    boolean inheritPerms = conf.getBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
    if (copied && inheritPerms) {
      HdfsFileStatus fullFileStatus = shims.getFullFileStatus(conf, dstFS, dst);
      try {
        shims.setFullFileStatus(conf, fullFileStatus, dstFS, dst);
      } catch (Exception e) {
        LOG.warn("Error setting permissions or group of " + dst, e);
      }
    }
    return copied;
  }

  /**
   * Deletes all files under a directory, sending them to the trash.  Leaves the directory as is.
   * @param fs FileSystem to use
   * @param f path of directory
   * @param conf hive configuration
   * @return true if deletion successful
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static boolean trashFilesUnderDir(FileSystem fs, Path f, Configuration conf) throws FileNotFoundException, IOException {
    FileStatus[] statuses = fs.listStatus(f, HIDDEN_FILES_PATH_FILTER);
    boolean result = true;
    for (FileStatus status : statuses) {
      result = result & moveToTrash(fs, status.getPath(), conf);
    }
    return result;
  }

  /**
   * Move a particular file or directory to the trash.
   * @param fs FileSystem to use
   * @param f path of file or directory to move to trash.
   * @param conf
   * @return true if move successful
   * @throws IOException
   */
  public static boolean moveToTrash(FileSystem fs, Path f, Configuration conf) throws IOException {
    LOG.info("deleting  " + f);
    HadoopShims hadoopShim = ShimLoader.getHadoopShims();

    if (hadoopShim.moveToAppropriateTrash(fs, f, conf)) {
      LOG.info("Moved to trash: " + f);
      return true;
    }

    boolean result = fs.delete(f, true);
    if (!result) {
      LOG.error("Failed to delete " + f);
    }
    return result;
  }

  /**
   * Check if first path is a subdirectory of second path.
   * Both paths must belong to the same filesystem.
   *
   * @param p1 first path
   * @param p2 second path
   * @param fs FileSystem, both paths must belong to the same filesystem
   * @return
   */
  public static boolean isSubDir(Path p1, Path p2, FileSystem fs) {
    String path1 = fs.makeQualified(p1).toString();
    String path2 = fs.makeQualified(p2).toString();
    if (path1.startsWith(path2)) {
      return true;
    }

    return false;
  }

  public static boolean renameWithPerms(FileSystem fs, Path sourcePath,
                               Path destPath, boolean inheritPerms,
                               Configuration conf) throws IOException {
    LOG.info("Renaming " + sourcePath + " to " + destPath);
    if (!inheritPerms) {
      //just rename the directory
      return fs.rename(sourcePath, destPath);
    } else {
      //rename the directory
      if (fs.rename(sourcePath, destPath)) {
        HadoopShims shims = ShimLoader.getHadoopShims();
        HdfsFileStatus fullFileStatus = shims.getFullFileStatus(conf, fs, destPath.getParent());
        try {
          shims.setFullFileStatus(conf, fullFileStatus, fs, destPath);
        } catch (Exception e) {
          LOG.warn("Error setting permissions or group of " + destPath, e);
        }

        return true;
      }

      return false;
    }
  }

  /**
   * @param fs1
   * @param fs2
   * @return return true if both file system arguments point to same file system
   */
  public static boolean equalsFileSystem(FileSystem fs1, FileSystem fs2) {
    //When file system cache is disabled, you get different FileSystem objects
    // for same file system, so '==' can't be used in such cases
    //FileSystem api doesn't have a .equals() function implemented, so using
    //the uri for comparison. FileSystem already uses uri+Configuration for
    //equality in its CACHE .
    //Once equality has been added in HDFS-4321, we should make use of it
    return fs1.getUri().equals(fs2.getUri());
  }

  /**
   * Checks if delete can be performed on given path by given user.
   * If file does not exist it just returns without throwing an Exception
   * @param path
   * @param conf
   * @param user
   * @throws Exception
   */
  public static void checkDeletePermission(Path path, Configuration conf, String user) throws  Exception {

    if(path == null) {
      // no file/dir to be deleted
      return;
    }

    // check user has write permissions on the parent dir
    final FileSystem fs = path.getFileSystem(conf);
    FileStatus stat = null;
    try {
      stat = fs.getFileStatus(path);
    } catch (FileNotFoundException e) {
      // ignore
    }

    if (stat == null) {
      // no file/dir to be deleted
      return;
    }

    checkDeletePermission(fs, Lists.newArrayList(stat), conf, user);
  }

  /**
   * Checks if delete can be performed on given path by given user.
   * If file does not exist it just returns without throwing an Exception
   * @param fs The FileSystem instance
   * @param fileStatuses The FileStatus instances for the paths being checked.
   * @param conf Configuration, corresponding to the FileSystem.
   * @param user The user, whose permission is to be checked.
   * @throws Exception
   */
  public static void checkDeletePermission(FileSystem fs, Iterable<FileStatus> fileStatuses,
                                           Configuration conf, String user) throws Exception {

    // This requires ability to delete the given path.
    // The following 2 conditions should be satisfied for this-
    // 1. Write permissions on parent dir
    // 2. If sticky bit is set on parent dir then one of following should be
    // true
    //   a. User is owner of the current dir/file
    //   b. User is owner of the parent dir
    FileUtils.checkFileAccessWithImpersonation(fs, fileStatuses.iterator(), EnumSet.of(FsAction.WRITE), user);

    HadoopShims shims = ShimLoader.getHadoopShims();
    if (!shims.supportStickyBit()) {
      // No support for sticky-bit.
      return;
    }

    List<Path> allParentPaths =
        Lists.newArrayList(
            Iterators.transform(fileStatuses.iterator(), new Function<FileStatus, Path>() {
              @Override
              public Path apply(FileStatus input) {
                return input.getPath().getParent();
              }
            })
        );

    Iterator<FileStatus> childStatusIterator = fileStatuses.iterator();
    for (List<Path> parentPaths : Lists.partition(allParentPaths, getListStatusBatchSize(conf))) {
      for (FileStatus parentFileStatus : fs.listStatus(parentPaths.toArray(new Path[parentPaths.size()]))) {
        assert childStatusIterator.hasNext() : "Number of parent-file-statuses doesn't match children.";
        FileStatus childFileStatus = childStatusIterator.next();
        // Check sticky-bits on parent-dirs.
        if (shims.hasStickyBit(parentFileStatus.getPermission())
            && !parentFileStatus.getOwner().equals(user)
            && !childFileStatus.getOwner().equals(user)) {
          throw new IOException(String.format("Permission Denied: User %s can't delete %s because sticky bit is\""
              + " set on the parent dir and user does not own this file or its parent\"", user, childFileStatus.getPath()));
        }
      } // for_each( parent_path );
    } // for_each( batch_of_parentPaths );

    assert !childStatusIterator.hasNext() : "Did not process all file-statuses.";

  } // static void checkDeletePermission();

  private static int getListStatusBatchSize(Configuration configuration) {
    return HiveConf.getIntVar(configuration,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_HDFS_LIST_STATUS_BATCH_SIZE);
  }

  /**
   * Attempts to get file status.  This method differs from the FileSystem API in that it returns
   * null instead of throwing FileNotFoundException if the path does not exist.
   *
   * @param fs file system to check
   * @param path file system path to check
   * @return FileStatus for path or null if path does not exist
   * @throws IOException if there is an I/O error
   */
  public static FileStatus getFileStatusOrNull(FileSystem fs, Path path) throws IOException {
    try {
      return fs.getFileStatus(path);
    } catch (FileNotFoundException e) {
      return null;
    }
  }
}
