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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of file manipulation utilities common across Hive.
 */
public final class FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class.getName());
  private static final Random random = new Random();

  public static final PathFilter HIDDEN_FILES_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  public static final PathFilter STAGING_DIR_PATH_FILTER = new PathFilter() {
    @Override
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
    UserGroupInformation ugi = Utils.getUGI();
    String currentUser = ugi.getShortUserName();

    if (user == null || currentUser.equals(user)) {
      // No need to impersonate user, do the checks as the currently configured user.
      ShimLoader.getHadoopShims().checkFileAccess(fs, stat, action);
      return;
    }

    // Otherwise, try user impersonation. Current user must be configured to do user impersonation.
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
        user, UserGroupInformation.getLoginUser());
    try {
      proxyUser.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          FileSystem fsAsUser = FileSystem.get(fs.getUri(), fs.getConf());
          ShimLoader.getHadoopShims().checkFileAccess(fsAsUser, stat, action);
          return null;
        }
      });
    } finally {
      FileSystem.closeAllForUGI(proxyUser);
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
    return isActionPermittedForFileHierarchy(fs,fileStatus,userName, action, true);
  }

  public static boolean isActionPermittedForFileHierarchy(FileSystem fs, FileStatus fileStatus,
      String userName, FsAction action, boolean recurse) throws Exception {
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

    if ((!isDir) || (!recurse)) {
      // no sub dirs to be checked
      return true;
    }
    // check all children
    FileStatus[] childStatuses = fs.listStatus(fileStatus.getPath());
    for (FileStatus childStatus : childStatuses) {
      // check children recursively - recurse is true if we're here.
      if (!isActionPermittedForFileHierarchy(fs, childStatus, userName, action, true)) {
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
    return isOwnerOfFileHierarchy(fs, fileStatus, userName, true);
  }

  public static boolean isOwnerOfFileHierarchy(FileSystem fs, FileStatus fileStatus,
      String userName, boolean recurse)
      throws IOException {
    if (!fileStatus.getOwner().equals(userName)) {
      return false;
    }

    if ((!fileStatus.isDir()) || (!recurse)) {
      // no sub dirs to be checked
      return true;
    }
    // check all children
    FileStatus[] childStatuses = fs.listStatus(fileStatus.getPath());
    for (FileStatus childStatus : childStatuses) {
      // check children recursively - recurse is true if we're here.
      if (!isOwnerOfFileHierarchy(fs, childStatus, userName, true)) {
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
        //set on the entire subtree
        if (inheritPerms) {
          HdfsUtils.setFullFileStatus(conf,
                  new HdfsUtils.HadoopFileStatus(conf, fs, lastExistingParent), fs,
                  firstNonExistentParent, true);
        }
        return true;
      }
    }
  }

  public static Path makeAbsolute(FileSystem fileSystem, Path path) throws IOException {
    if (path.isAbsolute()) {
      return path;
    } else {
      return new Path(fileSystem.getWorkingDirectory(), path);
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
    return copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf, ShimLoader.getHadoopShims());
  }

  @VisibleForTesting
  static boolean copy(FileSystem srcFS, Path src,
    FileSystem dstFS, Path dst,
    boolean deleteSource,
    boolean overwrite,
    HiveConf conf, HadoopShims shims) throws IOException {

    boolean copied = false;
    boolean triedDistcp = false;

    /* Run distcp if source file/dir is too big */
    if (srcFS.getUri().getScheme().equals("hdfs")) {
      ContentSummary srcContentSummary = srcFS.getContentSummary(src);
      if (srcContentSummary.getFileCount() > conf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES)
              && srcContentSummary.getLength() > conf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE)) {

        LOG.info("Source is " + srcContentSummary.getLength() + " bytes. (MAX: " + conf.getLongVar(
                HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE) + ")");
        LOG.info("Source is " + srcContentSummary.getFileCount() + " files. (MAX: " + conf.getLongVar(
                HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES) + ")");
        LOG.info("Launch distributed copy (distcp) job.");
        triedDistcp = true;
        copied = shims.runDistCp(src, dst, conf);
        if (copied && deleteSource) {
          srcFS.delete(src, true);
        }
      }
    }
    if (!triedDistcp) {
      copied = FileUtil.copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf);
    }

    boolean inheritPerms = conf.getBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
    if (copied && inheritPerms) {
      HdfsUtils.setFullFileStatus(conf, new HdfsUtils.HadoopFileStatus(conf, dstFS, dst.getParent()), dstFS, dst, true);
    }
    return copied;
  }

  /**
   * Move a particular file or directory to the trash.
   * @param fs FileSystem to use
   * @param f path of file or directory to move to trash.
   * @param conf
   * @return true if move successful
   * @throws IOException
   */
  public static boolean moveToTrash(FileSystem fs, Path f, Configuration conf, boolean purge)
      throws IOException {
    LOG.debug("deleting  " + f);
    boolean result = false;
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

  public static boolean renameWithPerms(FileSystem fs, Path sourcePath,
                               Path destPath, boolean inheritPerms,
                               Configuration conf) throws IOException {
    LOG.info("Renaming " + sourcePath + " to " + destPath);

    // If destPath directory exists, rename call will move the sourcePath
    // into destPath without failing. So check it before renaming.
    if (fs.exists(destPath)) {
      throw new IOException("Cannot rename the source path. The destination "
          + "path already exists.");
    }

    if (!inheritPerms) {
      //just rename the directory
      return fs.rename(sourcePath, destPath);
    } else {
      //rename the directory
      if (fs.rename(sourcePath, destPath)) {
        HdfsUtils.setFullFileStatus(conf, new HdfsUtils.HadoopFileStatus(conf, fs, destPath.getParent()), fs, destPath,
                true);
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
    //Once equality has been added in HDFS-9159, we should make use of it
    return fs1.getUri().equals(fs2.getUri());
  }

  /**
   * Checks if delete can be performed on given path by given user.
   * If file does not exist it just returns without throwing an Exception
   * @param path
   * @param conf
   * @param user
   * @throws AccessControlException
   * @throws InterruptedException
   * @throws Exception
   */
  public static void checkDeletePermission(Path path, Configuration conf, String user)
      throws AccessControlException, InterruptedException, Exception {
   // This requires ability to delete the given path.
    // The following 2 conditions should be satisfied for this-
    // 1. Write permissions on parent dir
    // 2. If sticky bit is set on parent dir then one of following should be
    // true
    //   a. User is owner of the current dir/file
    //   b. User is owner of the parent dir
    //   Super users are also allowed to drop the file, but there is no good way of checking
    //   if a user is a super user. Also super users running hive queries is not a common
    //   use case. super users can also do a chown to be able to drop the file

    if(path == null) {
      // no file/dir to be deleted
      return;
    }

    final FileSystem fs = path.getFileSystem(conf);
    // check user has write permissions on the parent dir
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
    FileUtils.checkFileAccessWithImpersonation(fs, stat, FsAction.WRITE, user);

    HadoopShims shims = ShimLoader.getHadoopShims();
    if (!shims.supportStickyBit()) {
      // not supports sticky bit
      return;
    }

    // check if sticky bit is set on the parent dir
    FileStatus parStatus = fs.getFileStatus(path.getParent());
    if (!shims.hasStickyBit(parStatus.getPermission())) {
      // no sticky bit, so write permission on parent dir is sufficient
      // no further checks needed
      return;
    }

    // check if user is owner of parent dir
    if (parStatus.getOwner().equals(user)) {
      return;
    }

    // check if user is owner of current dir/file
    FileStatus childStatus = fs.getFileStatus(path);
    if (childStatus.getOwner().equals(user)) {
      return;
    }
    String msg = String.format("Permission Denied: User %s can't delete %s because sticky bit is"
        + " set on the parent dir and user does not own this file or its parent", user, path);
    throw new IOException(msg);

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

  public static void deleteDirectory(File directory) throws IOException {
    org.apache.commons.io.FileUtils.deleteDirectory(directory);
  }

  /**
   * create temporary file and register it to delete-on-exit hook.
   * File.deleteOnExit is not used for possible memory leakage.
   *
   * Make sure to use {@link #deleteTmpFile(File)} after the file is no longer required,
   * and has been deleted to avoid a memory leak.
   */
  public static File createTempFile(String lScratchDir, String prefix, String suffix) throws IOException {
    File tmpDir = lScratchDir == null ? null : new File(lScratchDir);
    if (tmpDir != null && !tmpDir.exists() && !tmpDir.mkdirs()) {
      // Do another exists to check to handle possible race condition
      // Another thread might have created the dir, if that is why
      // mkdirs returned false, that is fine
      if (!tmpDir.exists()) {
        throw new RuntimeException("Unable to create temp directory "
            + lScratchDir);
      }
    }
    File tmpFile = File.createTempFile(prefix, suffix, tmpDir);
    ShutdownHookManager.deleteOnExit(tmpFile);
    return tmpFile;
  }

  public static File createLocalDirsTempFile(String localDirList, String prefix, String suffix,
      boolean isDirectory) throws IOException {
    if (localDirList == null || localDirList.isEmpty()) {
      return createFileInTmp(prefix, suffix, "Local directories not specified", isDirectory);
    }
    String[] localDirs = StringUtils.getTrimmedStrings(localDirList);
    if (localDirs.length == 0) {
      return createFileInTmp(prefix, suffix, "Local directories not specified", isDirectory);
    }
    // TODO: we could stagger these to threads by ID, but that can also lead to bad effects.
    String path = localDirs[random.nextInt(localDirs.length)];
    if (path == null || path.isEmpty()) {
      return createFileInTmp(prefix, suffix, "Empty path for one of the local dirs", isDirectory);
    }
    File targetDir = new File(path);
    if (!targetDir.exists() && !targetDir.mkdirs()) {
      return createFileInTmp(prefix, suffix, "Cannot access or create " + targetDir, isDirectory);
    }
    try {
      File file = File.createTempFile(prefix, suffix, targetDir);
      if (isDirectory && (!file.delete() || !file.mkdirs())) {
        // TODO: or we could just generate a name ourselves and not do this?
        return createFileInTmp(prefix, suffix,
            "Cannot recreate " + file + " as directory", isDirectory);
      }
      file.deleteOnExit();
      return file;
    } catch (IOException ex) {
      LOG.error("Error creating a file in " + targetDir, ex);
      return createFileInTmp(prefix, suffix, "Cannot create a file in " + targetDir, isDirectory);
    }
  }

  private static File createFileInTmp(String prefix, String suffix,
      String reason, boolean isDirectory) throws IOException {
    File file = File.createTempFile(prefix, suffix);
    if (isDirectory && (!file.delete() || !file.mkdirs())) {
      // TODO: or we could just generate a name ourselves and not do this?
      throw new IOException("Cannot recreate " + file + " as directory");
    }
    file.deleteOnExit();
    LOG.info(reason + "; created a tmp file: " + file.getAbsolutePath());
    return file;
  }

  public static File createLocalDirsTempFile(Configuration conf, String prefix, String suffix,
      boolean isDirectory) throws IOException {
    return createLocalDirsTempFile(
        conf.get("yarn.nodemanager.local-dirs"), prefix, suffix, isDirectory);
  }

  /**
   * delete a temporary file and remove it from delete-on-exit hook.
   */
  public static boolean deleteTmpFile(File tempFile) {
    if (tempFile != null) {
      tempFile.delete();
      ShutdownHookManager.cancelDeleteOnExit(tempFile);
      return true;
    }
    return false;
  }
  
  
  /**
   * Return whenever all paths in the collection are schemaless
   * 
   * @param paths
   * @return
   */
  public static boolean pathsContainNoScheme(Collection<Path> paths) {
    for( Path path  : paths){
      if(path.toUri().getScheme() != null){
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the deepest candidate path for the given path.
   * 
   * prioritizes on paths including schema / then includes matches without schema
   * 
   * @param path
   * @param candidates  the candidate paths
   * @return
   */
  public static Path getParentRegardlessOfScheme(Path path, Collection<Path> candidates) {
    Path schemalessPath = Path.getPathWithoutSchemeAndAuthority(path);
    
    for(;path!=null && schemalessPath!=null; path=path.getParent(),schemalessPath=schemalessPath.getParent()){
      if(candidates.contains(path))
        return path;
      if(candidates.contains(schemalessPath))
        return schemalessPath;
    }
    // exception?
    return null;
  }

  /**
   * Checks whenever path is inside the given subtree
   * 
   * return true iff
   *  * path = subtree
   *  * subtreeContains(path,d) for any descendant of the subtree node
   * @param path    the path in question
   * @param subtree
   * 
   * @return
   */
  public static boolean isPathWithinSubtree(Path path, Path subtree) {
    return isPathWithinSubtree(path, subtree, subtree.depth());
  }

  private static boolean isPathWithinSubtree(Path path, Path subtree, int subtreeDepth) {
    while(path != null){
      if (subtreeDepth > path.depth()) {
        return false;
      }
      if(subtree.equals(path)){
        return true;
      }
      path = path.getParent();
    }
    return false;
  }

  public static void populateParentPaths(Set<Path> parents, Path path) {
    if (parents == null) {
      return;
    }
    while(path != null) {
      parents.add(path);
      path = path.getParent();
    }
  }

  /**
   * Get the URI of the path. Assume to be local file system if no scheme.
   */
  public static URI getURI(String path) throws URISyntaxException {
    if (path == null) {
      return null;
    }

    URI uri = new URI(path);
    if (uri.getScheme() == null) {
      // if no scheme in the path, we assume it's file on local fs.
      uri = new File(path).toURI();
    }

    return uri;
  }

  /**
   * Given a path string, get all the jars from the folder or the files themselves.
   *
   * @param pathString  the path string is the comma-separated path list
   * @return            the list of the file names in the format of URI formats.
   */
  public static Set<String> getJarFilesByPath(String pathString, Configuration conf) {
    Set<String> result = new HashSet<String>();
    if (pathString == null || org.apache.commons.lang.StringUtils.isBlank(pathString)) {
        return result;
    }

    String[] paths = pathString.split(",");
    for(String path : paths) {
      try {
        Path p = new Path(getURI(path));
        FileSystem fs = p.getFileSystem(conf);
        if (!fs.exists(p)) {
          LOG.error("The jar file path " + path + " doesn't exist");
          continue;
        }
        if (fs.isDirectory(p)) {
          // add all jar files under the folder
          FileStatus[] files = fs.listStatus(p, new GlobFilter("*.jar"));
          for(FileStatus file : files) {
            result.add(file.getPath().toUri().toString());
          }
        } else {
          result.add(p.toUri().toString());
        }
      } catch(URISyntaxException | IOException e) {
        LOG.error("Invalid file path " + path, e);
      }
    }
    return result;
  }

}
