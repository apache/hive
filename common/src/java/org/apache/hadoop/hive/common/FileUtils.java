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

package org.apache.hadoop.hive.common;

import static org.apache.hadoop.hive.shims.Utils.RAW_RESERVED_VIRTUAL_PATH;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.Map;
import java.util.StringTokenizer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import com.google.common.base.Preconditions;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.hive.common.util.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;

/**
 * Collection of file manipulation utilities common across Hive.
 */
public final class FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class.getName());
  private static final Random random = new Random();
  public static final int IO_ERROR_SLEEP_TIME = 100;

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

  /**
   * Hex encoding characters indexed by integer value
   */
  private static final char[] HEX_UPPER_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

  static boolean needsEscaping(char c) {
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

    //  Fast-path detection, no escaping and therefore no copying necessary
    int firstEscapeIndex = -1;
    for (int i = 0; i < path.length(); i++) {
      if (needsEscaping(path.charAt(i))) {
        firstEscapeIndex = i;
        break;
      }
    }
    if (firstEscapeIndex == -1) {
      return path;
    }

    // slow path, escape beyond the first required escape character into a new string
    StringBuilder sb = new StringBuilder();
    if (firstEscapeIndex > 0) {
      sb.append(path, 0, firstEscapeIndex);
    }

    for (int i = firstEscapeIndex; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%').append(HEX_UPPER_CHARS[(0xF0 & c) >>> 4]).append(HEX_UPPER_CHARS[(0x0F & c)]);
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  public static String unescapePathName(String path) {
    int firstUnescapeIndex = path.indexOf('%');
    if (firstUnescapeIndex == -1) {
      return path;
    }

    StringBuilder sb = new StringBuilder();
    if (firstUnescapeIndex > 0) {
      sb.append(path, 0, firstUnescapeIndex);
    }

    for (int i = firstUnescapeIndex; i < path.length(); i++) {
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
    if (isS3a(fs)) {
      // S3A file system has an optimized recursive directory listing implementation however it doesn't support filtering.
      // Therefore we filter the result set afterwards. This might be not so optimal in HDFS case (which does a tree walking) where a filter could have been used.
      listS3FilesRecursive(fileStatus, fs, results);
    } else {
      generalListStatusRecursively(fs, fileStatus, results);
    }
  }

  private static void generalListStatusRecursively(FileSystem fs, FileStatus fileStatus, List<FileStatus> results) throws IOException {
    if (fileStatus.isDirectory()) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath(), HIDDEN_FILES_PATH_FILTER)) {
        generalListStatusRecursively(fs, stat, results);
      }
    } else {
      results.add(fileStatus);
    }
  }

  private static void listS3FilesRecursive(FileStatus base, FileSystem fs, List<FileStatus> results) throws IOException {
    if (!base.isDirectory()) {
      results.add(base);
      return;
    }
    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(base.getPath(), true);
    while (remoteIterator.hasNext()) {
      LocatedFileStatus each = remoteIterator.next();
      Path relativePath = makeRelative(base.getPath(), each.getPath());
      if (org.apache.hadoop.hive.metastore.utils.FileUtils.RemoteIteratorWithFilter.HIDDEN_FILES_FULL_PATH_FILTER.accept(relativePath)) {
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

  public static void checkFileAccessWithImpersonation(final FileSystem fs, final FileStatus stat, final FsAction action,
      final String user) throws Exception {
    UserGroupInformation proxyUser = null;
    try {
      proxyUser = getProxyUser(user);
      FileSystem fsAsUser = FileUtils.getFsAsUser(fs, proxyUser);
      checkFileAccessWithImpersonation(fs, stat, action, user, null, fsAsUser);
    } finally {
      closeFs(proxyUser);
    }
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
   * @param children List of children to be collected. If this is null, no children are collected.
   *        To be set only if this is a directory
   * @throws IOException
   * @throws AccessControlException
   * @throws InterruptedException
   * @throws Exception
   */
  public static void checkFileAccessWithImpersonation(final FileSystem fs, final FileStatus stat, final FsAction action,
      final String user, final List<FileStatus> children, final FileSystem fsAsUser)
      throws IOException, AccessControlException, InterruptedException, Exception {
    UserGroupInformation ugi = Utils.getUGI();
    String currentUser = ugi.getShortUserName();

    if (user == null || currentUser.equals(user)) {
      // No need to impersonate user, do the checks as the currently configured user.
      ShimLoader.getHadoopShims().checkFileAccess(fs, stat, action);
      addChildren(fs, stat.getPath(), children);
      return;
    }

    // Otherwise, try user impersonation. Current user must be configured to do user impersonation.
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
    proxyUser.doAs(new PrivilegedExceptionAction<Object>() {
      @Override public Object run() throws Exception {
        ShimLoader.getHadoopShims().checkFileAccess(fsAsUser, stat, action);
        addChildren(fsAsUser, stat.getPath(), children);
        return null;
      }
    });
  }

  private static void addChildren(FileSystem fsAsUser, Path path, List<FileStatus> children) throws IOException {
    if (children != null) {
      FileStatus[] listStatus;
      try {
        listStatus = fsAsUser.listStatus(path);
      } catch (IOException e) {
        LOG.warn("Unable to list files under " + path + " : " + e);
        throw e;
      }
      children.addAll(Arrays.asList(listStatus));
    }
  }

  public static UserGroupInformation getProxyUser(final String user) throws LoginException, IOException {
    UserGroupInformation ugi = Utils.getUGI();
    String currentUser = ugi.getShortUserName();
    UserGroupInformation proxyUser = null;
    if (user != null && !user.equals(currentUser)) {
      proxyUser = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
    }
    return proxyUser;
  }

  public static void closeFs(UserGroupInformation proxyUser) throws IOException {
    if (proxyUser != null) {
      FileSystem.closeAllForUGI(proxyUser);
    }
  }

  public static FileSystem getFsAsUser(FileSystem fs, UserGroupInformation proxyUser) throws IOException, InterruptedException {
    if (proxyUser == null) {
      return null;
    }
    FileSystem fsAsUser = proxyUser.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override public FileSystem run() throws Exception {
        return FileSystem.get(fs.getUri(), fs.getConf());
      }
    });
    return fsAsUser;
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
    UserGroupInformation proxyUser = null;
    boolean isPermitted = false;
    try {
      proxyUser = getProxyUser(userName);
      FileSystem fsAsUser = getFsAsUser(fs, proxyUser);
      isPermitted = isActionPermittedForFileHierarchy(fs, fileStatus, userName, action, true, fsAsUser);
    } finally {
      closeFs(proxyUser);
    }
    return isPermitted;
  }

  public static boolean isActionPermittedForFileHierarchy(FileSystem fs, FileStatus fileStatus,
      String userName, FsAction action, boolean recurse) throws Exception {
    UserGroupInformation proxyUser = null;
    boolean isPermitted;
    try {
      proxyUser = getProxyUser(userName);
      FileSystem fsAsUser = getFsAsUser(fs, proxyUser);
      isPermitted = isActionPermittedForFileHierarchy(fs, fileStatus, userName, action, recurse, fsAsUser);
    } finally {
      closeFs(proxyUser);
    }
    return isPermitted;
  }

  @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "Intended, dir privilege all-around bug")
  public static boolean isActionPermittedForFileHierarchy(FileSystem fs, FileStatus fileStatus,
      String userName, FsAction action, boolean recurse, FileSystem fsAsUser) throws Exception {
    boolean isDir = fileStatus.isDirectory();

    // for dirs user needs execute privileges as well
    FsAction dirActionNeeded = (isDir) ? action.and(FsAction.EXECUTE) : action;

    List<FileStatus> subDirsToCheck = null;
    if (isDir && recurse) {
      subDirsToCheck = new ArrayList<FileStatus>();
    }

    try {
      checkFileAccessWithImpersonation(fs, fileStatus, action, userName, subDirsToCheck, fsAsUser);
    } catch (AccessControlException err) {
      // Action not permitted for user
      LOG.warn("Action " + action + " denied on " + fileStatus.getPath() + " for user " + userName);
      return false;
    }

    if (subDirsToCheck == null || subDirsToCheck.isEmpty()) {
      // no sub dirs to be checked
      return true;
    }

    // check all children
    for (FileStatus childStatus : subDirsToCheck) {
      // check children recursively - recurse is true if we're here.
      if (!isActionPermittedForFileHierarchy(fs, childStatus, userName, action, true, fsAsUser)) {
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
      throws IOException, InterruptedException {
    return isOwnerOfFileHierarchy(fs, fileStatus, userName, true);
  }

  public static boolean isOwnerOfFileHierarchy(final FileSystem fs,
      final FileStatus fileStatus, final String userName, final boolean recurse)
      throws IOException, InterruptedException {
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(userName,
        UserGroupInformation.getLoginUser());
    try {
      boolean isOwner = proxyUser.doAs(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws Exception {
          FileSystem fsAsUser = FileSystem.get(fs.getUri(), fs.getConf());
          return checkIsOwnerOfFileHierarchy(fsAsUser, fileStatus, userName, recurse);
        }
      });
      return isOwner;
    } finally {
      FileSystem.closeAllForUGI(proxyUser);
    }
  }

  public static boolean checkIsOwnerOfFileHierarchy(FileSystem fs, FileStatus fileStatus,
      String userName, boolean recurse)
      throws IOException {
    if (!fileStatus.getOwner().equals(userName)) {
      return false;
    }

    if ((!fileStatus.isDirectory()) || (!recurse)) {
      // no sub dirs to be checked
      return true;
    }
    // check all children
    FileStatus[] childStatuses = null;
    try {
      childStatuses = fs.listStatus(fileStatus.getPath());
    } catch (FileNotFoundException fe) {
      LOG.debug("Skipping child access check since the directory is already removed");
      return true;
    }
    for (FileStatus childStatus : childStatuses) {
      // check children recursively - recurse is true if we're here.
      if (!checkIsOwnerOfFileHierarchy(fs, childStatus, userName, true)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Creates the directory and all necessary parent directories.
   * @param fs FileSystem to use
   * @param f path to create.
   * @param conf Hive configuration
   * @return true if directory created successfully.  False otherwise, including if it exists.
   * @throws IOException exception in creating the directory
   */
  public static boolean mkdir(FileSystem fs, Path f, Configuration conf) throws IOException {
    LOG.info("Creating directory if it doesn't exist: " + f);
    return fs.mkdirs(f);
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
      HiveConf conf, DataCopyStatistics copyStatistics
  ) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf, ShimLoader.getHadoopShims(), copyStatistics);
  }

  @VisibleForTesting
  static boolean copy(FileSystem srcFS, Path src,
    FileSystem dstFS, Path dst,
    boolean deleteSource,
    boolean overwrite,
    HiveConf conf, HadoopShims shims, DataCopyStatistics copyStatistics) throws IOException {

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
        copied = distCp(srcFS, Collections.singletonList(src), dst, deleteSource, null, conf, shims);
        // increment bytes copied counter
        copyStatistics.incrementBytesCopiedCounter(srcContentSummary.getLength());
      }
    }
    if (!triedDistcp) {
      // Note : Currently, this implementation does not "fall back" to regular copy if distcp
      // is tried and it fails. We depend upon that behaviour in cases like replication,
      // wherein if distcp fails, there is good reason to not plod along with a trivial
      // implementation, and fail instead.
      copied = doIOUtilsCopyBytes(srcFS, srcFS.getFileStatus(src), dstFS, dst, deleteSource, overwrite, shouldPreserveXAttrs(conf, srcFS, dstFS, src), conf, copyStatistics);
    }
    return copied;
  }

  public static boolean doIOUtilsCopyBytes(FileSystem srcFS, FileStatus srcStatus, FileSystem dstFS, Path dst, boolean deleteSource,
                             boolean overwrite, boolean preserveXAttrs, Configuration conf, DataCopyStatistics copyStatistics) throws IOException {
    Path src = srcStatus.getPath();
    dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcStatus.isDirectory()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }

      FileStatus[] fileStatus = srcFS.listStatus(src);
      for (FileStatus file : fileStatus) {
        doIOUtilsCopyBytes(srcFS, file, dstFS, new Path(dst, file.getPath().getName()), deleteSource, overwrite, preserveXAttrs, conf, copyStatistics);
      }
      if (preserveXAttrs) {
        preserveXAttr(srcFS, src, dstFS, dst);
      }
    } else {
      InputStream in = null;
      FSDataOutputStream out = null;

      try {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        IOUtils.copyBytes(in, out, conf, true);
        if (preserveXAttrs) {
          preserveXAttr(srcFS, src, dstFS, dst);
        }
        final long bytesCopied = srcFS.getFileStatus(src).getLen();
        copyStatistics.incrementBytesCopiedCounter(bytesCopied);
      } catch (IOException var11) {
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        throw var11;
      }
    }

    return deleteSource ? srcFS.delete(src, true) : true;
  }

  public static boolean copy(FileSystem srcFS, Path[] srcs, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, boolean preserveXAttr, Configuration conf,
                             DataCopyStatistics copyStatistics) throws IOException {
    boolean gotException = false;
    boolean returnVal = true;
    StringBuilder exceptions = new StringBuilder();
    if (srcs.length == 1) {
      return doIOUtilsCopyBytes(srcFS, srcFS.getFileStatus(srcs[0]), dstFS, dst, deleteSource, overwrite, preserveXAttr, conf, copyStatistics);
    } else {
      try {
        FileStatus sdst = dstFS.getFileStatus(dst);
        if (!sdst.isDirectory()) {
          throw new IOException("copying multiple files, but last argument `" + dst + "' is not a directory");
        }
      } catch (FileNotFoundException var16) {
        throw new IOException("`" + dst + "': specified destination directory does not exist", var16);
      }

      Path[] var17 = srcs;
      int var11 = srcs.length;

      for(int var12 = 0; var12 < var11; ++var12) {
        Path src = var17[var12];

        try {
          if (!doIOUtilsCopyBytes(srcFS, srcFS.getFileStatus(src), dstFS, dst, deleteSource, overwrite, preserveXAttr, conf, copyStatistics)) {
            returnVal = false;
          }
        } catch (IOException var15) {
          gotException = true;
          exceptions.append(var15.getMessage());
          exceptions.append("\n");
        }
      }

      if (gotException) {
        throw new IOException(exceptions.toString());
      } else {
        return returnVal;
      }
    }
  }

  private static void preserveXAttr(FileSystem srcFS, Path src, FileSystem dstFS, Path dst) throws IOException {
    for (Map.Entry<String, byte[]> attr : srcFS.getXAttrs(src).entrySet()) {
      dstFS.setXAttr(dst, attr.getKey(), attr.getValue());
    }
  }

  private static Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite) throws IOException {
    FileStatus sdst;
    try {
      sdst = dstFS.getFileStatus(dst);
    } catch (FileNotFoundException var6) {
      sdst = null;
    }
    if (null != sdst) {
      if (sdst.isDirectory()) {
        if (null == srcName) {
          throw new PathIsDirectoryException(dst.toString());
        }

        return checkDest((String)null, dstFS, new Path(dst, srcName), overwrite);
      }

      if (!overwrite) {
        throw new PathExistsException(dst.toString(), "Target " + dst + " already exists");
      }
    }

    return dst;
  }

  private static void checkDependencies(FileSystem srcFS, Path src, FileSystem dstFS, Path dst) throws IOException {
    if (srcFS == dstFS) {
      String srcq = srcFS.makeQualified(src).toString() + "/";
      String dstq = dstFS.makeQualified(dst).toString() + "/";
      if (dstq.startsWith(srcq)) {
        throw new IOException((srcq.length() == dstq.length()) ?
                "Cannot copy " + src + " to itself." : "Cannot copy " + src + " to its subdirectory " + dst);
      }
    }
  }

  public static boolean shouldPreserveXAttrs(HiveConf conf, FileSystem srcFS, FileSystem dstFS, Path path) throws IOException {
    Preconditions.checkNotNull(path);
    if (conf.getBoolVar(ConfVars.DFS_XATTR_ONLY_SUPPORTED_ON_RESERVED_NAMESPACE)) {

      if (!(path.toUri().getPath().startsWith(RAW_RESERVED_VIRTUAL_PATH)
        && Utils.checkFileSystemXAttrSupport(srcFS, new Path(RAW_RESERVED_VIRTUAL_PATH))
        && Utils.checkFileSystemXAttrSupport(dstFS, new Path(RAW_RESERVED_VIRTUAL_PATH)))) {
        return false;
      }
    } else {
      if (!Utils.checkFileSystemXAttrSupport(srcFS) || !Utils.checkFileSystemXAttrSupport(dstFS)) {
        return false;
      }
    }
    for (Map.Entry<String, String> entry : conf.getPropsWithPrefix(Utils.DISTCP_OPTIONS_PREFIX).entrySet()) {
      String distCpOption = entry.getKey();
      if (distCpOption.startsWith("p")) {
        return distCpOption.contains("x");
      }
    }
    return true;
  }

  public static boolean distCp(FileSystem srcFS, List<Path> srcPaths, Path dst,
      boolean deleteSource, UserGroupInformation proxyUser,
      HiveConf conf, HadoopShims shims) throws IOException {
    LOG.debug("copying srcPaths : {}, to DestPath :{} ,with doAs: {}",
        StringUtils.join(",", srcPaths), dst.toString(), proxyUser);
    boolean copied = false;
    if (proxyUser == null){
      copied = shims.runDistCp(srcPaths, dst, conf);
    } else {
      copied = shims.runDistCpAs(srcPaths, dst, conf, proxyUser);
    }
    if (copied && deleteSource) {
      if (proxyUser != null) {
        // if distcp is done using doAsUser, delete also should be done using same user.
        //TODO : Need to change the delete execution within doAs if doAsUser is given.
        throw new IOException("Distcp is called with doAsUser and delete source set as true");
      }
      for (Path path : srcPaths) {
        srcFS.delete(path, true);
      }
    }
    return copied;
  }

  public static boolean distCpWithSnapshot(String oldSnapshot, String newSnapshot, List<Path> srcPaths, Path dst,
      boolean overwriteTarget, HiveConf conf, HadoopShims shims, UserGroupInformation proxyUser) {
    boolean copied = false;
    try {
      if (proxyUser == null) {
        copied = shims.runDistCpWithSnapshots(oldSnapshot, newSnapshot, srcPaths, dst, overwriteTarget, conf);
      } else {
        copied =
            shims.runDistCpWithSnapshotsAs(oldSnapshot, newSnapshot, srcPaths, dst, overwriteTarget, proxyUser, conf);
      }
      if (copied)
        LOG.info("Successfully copied using snapshots source {} and dest {} using snapshots {} and {}", srcPaths, dst,
            oldSnapshot, newSnapshot);
    } catch (IOException e) {
      LOG.error("Can not copy using snapshot from source: {}, target: {}", srcPaths, dst);
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

  public static boolean rename(FileSystem fs, Path sourcePath,
                               Path destPath, Configuration conf) throws IOException {
    LOG.info("Renaming " + sourcePath + " to " + destPath);

    // If destPath directory exists, rename call will move the sourcePath
    // into destPath without failing. So check it before renaming.
    if (fs.exists(destPath)) {
      throw new IOException("Cannot rename the source path. The destination "
          + "path already exists.");
    }
    return fs.rename(sourcePath, destPath);
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
  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", justification = "Intended")
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
    if (org.apache.commons.lang3.StringUtils.isBlank(pathString)) {
      return Collections.emptySet();
    }
    Set<String> result = new HashSet<>();
    String[] paths = pathString.split(",");
    for (final String path : paths) {
      try {
        Path p = new Path(getURI(path));
        FileSystem fs = p.getFileSystem(conf);
        FileStatus fileStatus = fs.getFileStatus(p);
        if (fileStatus.isDirectory()) {
          // add all jar files under the folder
          FileStatus[] files = fs.listStatus(p, new GlobFilter("*.jar"));
          for (FileStatus file : files) {
            result.add(file.getPath().toUri().toString());
          }
        } else {
          result.add(p.toUri().toString());
        }
      } catch (FileNotFoundException fnfe) {
        LOG.error("The jar file path {} does not exist", path);
      } catch (URISyntaxException | IOException e) {
        LOG.error("Invalid file path {}", path, e);
      }
    }
    return result;
  }

  /**
   * Reads length bytes of data from the stream into the byte buffer.
   * @param stream Stream to read from.
   * @param length The number of bytes to read.
   * @param bb The buffer to read into; the data is written at current position and then the
   *           position is incremented by length.
   * @throws EOFException the length bytes cannot be read. The buffer position is not modified.
   */
  public static void readFully(InputStream stream, int length, ByteBuffer bb) throws IOException {
    byte[] b = null;
    int offset = 0;
    if (bb.hasArray()) {
      b = bb.array();
      offset = bb.arrayOffset() + bb.position();
    } else {
      b = new byte[bb.remaining()];
    }
    int fullLen = length;
    while (length > 0) {
      int result = stream.read(b, offset, length);
      if (result < 0) {
        throw new EOFException("Reading " + fullLen + " bytes");
      }
      offset += result;
      length -= result;
    }
    if (!bb.hasArray()) {
      bb.put(b);
    } else {
      bb.position(bb.position() + fullLen);
    }
  }

  /**
   * Returns the incremented sleep time in milli seconds.
   * @param repeatNum number of retry done so far.
   */
  public static int getSleepTime(int repeatNum) {
    return IO_ERROR_SLEEP_TIME * (int)(Math.pow(2.0, repeatNum));
  }

  /**
   * Attempts to delete a file if it exists.
   * @param fs FileSystem
   * @param path Path to be deleted.
   */
  public static void deleteIfExists(FileSystem fs, Path path) {
    try {
      fs.delete(path, true);
    } catch (IOException e) {
      LOG.debug("Unable to delete {}", path, e);
    }
  }

  public static RemoteIterator<FileStatus> listStatusIterator(FileSystem fs, Path path, PathFilter filter)
        throws IOException {
    return RemoteIterators.filteringRemoteIterator(fs.listStatusIterator(path),
        status -> filter.accept(status.getPath()));
  }

  public static RemoteIterator<LocatedFileStatus> listFiles(FileSystem fs, Path path, boolean recursive, PathFilter filter)
        throws IOException {
    return RemoteIterators.filteringRemoteIterator(fs.listFiles(path, recursive),
        status -> filter.accept(status.getPath()));
  }

  /**
   * Resolves a symlink on a local filesystem. In case of any exceptions or scheme other than "file"
   * it simply returns the original path. Refer to DEBUG level logs for further details.
   * @param path input path to be resolved
   * @param conf a Configuration instance to be used while e.g. resolving the FileSystem if necessary
   * @return the resolved target Path or the original if the input Path is not a symlink
   * @throws IOException
   */
  public static Path resolveSymlinks(Path path, Configuration conf) throws IOException {
    if (path == null) {
      throw new IllegalArgumentException("Cannot resolve symlink for a null Path");
    }

    URI uri = path.toUri();
    String scheme = uri.getScheme();

    /*
     * If you're about to extend this method to e.g. HDFS, simply remove this check.
     * There is a known exception reproduced by whroot_external1.q, which can be referred to,
     * which is because java.nio is not prepared by default for other schemes like "hdfs".
     */
    if (scheme != null && !"file".equalsIgnoreCase(scheme)) {
      LOG.debug("scheme '{}' is not supported for resolving symlinks", scheme);
      return path;
    }

    // we're expecting 'file' scheme, so if scheme == null, we need to add it to path before resolving,
    // otherwise Paths.get will fail with java.lang.IllegalArgumentException: Missing scheme
    if (scheme == null) {
      try {
        uri =  new URI("file", uri.getAuthority(), uri.toString(), null, null);
      } catch (URISyntaxException e) {
        // e.g. in case of relative URI, we cannot create a new URI
        LOG.debug("URISyntaxException while creating uri from path without scheme {}", path, e);
        return path;
      }
    }

    try {
      java.nio.file.Path srcPath = Paths.get(uri);
      URI targetUri = srcPath.toRealPath().toUri();
      // stick to the original scheme
      return new Path(scheme, targetUri.getAuthority(),
          Path.getPathWithoutSchemeAndAuthority(new Path(targetUri)).toString());
    } catch (Exception e) {
      LOG.debug("Exception while calling toRealPath of {}", path, e);
      return path;
    }
  }

  public static class AdaptingIterator<T> implements Iterator<T> {

    private final RemoteIterator<T> iterator;

    @Override
    public boolean hasNext() {
      try {
        return iterator.hasNext();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public T next() {
      try {
        if (iterator.hasNext()) {
          return iterator.next();
        } else {
          throw new NoSuchElementException();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public AdaptingIterator(RemoteIterator<T> iterator) {
      this.iterator = iterator;
    }
  }

  /**
   * Checks whether the filesystem are equal, if they are equal and belongs to ozone then check if they belong to
   * same bucket and volume.
   * @param srcFs source filesystem
   * @param destFs target filesystem
   * @param src source path
   * @param dest target path
   * @return true if filesystems are equal, if Ozone fs, then the path belongs to same bucket-volume.
   */
  public static boolean isEqualFileSystemAndSameOzoneBucket(FileSystem srcFs, FileSystem destFs, Path src, Path dest) {
    if (!equalsFileSystem(srcFs, destFs)) {
      return false;
    }
    if (srcFs.getScheme().equalsIgnoreCase("ofs") || srcFs.getScheme().equalsIgnoreCase("o3fs")) {
      return isSameOzoneBucket(src, dest);
    }
    return true;
  }

  public static boolean isSameOzoneBucket(Path src, Path dst) {
    String[] src1 = getVolumeAndBucket(src);
    String[] dst1 = getVolumeAndBucket(dst);

    return ((src1[0] == null && dst1[0] == null) || (src1[0] != null && src1[0].equalsIgnoreCase(dst1[0]))) &&
        ((src1[1] == null && dst1[1] == null) || (src1[1] != null && src1[1].equalsIgnoreCase(dst1[1])));
  }

  private static String[] getVolumeAndBucket(Path path) {
    URI uri = path.toUri();
    final String pathStr = uri.getPath();
    StringTokenizer token = new StringTokenizer(pathStr, "/");
    int numToken = token.countTokens();

    if (numToken >= 2) {
      return new String[] { token.nextToken(), token.nextToken() };
    } else if (numToken == 1) {
      return new String[] { token.nextToken(), null };
    } else {
      return new String[] { null, null };
    }
  }
}
