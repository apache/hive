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

package org.apache.hadoop.hive.ql.parse.repl;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveFatalException;

import javax.security.auth.login.LoginException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CopyUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CopyUtils.class);
  // https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/TransparentEncryption.html#Running_as_the_superuser
  private static final String RAW_RESERVED_VIRTUAL_PATH = "/.reserved/raw/";
  private static final int MAX_COPY_RETRY = 5;

  private final HiveConf hiveConf;
  private final long maxCopyFileSize;
  private final long maxNumberOfFiles;
  private final boolean hiveInTest;
  private final String copyAsUser;

  public CopyUtils(String distCpDoAsUser, HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    maxNumberOfFiles = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES);
    maxCopyFileSize = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE);
    hiveInTest = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
    this.copyAsUser = distCpDoAsUser;
  }

  // Used by replication, copy files from source to destination. It is possible source file is
  // changed/removed during copy, so double check the checksum after copy,
  // if not match, copy again from cm
  public void copyAndVerify(FileSystem destinationFs, Path destRoot,
                    List<ReplChangeManager.FileInfo> srcFiles) throws IOException, LoginException, HiveFatalException {
    Map<FileSystem, Map< Path, List<ReplChangeManager.FileInfo>>> map = fsToFileMap(srcFiles, destRoot);
    for (Map.Entry<FileSystem, Map<Path, List<ReplChangeManager.FileInfo>>> entry : map.entrySet()) {
      FileSystem sourceFs = entry.getKey();
      Map<Path, List<ReplChangeManager.FileInfo>> destMap = entry.getValue();
      for (Map.Entry<Path, List<ReplChangeManager.FileInfo>> destMapEntry : destMap.entrySet()) {
        Path destination = destMapEntry.getKey();
        List<ReplChangeManager.FileInfo> fileInfoList = destMapEntry.getValue();
        boolean useRegularCopy = regularCopy(destinationFs, sourceFs, fileInfoList);

        if (!destinationFs.exists(destination)
                && !FileUtils.mkdir(destinationFs, destination, hiveConf)) {
          LOG.error("Failed to create destination directory: " + destination);
          throw new IOException("Destination directory creation failed");
        }

		    // Copy files with retry logic on failure or source file is dropped or changed.
        doCopyRetry(sourceFs, fileInfoList, destinationFs, destination, useRegularCopy);
      }
    }
  }

  private void doCopyRetry(FileSystem sourceFs, List<ReplChangeManager.FileInfo> srcFileList,
                           FileSystem destinationFs, Path destination,
                           boolean useRegularCopy) throws IOException, LoginException, HiveFatalException {
    int repeat = 0;
    boolean isCopyError = false;
    List<Path> pathList = Lists.transform(srcFileList, ReplChangeManager.FileInfo::getEffectivePath);
    while (!pathList.isEmpty() && (repeat < MAX_COPY_RETRY)) {
      try {
        // if its retrying, first regenerate the path list.
        if (repeat > 0) {
          pathList = getFilesToRetry(sourceFs, srcFileList, destinationFs, destination, isCopyError);
          if (pathList.isEmpty()) {
            // all files were copied successfully in last try. So can break from here.
            break;
          }
        }

        LOG.info("Attempt: " + (repeat+1) + ". Copying files: " + pathList);

        // if exception happens during doCopyOnce, then need to call getFilesToRetry with copy error as true in retry.
        isCopyError = true;
        doCopyOnce(sourceFs, pathList, destinationFs, destination, useRegularCopy);

        // if exception happens after doCopyOnce, then need to call getFilesToRetry with copy error as false in retry.
        isCopyError = false;
      } catch (IOException e) {
        // If copy fails, fall through the retry logic
        LOG.info("file operation failed", e);

        if (repeat >= (MAX_COPY_RETRY - 1)) {
          //no need to wait in the last iteration
          break;
        }

        if (!(e instanceof FileNotFoundException)) {
          int sleepTime = FileUtils.getSleepTime(repeat);
          LOG.info("Sleep for " + sleepTime + " milliseconds before retry " + (repeat+1));
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException timerEx) {
            LOG.info("sleep interrupted", timerEx.getMessage());
          }

          // looks like some network outrage, reset the file system object and retry.
          FileSystem.closeAllForUGI(Utils.getUGI());
          sourceFs = pathList.get(0).getFileSystem(hiveConf);
          destinationFs = destination.getFileSystem(hiveConf);
        }
      }
      repeat++;
    }

    // If still files remains to be copied due to failure/checksum mismatch after several attempts, then throw error
    if (!pathList.isEmpty()) {
      LOG.error("File copy failed even after several attempts. Files list: " + pathList);
      throw new IOException(ErrorMsg.REPL_FILE_SYSTEM_OPERATION_RETRY.getMsg());
    }
  }

  // Traverse through all the source files and see if any file is not copied or partially copied.
  // If yes, then add to the retry list. If source file missing, then retry with CM path. if CM path
  // itself is missing, then throw error.
  private List<Path> getFilesToRetry(FileSystem sourceFs, List<ReplChangeManager.FileInfo> srcFileList,
                                     FileSystem destinationFs, Path destination, boolean isCopyError)
          throws IOException, HiveFatalException {
    List<Path> pathList = new ArrayList<Path>();

    // Going through file list and make the retry list
    for (ReplChangeManager.FileInfo srcFile : srcFileList) {
      if (srcFile.isCopyDone()) {
        // If already copied successfully, ignore it.
        continue;
      }
      Path srcPath = srcFile.getEffectivePath();
      Path destPath = new Path(destination, srcPath.getName());
      if (destinationFs.exists(destPath)) {
        // If destination file is present and checksum of source mismatch, then retry copy.
        if (isSourceFileMismatch(sourceFs, srcFile)) {
          // Delete the incorrectly copied file and retry with CM path
          destinationFs.delete(destPath, true);
          srcFile.setIsUseSourcePath(false);
        } else {
          // If the retry logic is reached after copy error, then include the copied file as well.
          // This is needed as we cannot figure out which file is incorrectly copied.
          // Expecting distcp to skip the properly copied file based on CRC check or copy it if CRC mismatch.

          if (!isCopyError) {
            // File is successfully copied, just skip this file from retry.
            srcFile.setCopyDone(true);
            continue;
          }
        }
      } else if (isSourceFileMismatch(sourceFs, srcFile)) {
        // If checksum does not match, likely the file is changed/removed, retry from CM path
        srcFile.setIsUseSourcePath(false);
      }

      srcPath = srcFile.getEffectivePath();
      if (null == srcPath) {
        // This case possible if CM path is not enabled.
        LOG.error("File copy failed and likely source file is deleted or modified."
                + "Source File: " + srcFile.getSourcePath());
        throw new HiveFatalException(ErrorMsg.REPL_FILE_MISSING_FROM_SRC_AND_CM_PATH.getMsg());
      }

      if (!srcFile.isUseSourcePath() && !sourceFs.exists(srcFile.getCmPath())) {
        // CM path itself is missing, cannot recover from this error
        LOG.error("File Copy Failed. Both source and CM files are missing from source. "
                + "Missing Source File: " + srcFile.getSourcePath() + ", CM File: " + srcFile.getCmPath() + ". "
                + "Try setting higher value for hive.repl.cm.retain in source warehouse. "
                + "Also, bootstrap the system again to get back the consistent replicated state.");
        throw new HiveFatalException(ErrorMsg.REPL_FILE_MISSING_FROM_SRC_AND_CM_PATH.getMsg());
      }

      pathList.add(srcPath);
    }
    return pathList;
  }

  // Check if the source file unmodified even after copy to see if we copied the right file
  private boolean isSourceFileMismatch(FileSystem sourceFs, ReplChangeManager.FileInfo srcFile) throws IOException {
    // If source is already CM path, the checksum will be always matching
    if (srcFile.isUseSourcePath()) {
      String sourceChecksumString = srcFile.getCheckSum();
      if (sourceChecksumString != null) {
        String verifySourceChecksumString;
        try {
          verifySourceChecksumString
                  = ReplChangeManager.checksumFor(srcFile.getSourcePath(), sourceFs);
        } catch (IOException e) {
          LOG.info("Unable to calculate checksum for source file: " + srcFile.getSourcePath(), e);

          if (!sourceFs.exists(srcFile.getSourcePath())) {
            // if source file is missing, then return true, so that cm path will be used for copy.
            return true;
          }
          throw e;
        }
        if (!sourceChecksumString.equals(verifySourceChecksumString)) {
          return true;
        }
      }
    }
    return false;
  }

  // Copy without retry
  private void doCopyOnce(FileSystem sourceFs, List<Path> srcList,
                          FileSystem destinationFs, Path destination,
                          boolean useRegularCopy) throws IOException, LoginException {
    UserGroupInformation ugi = Utils.getUGI();
    String currentUser = ugi.getShortUserName();
    boolean usePrivilegedUser = copyAsUser != null && !currentUser.equals(copyAsUser);

    if (useRegularCopy) {
      doRegularCopyOnce(sourceFs, srcList, destinationFs, destination, usePrivilegedUser);
    } else {
      doDistCpCopyOnce(sourceFs, srcList, destination, usePrivilegedUser);
    }
  }

  private void doDistCpCopyOnce(FileSystem sourceFs, List<Path> srcList, Path destination,
      boolean usePrivilegedUser) throws IOException {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.REPL_ADD_RAW_RESERVED_NAMESPACE)) {
      srcList = srcList.stream().map(path -> {
        URI uri = path.toUri();
        return new Path(uri.getScheme(), uri.getAuthority(),
            RAW_RESERVED_VIRTUAL_PATH + uri.getPath());
      }).collect(Collectors.toList());
      URI destinationUri = destination.toUri();
      destination = new Path(destinationUri.getScheme(), destinationUri.getAuthority(),
          RAW_RESERVED_VIRTUAL_PATH + destinationUri.getPath());
    }

    if (!FileUtils.distCp(
        sourceFs, // source file system
        srcList,  // list of source paths
        destination,
        false,
        usePrivilegedUser ? copyAsUser : null,
        hiveConf,
        ShimLoader.getHadoopShims())) {
      LOG.error("Distcp failed to copy files: " + srcList + " to destination: " + destination);
      throw new IOException("Distcp operation failed.");
    }
  }

  private void doRegularCopyOnce(FileSystem sourceFs, List<Path> srcList, FileSystem destinationFs,
      Path destination, boolean usePrivilegedUser) throws IOException {
  /*
    even for regular copy we have to use the same user permissions that distCp will use since
    hive-server user might be different that the super user required to copy relevant files.
   */
    final Path[] paths = srcList.toArray(new Path[] {});
    if (usePrivilegedUser) {
      final Path finalDestination = destination;
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
          copyAsUser, UserGroupInformation.getLoginUser());
      try {
        proxyUser.doAs((PrivilegedExceptionAction<Boolean>) () -> {
          FileUtil
              .copy(sourceFs, paths, destinationFs, finalDestination, false, true, hiveConf);
          return true;
        });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    } else {
      FileUtil.copy(sourceFs, paths, destinationFs, destination, false, true, hiveConf);
    }
  }

  public void doCopy(Path destination, List<Path> srcPaths) throws IOException, LoginException {
    Map<FileSystem, List<Path>> map = fsToPathMap(srcPaths);
    FileSystem destinationFs = destination.getFileSystem(hiveConf);

    for (Map.Entry<FileSystem, List<Path>> entry : map.entrySet()) {
      final FileSystem sourceFs = entry.getKey();
      List<ReplChangeManager.FileInfo> fileList = Lists.transform(entry.getValue(),
              path -> new ReplChangeManager.FileInfo(sourceFs, path, null));
      doCopyOnce(sourceFs, entry.getValue(),
                 destinationFs, destination,
                 regularCopy(destinationFs, sourceFs, fileList));
    }
  }

  /*
      Check for conditions that will lead to local copy, checks are:
      1. we are testing hive.
      2. either source or destination is a "local" FileSystem("file")
      3. aggregate fileSize of all source Paths(can be directory /  file) is less than configured size.
      4. number of files of all source Paths(can be directory /  file) is less than configured size.
  */
  boolean regularCopy(FileSystem destinationFs, FileSystem sourceFs, List<ReplChangeManager.FileInfo> fileList)
      throws IOException {
    if (hiveInTest) {
      return true;
    }
    if (isLocal(sourceFs) || isLocal(destinationFs)) {
      return true;
    }

    /*
       we have reached the point where we are transferring files across fileSystems.
    */
    long size = 0;
    long numberOfFiles = 0;

    for (ReplChangeManager.FileInfo fileInfo : fileList) {
      ContentSummary contentSummary = null;
      try {
        contentSummary = sourceFs.getContentSummary(fileInfo.getEffectivePath());
      } catch (IOException e) {
        // In replication, if source file does not exist, try cmroot
        if (fileInfo.isUseSourcePath() && fileInfo.getCmPath() != null) {
          contentSummary = sourceFs.getContentSummary(fileInfo.getCmPath());
          fileInfo.setIsUseSourcePath(false);
        }
      }
      if (contentSummary != null) {
        size += contentSummary.getLength();
        numberOfFiles += contentSummary.getFileCount();
        if (limitReachedForLocalCopy(size, numberOfFiles)) {
          return false;
        }
      }
    }
    return true;
  }

  boolean limitReachedForLocalCopy(long size, long numberOfFiles) {
    boolean result = size > maxCopyFileSize && numberOfFiles > maxNumberOfFiles;
    if (result) {
      LOG.info("Source is {} bytes. (MAX: {})", size, maxCopyFileSize);
      LOG.info("Source is {} files. (MAX: {})", numberOfFiles, maxNumberOfFiles);
      LOG.info("going to launch distributed copy (distcp) job.");
    }
    return result;
  }

  private boolean isLocal(FileSystem fs) {
    return fs.getScheme().equals("file");
  }

  private Map<FileSystem, List<Path>> fsToPathMap(List<Path> srcPaths) throws IOException {
    Map<FileSystem, List<Path>> result = new HashMap<>();
    for (Path path : srcPaths) {
      FileSystem fileSystem = path.getFileSystem(hiveConf);
      if (!result.containsKey(fileSystem)) {
        result.put(fileSystem, new ArrayList<>());
      }
      result.get(fileSystem).add(path);
    }
    return result;
  }

  // Create map of source file system to destination path to list of files to copy
  private Map<FileSystem, Map<Path, List<ReplChangeManager.FileInfo>>> fsToFileMap(
      List<ReplChangeManager.FileInfo> srcFiles, Path destRoot) throws IOException {
    Map<FileSystem, Map<Path, List<ReplChangeManager.FileInfo>>> result = new HashMap<>();
    for (ReplChangeManager.FileInfo file : srcFiles) {
      FileSystem fileSystem = file.getSrcFs();
      if (!result.containsKey(fileSystem)) {
        result.put(fileSystem, new HashMap<>());
      }
      Path destination = getCopyDestination(file, destRoot);
      if (!result.get(fileSystem).containsKey(destination)) {
        result.get(fileSystem).put(destination, new ArrayList<>());
      }
      result.get(fileSystem).get(destination).add(file);
    }
    return result;
  }

  public static Path getCopyDestination(ReplChangeManager.FileInfo fileInfo, Path destRoot) {
    if (fileInfo.getSubDir() == null) {
      return destRoot;
    }
    String[] subDirs = fileInfo.getSubDir().split(Path.SEPARATOR);
    Path destination = destRoot;
    for (String subDir: subDirs) {
      destination = new Path(destination, subDir);
    }
    return destination;
  }
}
