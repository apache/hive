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

import javax.security.auth.login.LoginException;
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
  private static final int MAX_COPY_RETRY = 3;

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
  public void copyAndVerify(FileSystem destinationFs, Path destination,
                    List<ReplChangeManager.FileInfo> srcFiles) throws IOException, LoginException {
    Map<FileSystem, List<ReplChangeManager.FileInfo>> map = fsToFileMap(srcFiles);
    for (Map.Entry<FileSystem, List<ReplChangeManager.FileInfo>> entry : map.entrySet()) {
      FileSystem sourceFs = entry.getKey();
      List<ReplChangeManager.FileInfo> fileInfoList = entry.getValue();
      boolean useRegularCopy = regularCopy(destinationFs, sourceFs, fileInfoList);

      doCopyRetry(sourceFs, fileInfoList, destinationFs, destination, useRegularCopy);

      // Verify checksum, retry if checksum changed
      List<ReplChangeManager.FileInfo> retryFileInfoList = new ArrayList<>();
      for (ReplChangeManager.FileInfo srcFile : srcFiles) {
        if(!srcFile.isUseSourcePath()) {
          // If already use cmpath, nothing we can do here, skip this file
          continue;
        }
        String sourceChecksumString = srcFile.getCheckSum();
        if (sourceChecksumString != null) {
          String verifySourceChecksumString;
          try {
            verifySourceChecksumString
                    = ReplChangeManager.checksumFor(srcFile.getSourcePath(), sourceFs);
          } catch (IOException e) {
            // Retry with CM path
            verifySourceChecksumString = null;
          }
          if ((verifySourceChecksumString == null)
                  || !sourceChecksumString.equals(verifySourceChecksumString)) {
            // If checksum does not match, likely the file is changed/removed, copy again from cm
            srcFile.setIsUseSourcePath(false);
            retryFileInfoList.add(srcFile);
          }
        }
      }
      if (!retryFileInfoList.isEmpty()) {
        doCopyRetry(sourceFs, retryFileInfoList, destinationFs, destination, useRegularCopy);
      }
    }
  }

  private void doCopyRetry(FileSystem sourceFs, List<ReplChangeManager.FileInfo> fileList,
                           FileSystem destinationFs, Path destination,
                           boolean useRegularCopy) throws IOException, LoginException {
    int repeat = 0;
    List<Path> pathList = Lists.transform(fileList, ReplChangeManager.FileInfo::getEffectivePath);
    while (!pathList.isEmpty() && (repeat < MAX_COPY_RETRY)) {
      try {
        doCopyOnce(sourceFs, pathList, destinationFs, destination, useRegularCopy);
        return;
      } catch (IOException e) {
        pathList = new ArrayList<>();

        // Going through file list, retry with CM if applicable
        for (ReplChangeManager.FileInfo file : fileList) {
          Path copyPath = file.getEffectivePath();
          if (!destinationFs.exists(new Path(destination, copyPath.getName()))) {
            if (!sourceFs.exists(copyPath)) {
              if (file.isUseSourcePath()) {
                // Source file missing, then try with CM path
                file.setIsUseSourcePath(false);
              } else {
                // CM path itself is missing, so, cannot recover from this error
                throw e;
              }
            }
            pathList.add(file.getEffectivePath());
          }
        }
      }
      repeat++;
    }
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
      hiveConf.set("distcp.options.px","");
    }

    FileUtils.distCp(
        sourceFs, // source file system
        srcList,  // list of source paths
        destination,
        false,
        usePrivilegedUser ? copyAsUser : null,
        hiveConf,
        ShimLoader.getHadoopShims()
    );
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
                                path -> { return new ReplChangeManager.FileInfo(sourceFs, path);});
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
  private boolean regularCopy(FileSystem destinationFs, FileSystem sourceFs, List<ReplChangeManager.FileInfo> fileList)
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
        // in replication, if source file does not exist, try cmroot
        if (fileInfo.isUseSourcePath() && fileInfo.getCmPath() != null) {
          contentSummary = sourceFs.getContentSummary(fileInfo.getCmPath());
          fileInfo.setIsUseSourcePath(false);
        }
      }
      size += contentSummary.getLength();
      numberOfFiles += contentSummary.getFileCount();
      if (limitReachedForLocalCopy(size, numberOfFiles)) {
        return false;
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
        result.put(fileSystem, new ArrayList<Path>());
      }
      result.get(fileSystem).add(path);
    }
    return result;
  }

  private Map<FileSystem, List<ReplChangeManager.FileInfo>> fsToFileMap(
      List<ReplChangeManager.FileInfo> srcFiles) throws IOException {
    Map<FileSystem, List<ReplChangeManager.FileInfo>> result = new HashMap<>();
    for (ReplChangeManager.FileInfo file : srcFiles) {
      FileSystem fileSystem = file.getSrcFs();
      if (!result.containsKey(fileSystem)) {
        result.put(fileSystem, new ArrayList<ReplChangeManager.FileInfo>());
      }
      result.get(fileSystem).add(file);
    }
    return result;
  }
}
