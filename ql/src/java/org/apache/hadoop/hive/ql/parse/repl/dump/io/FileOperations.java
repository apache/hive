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
package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.CopyUtils;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;
import org.apache.hadoop.hive.shims.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.ErrorMsg.FILE_NOT_FOUND;

//TODO: this object is created once to call one method and then immediately destroyed.
//So it's basically just a roundabout way to pass arguments to a static method. Simplify?
public class FileOperations {
  private static Logger logger = LoggerFactory.getLogger(FileOperations.class);
  private final List<Path> dataPathList;
  private final Path exportRootDataDir;
  private final String distCpDoAsUser;
  private HiveConf hiveConf;
  private final MmContext mmCtx;
  private FileSystem dataFileSystem, exportFileSystem;

  public FileOperations(List<Path> dataPathList, Path exportRootDataDir, String distCpDoAsUser,
      HiveConf hiveConf, MmContext mmCtx) throws IOException {
    this.dataPathList = dataPathList;
    this.exportRootDataDir = exportRootDataDir;
    this.distCpDoAsUser = distCpDoAsUser;
    this.hiveConf = hiveConf;
    this.mmCtx = mmCtx;
    if ((dataPathList != null) && !dataPathList.isEmpty()) {
      dataFileSystem = dataPathList.get(0).getFileSystem(hiveConf);
    } else {
      dataFileSystem = null;
    }
    exportFileSystem = exportRootDataDir.getFileSystem(hiveConf);
  }

  public void export(ReplicationSpec forReplicationSpec) throws Exception {
    if (forReplicationSpec.isLazy()) {
      exportFilesAsList();
    } else {
      copyFiles();
    }
  }

  /**
   * This writes the actual data in the exportRootDataDir from the source.
   */
  private void copyFiles() throws IOException, LoginException {
    if (mmCtx == null) {
      for (Path dataPath : dataPathList) {
        copyOneDataPath(dataPath, exportRootDataDir);
      }
    } else {
      copyMmPath();
    }
  }

  private void copyOneDataPath(Path fromPath, Path toPath) throws IOException, LoginException {
    FileStatus[] fileStatuses = LoadSemanticAnalyzer.matchFilesOrDir(dataFileSystem, fromPath);
    List<Path> srcPaths = new ArrayList<>();
    for (FileStatus fileStatus : fileStatuses) {
      srcPaths.add(fileStatus.getPath());
    }

    new CopyUtils(distCpDoAsUser, hiveConf).doCopy(toPath, srcPaths);
  }

  private void copyMmPath() throws LoginException, IOException {
    ValidWriteIdList ids = AcidUtils.getTableValidWriteIdList(hiveConf, mmCtx.getFqTableName());
    for (Path fromPath : dataPathList) {
      fromPath = dataFileSystem.makeQualified(fromPath);
      List<Path> validPaths = new ArrayList<>(), dirsWithOriginals = new ArrayList<>();
      HiveInputFormat.processPathsForMmRead(dataPathList,
          hiveConf, ids, validPaths, dirsWithOriginals);
      String fromPathStr = fromPath.toString();
      if (!fromPathStr.endsWith(Path.SEPARATOR)) {
         fromPathStr += Path.SEPARATOR;
      }
      for (Path validPath : validPaths) {
        // Export valid directories with a modified name so they don't look like bases/deltas.
        // We could also dump the delta contents all together and rename the files if names collide.
        String mmChildPath = "export_old_" + validPath.toString().substring(fromPathStr.length());
        Path destPath = new Path(exportRootDataDir, mmChildPath);
        Utilities.FILE_OP_LOGGER.debug("Exporting {} to {}", validPath, destPath);
        exportFileSystem.mkdirs(destPath);
        copyOneDataPath(validPath, destPath);
      }
      for (Path dirWithOriginals : dirsWithOriginals) {
        FileStatus[] files = dataFileSystem.listStatus(dirWithOriginals, AcidUtils.hiddenFileFilter);
        List<Path> srcPaths = new ArrayList<>();
        for (FileStatus fileStatus : files) {
          if (fileStatus.isDirectory()) continue;
          srcPaths.add(fileStatus.getPath());
        }
        Utilities.FILE_OP_LOGGER.debug("Exporting originals from {} to {}",
            dirWithOriginals, exportRootDataDir);
        new CopyUtils(distCpDoAsUser, hiveConf).doCopy(exportRootDataDir, srcPaths);
      }
    }
  }



  /**
   * This needs the root data directory to which the data needs to be exported to.
   * The data export here is a list of files either in table/partition that are written to the _files
   * in the exportRootDataDir provided.
   */
  private void exportFilesAsList() throws SemanticException, IOException, LoginException {
    if (dataPathList.isEmpty()) {
      return;
    }
    boolean done = false;
    int repeat = 0;
    while (!done) {
      // This is only called for replication that handles MM tables; no need for mmCtx.
      try (BufferedWriter writer = writer()) {
        for (Path dataPath : dataPathList) {
          writeFilesList(listFilesInDir(dataPath), writer, AcidUtils.getAcidSubDir(dataPath));
        }
        done = true;
      } catch (IOException e) {
        if (e instanceof FileNotFoundException) {
          logger.error("exporting data files in dir : " + dataPathList + " to " + exportRootDataDir + " failed");
          throw new FileNotFoundException(FILE_NOT_FOUND.format(e.getMessage()));
        }
        repeat++;
        logger.info("writeFilesList failed", e);
        if (repeat >= FileUtils.MAX_IO_ERROR_RETRY) {
          logger.error("exporting data files in dir : " + dataPathList + " to " + exportRootDataDir + " failed");
          throw new IOException(ErrorMsg.REPL_FILE_SYSTEM_OPERATION_RETRY.getMsg());
        }

        int sleepTime = FileUtils.getSleepTime(repeat - 1);
        logger.info(" sleep for {} milliseconds for retry num {} ", sleepTime , repeat);
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException timerEx) {
          logger.info("thread sleep interrupted", timerEx.getMessage());
        }

        // in case of io error, reset the file system object
        FileSystem.closeAllForUGI(Utils.getUGI());
        dataFileSystem = dataPathList.get(0).getFileSystem(hiveConf);
        exportFileSystem = exportRootDataDir.getFileSystem(hiveConf);
        Path exportPath = new Path(exportRootDataDir, EximUtil.FILES_NAME);
        if (exportFileSystem.exists(exportPath)) {
          exportFileSystem.delete(exportPath, true);
        }
      }
    }
  }

  private void writeFilesList(FileStatus[] fileStatuses, BufferedWriter writer, String encodedSubDirs)
          throws IOException {
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        // Write files inside the sub-directory.
        Path subDir = fileStatus.getPath();
        writeFilesList(listFilesInDir(subDir), writer, encodedSubDir(encodedSubDirs, subDir));
      } else {
        writer.write(encodedUri(fileStatus, encodedSubDirs));
        writer.newLine();
      }
    }
  }

  private FileStatus[] listFilesInDir(Path path) throws IOException {
    return dataFileSystem.listStatus(path, p -> {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    });
  }

  private BufferedWriter writer() throws IOException {
    Path exportToFile = new Path(exportRootDataDir, EximUtil.FILES_NAME);
    if (exportFileSystem.exists(exportToFile)) {
      throw new IllegalArgumentException(
          exportToFile.toString() + " already exists and cant export data from path(dir) "
              + dataPathList);
    }
    logger.debug("exporting data files in dir : " + dataPathList + " to " + exportToFile);
    return new BufferedWriter(
        new OutputStreamWriter(exportFileSystem.create(exportToFile))
    );
  }

  private String encodedSubDir(String encodedParentDirs, Path subDir) {
    if (null == encodedParentDirs) {
      return subDir.getName();
    } else {
      return encodedParentDirs + Path.SEPARATOR + subDir.getName();
    }
  }

  private String encodedUri(FileStatus fileStatus, String encodedSubDir) throws IOException {
    Path currentDataFilePath = fileStatus.getPath();
    String checkSum = ReplChangeManager.checksumFor(currentDataFilePath, dataFileSystem);
    return ReplChangeManager.encodeFileUri(currentDataFilePath.toString(), checkSum, encodedSubDir);
  }
}
