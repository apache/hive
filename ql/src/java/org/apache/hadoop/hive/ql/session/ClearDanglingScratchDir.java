/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.session;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool to remove dangling scratch directory. A scratch directory could be left behind
 * in some cases, such as when vm restarts and leave no chance for Hive to run shutdown hook.
 * The tool will test a scratch directory is use, if not, remove it.
 * We rely on HDFS write lock for to detect if a scratch directory is in use:
 * 1. A HDFS client open HDFS file ($scratchdir/inuse.lck) for write and only close
 *    it at the time the session is closed
 * 2. cleardanglingscratchDir can try to open $scratchdir/inuse.lck for write. If the
 *    corresponding HiveCli/HiveServer2 is still running, we will get exception.
 *    Otherwise, we know the session is dead
 * 3. If the HiveCli/HiveServer2 dies without closing the HDFS file, NN will reclaim the
 *    lease after 10 min, ie, the HDFS file hold by the dead HiveCli/HiveServer2 is writable
 *    again after 10 min. Once it become writable, cleardanglingscratchDir will be able to
 *    remove it
 * 4. Additional functionality; once it is decided which session scratch dirs are residual,
 *    while removing them from hdfs, we will remove them from local tmp location as well.
 *    Please see {@link ClearDanglingScratchDir#removeLocalTmpFiles(String, String)}.
 */
public class ClearDanglingScratchDir implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ClearDanglingScratchDir.class);
  boolean dryRun = false;
  boolean verbose = false;
  boolean useConsole = false;
  String rootHDFSDir;
  HiveConf conf;

  public static void main(String[] args) throws Exception {
    try {
      LogUtils.initHiveLog4j();
    } catch (LogInitializationException e) {
    }
    Options opts = createOptions();
    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("cleardanglingscratchdir"
          + " (clear scratch dir left behind by dead HiveCli or HiveServer2)", opts);
      return;
    }

    boolean dryRun = false;
    boolean verbose = false;

    if (cli.hasOption("r")) {
      dryRun = true;
      SessionState.getConsole().printInfo("dry-run mode on");
    }

    if (cli.hasOption("v")) {
      verbose = true;
    }

    HiveConf conf = new HiveConf();

    String rootHDFSDir;
    if (cli.hasOption("s")) {
      rootHDFSDir = cli.getOptionValue("s");
    } else {
      rootHDFSDir = HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR);
    }
    ClearDanglingScratchDir clearDanglingScratchDirMain = new ClearDanglingScratchDir(dryRun,
        verbose, true, rootHDFSDir, conf);
    clearDanglingScratchDirMain.run();
  }

  public ClearDanglingScratchDir(boolean dryRun, boolean verbose, boolean useConsole,
      String rootHDFSDir, HiveConf conf) {
    this.dryRun = dryRun;
    this.verbose = verbose;
    this.useConsole = useConsole;
    this.rootHDFSDir = rootHDFSDir;
    this.conf = conf;
  }

  @Override
  public void run() {
    try {
      Path rootHDFSDirPath = new Path(rootHDFSDir);
      FileSystem fs = FileSystem.get(rootHDFSDirPath.toUri(), conf);
      FileStatus[] userHDFSDirList = fs.listStatus(rootHDFSDirPath);

      List<Path> scratchDirToRemove = new ArrayList<Path>();
      for (FileStatus userHDFSDir : userHDFSDirList) {
        FileStatus[] scratchDirList = fs.listStatus(userHDFSDir.getPath());
        for (FileStatus scratchDir : scratchDirList) {
          Path lockFilePath = new Path(scratchDir.getPath(), SessionState.LOCK_FILE_NAME);
          if (!fs.exists(lockFilePath)) {
            String message = "Skipping " + scratchDir.getPath() + " since it does not contain " +
                SessionState.LOCK_FILE_NAME;
            if (verbose) {
              consoleMessage(message);
            }
            continue;
          }
          boolean removable = false;
          boolean inuse = false;
          try {
            IOUtils.closeStream(fs.append(lockFilePath));
            removable = true;
          } catch (RemoteException eAppend) {
            // RemoteException with AlreadyBeingCreatedException will be thrown
            // if the file is currently held by a writer
            if(AlreadyBeingCreatedException.class.getName().equals(eAppend.getClassName())){
              inuse = true;
            } else {
              consoleMessage("Unexpected error:" + eAppend.getMessage());
            }
          } catch (UnsupportedOperationException eUnsupported) {
            // In Hadoop-3, append method is not supported.
            // This is an alternative check to make sure whether a file is in use or not.
            // Trying to open the file. If it is in use, it will throw IOException.
            try {
              IOUtils.closeStream(fs.create(lockFilePath, false));
            } catch (RemoteException eCreate) {
              if (AlreadyBeingCreatedException.class.getName().equals(eCreate.getClassName())){
                // If the file is held by a writer, will throw AlreadyBeingCreatedException
                inuse = true;
              }  else {
                consoleMessage("Unexpected error:" + eCreate.getMessage());
              }
            } catch (FileAlreadyExistsException eCreateNormal) {
              // Otherwise, throw FileAlreadyExistsException, which means the file owner is dead
              removable = true;
            }
          }
          if (inuse) {
            // Cannot open the lock file for writing, must be held by a live process
            String message = scratchDir.getPath() + " is being used by live process";
            if (verbose) {
              consoleMessage(message);
            }
          }
          if (removable) {
            scratchDirToRemove.add(scratchDir.getPath());
          }
        }
      }

      if (scratchDirToRemove.size()==0) {
        consoleMessage("Cannot find any scratch directory to clear");
        return;
      }
      consoleMessage("Removing " + scratchDirToRemove.size() + " scratch directories");
      String localTmpDir = HiveConf.getVar(conf, HiveConf.ConfVars.LOCALSCRATCHDIR);
      for (Path scratchDir : scratchDirToRemove) {
        if (dryRun) {
          System.out.println(scratchDir);
        } else {
          boolean succ = fs.delete(scratchDir, true);
          if (!succ) {
            consoleMessage("Cannot remove " + scratchDir);
          } else {
            String message = scratchDir + " removed";
            if (verbose) {
              consoleMessage(message);
            }
          }
          // cleaning up on local file system as well
          removeLocalTmpFiles(scratchDir.getName(), localTmpDir);
        }
      }
    } catch (IOException e) {
      consoleMessage("Unexpected exception " + e.getMessage());
    }
  }

  private void consoleMessage(String message) {
    if (useConsole) {
      SessionState.getConsole().printInfo(message);
    } else {
      LOG.info(message);
    }
  }

  static Options createOptions() {
    Options result = new Options();

    // add -r and --dry-run to generate list only
    result.addOption(OptionBuilder
        .withLongOpt("dry-run")
        .withDescription("Generate a list of dangling scratch dir, printed on console")
        .create('r'));

    // add -s and --scratchdir to specify a non-default scratch dir
    result.addOption(OptionBuilder
        .withLongOpt("scratchdir")
        .withDescription("Specify a non-default location of the scratch dir")
        .hasArg()
        .create('s'));

    // add -v and --verbose to print verbose message
    result.addOption(OptionBuilder
        .withLongOpt("verbose")
        .withDescription("Print verbose message")
        .create('v'));

    result.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("print help message")
        .create('h'));

    return result;
  }

  /**
   * While deleting dangling scratch dirs from hdfs, we can clean corresponding local files as well
   * @param sessionName prefix to determine removable tmp files
   * @param localTmpdir local tmp file location
   */
  private void removeLocalTmpFiles(String sessionName, String localTmpdir) {
    File[] files = new File(localTmpdir).listFiles(fn -> fn.getName().startsWith(sessionName));
    boolean success;
    if (files != null) {
      for (File file : files) {
        success = false;
        if (file.canWrite()) {
          success = file.delete();
        }
        if (success) {
          consoleMessage("While removing '" + sessionName + "' dangling scratch dir from HDFS, "
                  + "local tmp session file '" + file.getPath() + "' has been cleaned as well.");
        } else if (file.getName().startsWith(sessionName)) {
          consoleMessage("Even though '" + sessionName + "' is marked as dangling session dir, "
                  + "local tmp session file '" + file.getPath() + "' could not be removed.");
        }
      }
    }
  }
}