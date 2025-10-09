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
package org.apache.hadoop.hive.ql.hooks;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.orc.tools.FileDump;
import org.apache.orc.FileFormatException;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.collect.Lists;

/**
 * Post exec hook to print orc file dump for files that will be read by fetch task. The file dump
 * output will be printed before fetch task output. It also prints the row index for the 1st column
 * in the file just to verify the impact of bloom filter fpp.
 */
public class PostExecOrcFileDump implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(PostExecOrcFileDump.class.getName());

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert (hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);
    HiveConf conf = hookContext.getConf();

    LOG.info("Executing post execution hook to print orc file dump..");
    QueryPlan plan = hookContext.getQueryPlan();
    if (plan == null) {
      return;
    }

    FetchTask fetchTask = plan.getFetchTask();
    if (fetchTask != null) {
      SessionState ss = SessionState.get();
      SessionState.LogHelper console = ss.getConsole();

      // file dump should write to session state console's error stream
      PrintStream old = System.out;
      System.setOut(console.getErrStream());

      FetchWork fetchWork = fetchTask.getWork();
      boolean partitionedTable = fetchWork.isPartitioned();
      List<Path> directories;
      if (partitionedTable) {
        LOG.info("Printing orc file dump for files from partitioned directory..");
        directories = Lists.newArrayList(fetchWork.getPartDir());
      } else {
        LOG.info("Printing orc file dump for files from table directory..");
        directories = Lists.newArrayList(fetchWork.getTblDir());
      }

      Collections.sort(directories);
      for (Path dir : directories) {
        printFileStatus(console, dir.getFileSystem(conf), dir);
      }

      // restore the old out stream
      System.out.flush();
      System.setOut(old);
    }
  }

  private void printFileStatus(SessionState.LogHelper console, FileSystem fs, Path dir) throws Exception {
    List<FileStatus> fileList = Arrays.asList(fs.listStatus(dir, hiddenFileFilter));
    Collections.sort(fileList);

    for (FileStatus fileStatus : fileList) {
      if (fileStatus.isDirectory()) {

        // delta directory in case of ACID
        printFileStatus(console, fs, fileStatus.getPath());
      } else {

        LOG.info("Printing orc file dump for " + fileStatus.getPath());
        if (fileStatus.getLen() > 0) {
          try {
            try (Reader notUsed = OrcFile.createReader(fs, fileStatus.getPath())) {
              // just creating orc reader is going to do sanity checks to make sure its valid ORC file
            }
            console.printError("-- BEGIN ORC FILE DUMP --");
            FileDump.main(new String[]{fileStatus.getPath().toString(), "--rowindex=*"});
            console.printError("-- END ORC FILE DUMP --");
          } catch (FileFormatException e) {
            LOG.warn("File " + fileStatus.getPath() + " is not ORC. Skip printing orc file dump");
          } catch (IOException e) {
            LOG.warn("Skip printing orc file dump. Exception: " + e.getMessage());
          }
        } else {
          LOG.warn("Zero length file encountered. Skip printing orc file dump.");
        }
      }
    }
  }

}
