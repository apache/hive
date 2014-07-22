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
package org.apache.hive.hcatalog.templeton;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.ExecuteException;
import org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;

/**
 * Submit a Sqoop job.
 *
 * This is the backend of the Sqoop web service.
 */
public class SqoopDelegator extends LauncherDelegator {

  public SqoopDelegator(AppConfig appConf) {
    super(appConf);
  }

  public EnqueueBean run(String user,
               Map<String, Object> userArgs, String command, 
               String optionsFile, String otherFiles, String statusdir, 
               String callback, String completedUrl, boolean enablelog)
  throws NotAuthorizedException, BadParam, BusyException, QueueException,
  ExecuteException, IOException, InterruptedException
  {
    runAs = user;
    List<String> args = makeArgs(command, optionsFile, otherFiles, statusdir,
                   completedUrl, enablelog);

    return enqueueController(user, userArgs, callback, args);
  }

  List<String> makeArgs(String command, String optionsFile, String otherFiles,
            String statusdir, String completedUrl, boolean enablelog)
    throws BadParam, IOException, InterruptedException
  {
    ArrayList<String> args = new ArrayList<String>();
    try {
      args.addAll(makeBasicArgs(optionsFile, otherFiles, statusdir, completedUrl, enablelog));
      args.add("--");
      TempletonUtils.addCmdForWindows(args);
      args.add(appConf.sqoopPath());
      if (TempletonUtils.isset(command)) {
        String[] temArgs = command.split(" ");
        for (int i = 0; i < temArgs.length; i++) {
          args.add(TempletonUtils.quoteForWindows(temArgs[i]));

          // The token file location and mapreduce job tag should be right after the tool argument
          if (i == 0 && !temArgs[i].startsWith("--")) {
            args.add("-D" + TempletonControllerJob.TOKEN_FILE_ARG_PLACEHOLDER);
            args.add("-D" + TempletonControllerJob.MAPREDUCE_JOB_TAGS_ARG_PLACEHOLDER);
          }
        }
      } else if (TempletonUtils.isset(optionsFile)) {
        args.add("--options-file");
        args.add(TempletonUtils.hadoopFsPath(optionsFile, appConf, runAs)
                        .getName());
      }
    } catch (FileNotFoundException e) {
      throw new BadParam(e.getMessage());
    } catch (URISyntaxException e) {
      throw new BadParam(e.getMessage());
    }
    return args;
  }

  private List<String> makeBasicArgs(String optionsFile, String otherFiles,
            String statusdir, String completedUrl, boolean enablelog)
    throws URISyntaxException, FileNotFoundException, IOException,
                          InterruptedException
  {
    ArrayList<String> args = new ArrayList<String>();
    ArrayList<String> allFiles = new ArrayList<String>();
    if (TempletonUtils.isset(optionsFile))
      allFiles.add(TempletonUtils.hadoopFsFilename(optionsFile, appConf, runAs));
    if (TempletonUtils.isset(otherFiles)) {
      String[] ofs = TempletonUtils.hadoopFsListAsArray(otherFiles, appConf, runAs);
      allFiles.addAll(Arrays.asList(ofs));
    }
    args.addAll(makeLauncherArgs(appConf, statusdir, completedUrl, allFiles,
                enablelog, JobType.SQOOP));
    if (appConf.sqoopArchive() != null && !appConf.sqoopArchive().equals("")) {
      args.add("-archives");
      args.add(appConf.sqoopArchive());
    }
    return args;
  }
}
