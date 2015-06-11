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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.templeton.tool.JobSubmissionConstants;
import org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;

/**
 * Submit a Hive job.
 *
 * This is the backend of the hive web service.
 */
public class HiveDelegator extends LauncherDelegator {

  public HiveDelegator(AppConfig appConf) {
    super(appConf);
  }

  public EnqueueBean run(String user, Map<String, Object> userArgs,
               String execute, String srcFile, List<String> defines,
               List<String> hiveArgs, String otherFiles,
               String statusdir, String callback, String completedUrl, boolean enablelog,
               Boolean enableJobReconnect)
    throws NotAuthorizedException, BadParam, BusyException, QueueException,
    ExecuteException, IOException, InterruptedException
  {
    runAs = user;
    List<String> args = makeArgs(execute, srcFile, defines, hiveArgs, otherFiles, statusdir,
                   completedUrl, enablelog, enableJobReconnect);

    return enqueueController(user, userArgs, callback, args);
  }

  private List<String> makeArgs(String execute, String srcFile,
             List<String> defines, List<String> hiveArgs, String otherFiles,
             String statusdir, String completedUrl, boolean enablelog,
             Boolean enableJobReconnect)
    throws BadParam, IOException, InterruptedException
  {
    ArrayList<String> args = new ArrayList<String>();
    try {
      args.addAll(makeBasicArgs(execute, srcFile, otherFiles, statusdir, completedUrl,
          enablelog, enableJobReconnect));
      args.add("--");
      TempletonUtils.addCmdForWindows(args);
      addHiveMetaStoreTokenArg();
      
      args.add(appConf.hivePath());

      args.add("--service");
      args.add("cli");

      //the token file location as initial hiveconf arg
      args.add("--hiveconf");
      args.add(TempletonControllerJob.TOKEN_FILE_ARG_PLACEHOLDER);

      //this is needed specifcally for Hive on Tez (in addition to
      //JobSubmissionConstants.TOKEN_FILE_ARG_PLACEHOLDER)
      args.add("--hiveconf");
      args.add(JobSubmissionConstants.TOKEN_FILE_ARG_PLACEHOLDER_TEZ);

      //add mapreduce job tag placeholder
      args.add("--hiveconf");
      args.add(TempletonControllerJob.MAPREDUCE_JOB_TAGS_ARG_PLACEHOLDER);

      for (String prop : appConf.hiveProps()) {
        args.add("--hiveconf");
        args.add(TempletonUtils.quoteForWindows(prop));
      }
      for (String prop : defines) {
        args.add("--hiveconf");
        args.add(TempletonUtils.quoteForWindows(prop));
      }
      for (String hiveArg : hiveArgs) {
        args.add(TempletonUtils.quoteForWindows(hiveArg));
      }
      if (TempletonUtils.isset(execute)) {
        args.add("-e");
        args.add(TempletonUtils.quoteForWindows(execute));
      } else if (TempletonUtils.isset(srcFile)) {
        args.add("-f");
        args.add(TempletonUtils.hadoopFsPath(srcFile, appConf, runAs)
            .getName());
      }
    } catch (FileNotFoundException e) {
      throw new BadParam(e.getMessage());
    } catch (URISyntaxException e) {
      throw new BadParam(e.getMessage());
    }

    return args;
  }

  private List<String> makeBasicArgs(String execute, String srcFile, String otherFiles,
                                         String statusdir, String completedUrl,
                                         boolean enablelog, Boolean enableJobReconnect)
    throws URISyntaxException, IOException,
    InterruptedException
  {
    ArrayList<String> args = new ArrayList<String>();

    ArrayList<String> allFiles = new ArrayList<String>();
    if (TempletonUtils.isset(srcFile)) {
      allFiles.add(TempletonUtils.hadoopFsFilename(srcFile, appConf,
          runAs));
    }

    if (TempletonUtils.isset(otherFiles)) {
      String[] ofs = TempletonUtils.hadoopFsListAsArray(otherFiles, appConf, runAs);
      allFiles.addAll(Arrays.asList(ofs));
    }

    args.addAll(makeLauncherArgs(appConf, statusdir, completedUrl, allFiles,
                enablelog, enableJobReconnect, JobType.HIVE));

    if (appConf.hiveArchive() != null && !appConf.hiveArchive().equals(""))
    {
      args.add(ARCHIVES);
      args.add(appConf.hiveArchive());
    }

    //ship additional artifacts, for example for Tez
    String extras = appConf.get(AppConfig.HIVE_EXTRA_FILES); 
    if(extras != null && extras.length() > 0) {
      boolean foundFiles = false;
      for(int i = 0; i < args.size(); i++) {
        if(FILES.equals(args.get(i))) {
          String value = args.get(i + 1);
          args.set(i + 1, value + "," + extras);
          foundFiles = true;
        }
      }
      if(!foundFiles) {
        args.add(FILES);
        args.add(extras);
      }
      String[] extraFiles = appConf.getStrings(AppConfig.HIVE_EXTRA_FILES);
      StringBuilder extraFileNames = new StringBuilder();
      //now tell LaunchMapper which files it should add to HADOOP_CLASSPATH
      for(String file : extraFiles) {
        Path p = new Path(file);
        extraFileNames.append(p.getName()).append(",");
      }
      addDef(args, JobSubmissionConstants.HADOOP_CLASSPATH_EXTRAS, extraFileNames.toString());
    }
    return args;
  }
}
