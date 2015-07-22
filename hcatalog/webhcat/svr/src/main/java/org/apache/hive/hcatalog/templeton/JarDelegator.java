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
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.ExecuteException;
import org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;

/**
 * Submit a job to the MapReduce queue.
 *
 * This is the backend of the mapreduce/jar web service.
 */
public class JarDelegator extends LauncherDelegator {
  public JarDelegator(AppConfig appConf) {
    super(appConf);
  }

  public EnqueueBean run(String user, Map<String, Object> userArgs, String jar, String mainClass,
               String libjars, String files,
               List<String> jarArgs, List<String> defines,
               String statusdir, String callback, 
               boolean usesHcatalog, String completedUrl,
               boolean enablelog, Boolean enableJobReconnect, JobType jobType)
    throws NotAuthorizedException, BadParam, BusyException, QueueException,
    ExecuteException, IOException, InterruptedException {
    runAs = user;
    List<String> args = makeArgs(jar, mainClass,
      libjars, files, jarArgs, defines,
      statusdir, usesHcatalog, completedUrl, enablelog, enableJobReconnect, jobType);

    return enqueueController(user, userArgs, callback, args);
  }

  private List<String> makeArgs(String jar, String mainClass,
                  String libjars, String files,
                  List<String> jarArgs, List<String> defines,
                  String statusdir, boolean usesHcatalog, String completedUrl,
                  boolean enablelog, Boolean enableJobReconnect, JobType jobType)
    throws BadParam, IOException, InterruptedException {
    ArrayList<String> args = new ArrayList<String>();
    try {
      ArrayList<String> allFiles = new ArrayList<String>();
      allFiles.add(TempletonUtils.hadoopFsFilename(jar, appConf, runAs));

      args.addAll(makeLauncherArgs(appConf, statusdir,
        completedUrl, allFiles, enablelog, enableJobReconnect, jobType));
      args.add("--");
      TempletonUtils.addCmdForWindows(args);

      //check if the rest command specified explicitly to use hcatalog
      if(usesHcatalog){
        addHiveMetaStoreTokenArg();
      }

      args.add(appConf.clusterHadoop());
      args.add("jar");
      args.add(TempletonUtils.hadoopFsPath(jar, appConf, runAs).getName());
      if (TempletonUtils.isset(mainClass)) {
        args.add(mainClass);
      }
      if (TempletonUtils.isset(libjars)) {
        String libjarsListAsString =
            TempletonUtils.hadoopFsListAsString(libjars, appConf, runAs);
        //This will work only if the files are local files on webhcat server
        // (which is not very useful since users might not have access to that file system).
        //This is likely the HIVE-5188 issue
        args.add("-libjars");
        args.add(TempletonUtils.quoteForWindows(libjarsListAsString));
      }
      if (TempletonUtils.isset(files)) {
        String filesListAsString =
            TempletonUtils.hadoopFsListAsString(files, appConf, runAs);
        args.add("-files");
        args.add(TempletonUtils.quoteForWindows(filesListAsString));
      }
      //the token file location comes after mainClass, as a -D prop=val
      args.add("-D");
      args.add(TempletonControllerJob.TOKEN_FILE_ARG_PLACEHOLDER);

      //add mapreduce job tag placeholder
      args.add("-D");
      args.add(TempletonControllerJob.MAPREDUCE_JOB_TAGS_ARG_PLACEHOLDER);

      for (String d : defines) {
        args.add("-D");
        args.add(TempletonUtils.quoteForWindows(d));
      }
      for (String arg : jarArgs) {
        args.add(TempletonUtils.quoteForWindows(arg));
      }
    } catch (FileNotFoundException e) {
      throw new BadParam(e.getMessage());
    } catch (URISyntaxException e) {
      throw new BadParam(e.getMessage());
    }

    return args;
  }
}
