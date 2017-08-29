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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hive.hcatalog.templeton.tool.JobSubmissionConstants;
import org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;

/**
 * Submit a Pig job.
 *
 * This is the backend of the pig web service.
 */
public class PigDelegator extends LauncherDelegator {
  private static final Logger LOG = LoggerFactory.getLogger(PigDelegator.class);
  public PigDelegator(AppConfig appConf) {
    super(appConf);
  }

  public EnqueueBean run(String user, Map<String, Object> userArgs,
               String execute, String srcFile,
               List<String> pigArgs, String otherFiles,
               String statusdir, String callback, 
               boolean usesHcatalog, String completedUrl, boolean enablelog,
               Boolean enableJobReconnect)
    throws NotAuthorizedException, BadParam, BusyException, QueueException,
    ExecuteException, IOException, InterruptedException, TooManyRequestsException {
    runAs = user;
    List<String> args = makeArgs(execute,
      srcFile, pigArgs,
      otherFiles, statusdir, usesHcatalog, completedUrl, enablelog, enableJobReconnect);

    return enqueueController(user, userArgs, callback, args);
  }

  /**
   * @param execute pig query string to be executed
   * @param srcFile pig query file to be executed
   * @param pigArgs pig command line arguments
   * @param otherFiles  files to be copied to the map reduce cluster
   * @param statusdir status dir location
   * @param usesHcatalog whether the command uses hcatalog/needs to connect
   *         to hive metastore server
   * @param completedUrl call back url
   * @param enablelog
   * @param enableJobReconnect
   * @return list of arguments
   * @throws BadParam
   * @throws IOException
   * @throws InterruptedException
   */
  private List<String> makeArgs(String execute, String srcFile,
                  List<String> pigArgs, String otherFiles,
                  String statusdir, boolean usesHcatalog,
                  String completedUrl, boolean enablelog,
                  Boolean enableJobReconnect)
    throws BadParam, IOException, InterruptedException {
    ArrayList<String> args = new ArrayList<String>();
    //check if the REST command specified explicitly to use hcatalog
    // or if it says that implicitly using the pig -useHCatalog arg
    boolean needsMetastoreAccess = usesHcatalog || hasPigArgUseHcat(pigArgs);
    
    try {
      ArrayList<String> allFiles = new ArrayList<String>();
      if (TempletonUtils.isset(srcFile)) {
        allFiles.add(TempletonUtils.hadoopFsFilename(srcFile, appConf, runAs));
      }
      if (TempletonUtils.isset(otherFiles)) {
        String[] ofs = TempletonUtils.hadoopFsListAsArray(otherFiles, appConf, runAs);
        allFiles.addAll(Arrays.asList(ofs));
      }

      args.addAll(makeLauncherArgs(appConf, statusdir, completedUrl, allFiles, enablelog,
          enableJobReconnect, JobType.PIG));
      boolean shipPigTar = appConf.pigArchive() != null && !appConf.pigArchive().equals("");
      boolean shipHiveTar = needsMetastoreAccess && appConf.hiveArchive() != null 
              && !appConf.hiveArchive().equals("");
      if(shipPigTar || shipHiveTar) {
        args.add(ARCHIVES);
        StringBuilder archives = new StringBuilder();
        if(shipPigTar) {
          archives.append(appConf.pigArchive());
        }
        if(shipPigTar && shipHiveTar) {
          archives.append(",");
        }
        if(shipHiveTar) {
          archives.append(appConf.hiveArchive());
        }
        args.add(archives.toString());
      }
      if(shipHiveTar) {
        addDef(args, JobSubmissionConstants.PigConstants.HIVE_HOME,
                appConf.get(AppConfig.HIVE_HOME_PATH));
        addDef(args, JobSubmissionConstants.PigConstants.HCAT_HOME,  
                appConf.get(AppConfig.HCAT_HOME_PATH));
        //Pig which uses HCat will pass this to HCat so that it can find the metastore
        addDef(args, JobSubmissionConstants.PigConstants.PIG_OPTS, 
                appConf.get(AppConfig.HIVE_PROPS_NAME));
      }
      args.add("--");
      args.add(appConf.pigPath());
      //the token file location should be first argument of pig
      args.add("-D" + TempletonControllerJob.TOKEN_FILE_ARG_PLACEHOLDER);

      //add mapreduce job tag placeholder
      args.add("-D" + TempletonControllerJob.MAPREDUCE_JOB_TAGS_ARG_PLACEHOLDER);

      for (String pigArg : pigArgs) {
        args.add(pigArg);
      }
      if(needsMetastoreAccess) {
        addHiveMetaStoreTokenArg();
      }
      
      if (TempletonUtils.isset(execute)) {
        args.add("-execute");
        args.add(execute);
      } else if (TempletonUtils.isset(srcFile)) {
        args.add("-file");
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

  /**
   * Check if the pig arguments has -useHCatalog set
   * see http://hive.apache.org/docs/hcat_r0.5.0/loadstore.pdf
   */
  private boolean hasPigArgUseHcat(List<String> pigArgs) {
    return pigArgs.contains("-useHCatalog");
  }
}
