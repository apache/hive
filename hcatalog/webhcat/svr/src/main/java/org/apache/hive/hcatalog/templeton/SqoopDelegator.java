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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.templeton.tool.JobSubmissionConstants;
import org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;

/**
 * Submit a Sqoop job.
 *
 * This is the backend of the Sqoop web service.
 */
public class SqoopDelegator extends LauncherDelegator {
  private static final Log LOG = LogFactory.getLog(SqoopDelegator.class);

  public SqoopDelegator(AppConfig appConf) {
    super(appConf);
  }

  public EnqueueBean run(String user,
               Map<String, Object> userArgs, String command, 
               String optionsFile, String otherFiles, String statusdir, 
               String callback, String completedUrl, boolean enablelog, String libdir)
  throws NotAuthorizedException, BadParam, BusyException, QueueException,
  IOException, InterruptedException
  {
    if(TempletonUtils.isset(appConf.sqoopArchive())) {
      if(!TempletonUtils.isset(appConf.sqoopPath()) && !TempletonUtils.isset(appConf.sqoopHome())) {
        throw new IllegalStateException("If '" + AppConfig.SQOOP_ARCHIVE_NAME + "' is defined, '" +
        AppConfig.SQOOP_PATH_NAME + "' and '" + AppConfig.SQOOP_HOME_PATH + "' must be defined");
      }
    }
    runAs = user;
    List<String> args = makeArgs(command, optionsFile, otherFiles, statusdir,
                   completedUrl, enablelog, libdir);

    return enqueueController(user, userArgs, callback, args);
  }
  private List<String> makeArgs(String command, String optionsFile, String otherFiles,
            String statusdir, String completedUrl, boolean enablelog, String libdir)
    throws BadParam, IOException, InterruptedException
  {
    ArrayList<String> args = new ArrayList<String>();
    try {
      args.addAll(makeBasicArgs(optionsFile, otherFiles, statusdir, completedUrl, enablelog, libdir));
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
          if(i == 0 && TempletonUtils.isset(libdir) && TempletonUtils.isset(appConf.sqoopArchive())) {
            //http://sqoop.apache.org/docs/1.4.5/SqoopUserGuide.html#_using_generic_and_specific_arguments
            String libJars = null;
            for(String s : args) {
              if(s.startsWith(JobSubmissionConstants.Sqoop.LIB_JARS)) {
                libJars = s.substring(s.indexOf("=") + 1);
                break;
              }
            }
            //the jars in libJars will be localized to CWD of the launcher task; then -libjars will
            //cause them to be localized for the Sqoop MR job tasks
            args.add(TempletonUtils.quoteForWindows("-libjars"));
            args.add(TempletonUtils.quoteForWindows(libJars));
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
            String statusdir, String completedUrl, boolean enablelog, String libdir)
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
    if(TempletonUtils.isset(libdir) && TempletonUtils.isset(appConf.sqoopArchive())) {
      /**Sqoop accesses databases via JDBC.  This means it needs to have appropriate JDBC
      drivers available.  Normally, the user would install Sqoop and place these jars
      into SQOOP_HOME/lib.  When WebHCat is configured to auto-ship the Sqoop tar file, we
      need to make sure that relevant JDBC jars are available on target node but we cannot modify
      lib/ of exploded tar because Dist Cache intentionally prevents this.
      The user is expected to place any JDBC jars into an HDFS directory and specify this
      dir in "libdir" parameter.  WebHCat then ensures that these jars are localized for the launcher task
      and made available to Sqoop.
      {@link org.apache.hive.hcatalog.templeton.tool.LaunchMapper#handleSqoop(org.apache.hadoop.conf.Configuration, java.util.Map)}
      {@link #makeArgs(String, String, String, String, String, boolean, String)}
      */
      LOG.debug("libdir=" + libdir);
      List<Path> jarList = TempletonUtils.hadoopFsListChildren(libdir, appConf, runAs);
      if(TempletonUtils.isset(jarList)) {
        StringBuilder sb = new StringBuilder();
        for(Path jar : jarList) {
          allFiles.add(jar.toString());
          sb.append(jar.getName()).append(',');
        }
        sb.setLength(sb.length() - 1);
        //we use the same mechanism to copy "files"/"otherFiles" and "libdir", but we only want to put
        //contents of "libdir" in Sqoop/lib, thus we pass the list of names here
        addDef(args, JobSubmissionConstants.Sqoop.LIB_JARS, sb.toString());
        addDef(args, AppConfig.SQOOP_HOME_PATH, appConf.get(AppConfig.SQOOP_HOME_PATH));
      }
    }
    args.addAll(makeLauncherArgs(appConf, statusdir, completedUrl, allFiles,
                enablelog, JobType.SQOOP));
    if(TempletonUtils.isset(appConf.sqoopArchive())) {
      args.add("-archives");
      args.add(appConf.sqoopArchive());
    }
    return args;
  }
}
