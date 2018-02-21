/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hive.ptest.execution.conf.TestConfiguration;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

/**
 * Wrapper phase for Yetus check execution kick-off. It will invoke ./dev-support/test-patch.sh
 * with the proper arguments and run the check itself in a asynchronous fashion.
 */
public class YetusPhase extends Phase {

  private static final String YETUS_LOG_FILE = "yetus.txt";
  private static final String YETUS_OUTPUT_FOLDER = "yetus";
  private static final String YETUS_EXEC_SCRIPT = "yetus-exec.sh";
  private static final String YETUS_EXEC_VM = "yetus-exec.vm";

  private final File mPatchFile;
  private final File mWorkingDir;
  private final File mLogFile;
  private final File mOutputDir;
  private final File mScratchDir;
  private final String buildTag;
  private final String buildUrl;
  private final TestConfiguration conf;


  public YetusPhase(TestConfiguration configuration, List<HostExecutor> hostExecutors,
      LocalCommandFactory localCommandFactory, ImmutableMap<String, String> templateDefaults,
      String workingDir, File scratchDir, Logger logger, File logDir, File patchFile) {

    super(hostExecutors, localCommandFactory, templateDefaults, logger);
    this.mPatchFile = patchFile;
    this.buildTag = templateDefaults.get("buildTag");
    this.mWorkingDir = new File(workingDir, YETUS_OUTPUT_FOLDER + "_" + this.buildTag);
    this.mLogFile = new File(logDir, YETUS_LOG_FILE);
    this.mOutputDir = new File(logDir, YETUS_OUTPUT_FOLDER);
    this.mScratchDir = scratchDir;
    this.conf = configuration;
    this.buildUrl = conf.getLogsURL() + "/" + this.buildTag + "/";

  }

  /**
   * This method will start a new thread to handle the Yetus test patch script execution.
   * It creates a separate directory, instantiates the Yetus velocity template, runs it, and
   * cleans it up after.
   * @throws Exception
   */
  @Override
  public void execute() throws Exception {

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {

        if (!checkDependencies()) {
          return;
        }
        File yetusScriptDir = new File(mScratchDir, buildTag);
        yetusScriptDir.mkdir();
        File yetusExecScript = new File(yetusScriptDir, YETUS_EXEC_SCRIPT);
        Map<String, String> templateVars = new HashMap<>();
        templateVars.putAll(getTemplateDefaults());
        templateVars.put("workingDir", mWorkingDir.getAbsolutePath());
        templateVars.put("jiraName", conf.getJiraName());
        templateVars.put("patchFile", mPatchFile.getAbsolutePath());
        templateVars.put("jiraUrl", conf.getJiraUrl());
        templateVars.put("jiraUser", conf.getJiraUser());
        templateVars.put("jiraPass", conf.getJiraPassword());
        templateVars.put("outputDir", mOutputDir.getAbsolutePath());
        templateVars.put("buildUrl", buildUrl);
        templateVars.put("buildUrlLog", YETUS_LOG_FILE);
        templateVars.put("buildUrlOutputDir", YETUS_OUTPUT_FOLDER);
        templateVars.put("logFile", mLogFile.getAbsolutePath());

        try {
          logger.info("Writing {} from template", yetusExecScript);

          Templates.writeTemplateResult(YETUS_EXEC_VM, yetusExecScript, templateVars);
          Process proc = new ProcessBuilder().command("bash", yetusExecScript.getPath()).start();
          int exitCode = proc.waitFor();

          if (exitCode == 0) {
            logger.info("Finished processing Yetus check successfully");
          }
        } catch (Exception e) {
          logger.error("Error processing Yetus check", e);
        } finally {
          logger.debug("Deleting " + yetusExecScript + ": " + yetusExecScript.delete());
          logger.debug("Deleting " + yetusScriptDir + ": " + yetusScriptDir.delete());
        }
      }
    });
    t.start();
    logger.info("Started Yetus check..");
  }

  private boolean checkDependencies(){

    if (mPatchFile == null || !mPatchFile.canRead()) {
      logger.error("Cannot run Yetus check - patch file is null or not readable.");
      return false;
    }

    if (!((mWorkingDir.isDirectory() && mWorkingDir.canWrite()) &&
            (mOutputDir.isDirectory() && mOutputDir.canWrite()))) {
      logger.error("Cannot run Yetus check - output directories not present and writable: " +
        "workingDir:%s, outputDir:%s", mWorkingDir.getAbsolutePath(), mOutputDir.getAbsolutePath());
      return false;
    }

    if (Strings.isNullOrEmpty(conf.getJiraUrl()) ||
            Strings.isNullOrEmpty(conf.getJiraName()) ||
            Strings.isNullOrEmpty(conf.getJiraPassword()) ||
            Strings.isNullOrEmpty(conf.getJiraUser())) {
      logger.error("Cannot run Yetus check - credentials for Jira not provided.");
      return false;
    }

    return true;
  }
}
