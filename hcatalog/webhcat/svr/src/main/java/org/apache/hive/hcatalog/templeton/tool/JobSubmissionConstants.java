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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

public interface JobSubmissionConstants {
  public static final String COPY_NAME = "templeton.copy";
  public static final String STATUSDIR_NAME = "templeton.statusdir";
  public static final String ENABLE_LOG = "templeton.enablelog";
  public static final String ENABLE_JOB_RECONNECT = "templeton.enablejobreconnect";
  public static final String JOB_TYPE = "templeton.jobtype";
  public static final String JAR_ARGS_NAME = "templeton.args";
  public static final String TEMPLETON_JOB_LAUNCH_TIME_NAME = "templeton.job.launch.time";
  public static final String OVERRIDE_CLASSPATH = "templeton.override-classpath";
  public static final String STDOUT_FNAME = "stdout";
  public static final String STDERR_FNAME = "stderr";
  public static final String EXIT_FNAME = "exit";
  public static final int WATCHER_TIMEOUT_SECS = 10;
  public static final int KEEP_ALIVE_MSEC = 60 * 1000;
  public static final int POLL_JOBPROGRESS_MSEC = 30 * 1000;
  /**
   * A comma-separated list of files to be added to HADOOP_CLASSPATH in 
   * {@link org.apache.hive.hcatalog.templeton.tool.LaunchMapper}.  Used to localize additional
   * artifacts for job submission requests.
   */
  public static final String HADOOP_CLASSPATH_EXTRAS = "templeton.hadoop.classpath.extras";
  /*
   * The = sign in the string for TOKEN_FILE_ARG_PLACEHOLDER is required because
   * org.apache.hadoop.util.GenericOptionsParser.preProcessForWindows() prepares
   * arguments expecting an = sign. It will fail to prepare the arguments correctly
   * without the = sign present.
   */
  public static final String TOKEN_FILE_ARG_PLACEHOLDER =
    "__MR_JOB_CREDENTIALS_OPTION=WEBHCAT_TOKEN_FILE_LOCATION__";
  public static final String TOKEN_FILE_ARG_PLACEHOLDER_TEZ =
    "__TEZ_CREDENTIALS_OPTION=WEBHCAT_TOKEN_FILE_LOCATION_TEZ__";
  // MRv2 job tag used to identify Templeton launcher child jobs. Each child job
  // will be tagged with the parent jobid so that on launcher task restart, all
  // previously running child jobs can be killed before the child job is launched
  // again.
  public static final String MAPREDUCE_JOB_TAGS = "mapreduce.job.tags";
  public static final String MAPREDUCE_JOB_TAGS_ARG_PLACEHOLDER =
    "__MR_JOB_TAGS_OPTION=MR_JOB_TAGS_JOBID__";

  public static final String HADOOP_CLASSPATH = "HADOOP_CLASSPATH";
  /**
   * constants needed for Pig job submission
   * The string values here are what Pig expects to see in it's environment
   */
  public static interface PigConstants {
    public static final String HIVE_HOME = "HIVE_HOME";
    public static final String HCAT_HOME = "HCAT_HOME";
    public static final String PIG_OPTS = "PIG_OPTS";
  }
  public static interface Sqoop {
    /**
     * comma-separated list of jar names (short name) which are needed for Sqoop JDBC access
     */
    public static final String LIB_JARS = "templeton.sqoop.lib.jar";
  }
}
