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
package org.apache.hive.hcatalog.templeton.tool;

public interface JobSubmissionConstants {
  public static final String COPY_NAME = "templeton.copy";
  public static final String STATUSDIR_NAME = "templeton.statusdir";
  public static final String ENABLE_LOG = "templeton.enablelog";
  public static final String JOB_TYPE = "templeton.jobtype";
  public static final String JAR_ARGS_NAME = "templeton.args";
  public static final String OVERRIDE_CLASSPATH = "templeton.override-classpath";
  public static final String OVERRIDE_CONTAINER_LOG4J_PROPS = "override.containerLog4j";
  //name of file
  static final String CONTAINER_LOG4J_PROPS = "override-container-log4j.properties";
  public static final String STDOUT_FNAME = "stdout";
  public static final String STDERR_FNAME = "stderr";
  public static final String EXIT_FNAME = "exit";
  public static final int WATCHER_TIMEOUT_SECS = 10;
  public static final int KEEP_ALIVE_MSEC = 60 * 1000;
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
  /**
   * constants needed for Pig job submission
   */
  public static interface PigConstants {
    public static final String HIVE_HOME = "HIVE_HOME";
    public static final String HCAT_HOME = "HCAT_HOME";
    public static final String PIG_OPTS = "PIG_OPTS";
  }
}
