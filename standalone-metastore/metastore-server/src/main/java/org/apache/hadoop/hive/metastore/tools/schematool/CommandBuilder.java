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
package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * This class is responsible to build the command required to run scripts with either {@link sqlline.SqlLine} or
 * Beeline.
 * Note: I was not able to make {@link sqlline.SqlLine} work with Streams, so all scripts must be persisted into files
 * and the path must be provided.
 */
public class CommandBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CommandBuilder.class);

  private static final String PASSWD_MASK = "[passwd stripped]";

  private final HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo;
  private final String[] additionalArgs;
  private boolean verbose = false;

  public CommandBuilder(HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo, String[] additionalArgs) {
    this.connectionInfo = connectionInfo;
    this.additionalArgs = additionalArgs;
  }

  public CommandBuilder setVerbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  public HiveSchemaHelper.MetaStoreConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public String[] buildToRun(String sqlScriptFile) {
    return argsWith(connectionInfo.getPassword(), sqlScriptFile);
  }

  public String buildToLog(String sqlScriptFile) throws IOException {
    if (verbose) {
      logScript(sqlScriptFile);
    }
    return StringUtils.join(argsWith(PASSWD_MASK, sqlScriptFile), " ");
  }

  protected String[] argsWith(String password, String sqlScriptFile) {
    String[] args = new String[]
    {
        "-u", connectionInfo.getUrl(),
        "-d", connectionInfo.getDriver(),
        "-n", connectionInfo.getUsername(),
        "-p", password,
        "-f", sqlScriptFile
    };
    if (additionalArgs == null || additionalArgs.length == 0) {
      return args;
    }
    String[] result = Arrays.copyOf(args, args.length + additionalArgs.length);
    System.arraycopy(additionalArgs, 0, result, args.length, additionalArgs.length);
    return result;
  }

  private void logScript(String sqlScriptFile) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to invoke file that contains:");
      try (BufferedReader reader = new BufferedReader(new FileReader(sqlScriptFile))) {
        String line;
        while ((line = reader.readLine()) != null) {
          LOG.debug("script: " + line);
        }
      }
    }
  }
}
