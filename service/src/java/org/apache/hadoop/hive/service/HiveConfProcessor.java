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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.service;

import java.util.Properties;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HiveConfProcessor {
  protected static final Log l4j = LogFactory.getLog(HiveConfProcessor.class.getName());
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;

  @SuppressWarnings("static-access")
  public HiveConfProcessor() {

    //-hiveconf x=y
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Use value for given property")
        .create());
  }

  public void process(String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);

      //HiveConf will read these from the System.Properties
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        System.setProperty(propKey, confProps.getProperty(propKey));
      }

    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printUsage();
      System.exit(1);
    }
  }

  private void printUsage() {
    new HelpFormatter().printHelp("hive --service hiveserver", options);
  }

}
