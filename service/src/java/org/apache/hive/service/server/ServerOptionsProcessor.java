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

package org.apache.hive.service.server;

import java.util.Properties;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ServerOptionsProcessor.
 * Process arguments given to servers (-hiveconf property=value)
 * Set properties in System properties
 */
public class ServerOptionsProcessor {
  protected static final Log LOG = LogFactory.getLog(ServerOptionsProcessor.class.getName());
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;
  private final String serverName;
  private StringBuilder debugMessage = new StringBuilder();


  @SuppressWarnings("static-access")
  public ServerOptionsProcessor(String serverName) {
    this.serverName = serverName;
    // -hiveconf x=y
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Use value for given property")
        .create());

    options.addOption(new Option("H", "help", false, "Print help information"));

  }

  public boolean process(String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);
      if (commandLine.hasOption('H')) {
        printUsage();
        return false;
      }
      //get hiveconf param values and set the System property values
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        //save logging message for log4j output latter after log4j initialize properly
        debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n");
        System.setProperty(propKey, confProps.getProperty(propKey));
      }
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printUsage();
      return false;
    }
    return true;
  }

  public StringBuilder getDebugMessage() {
    return debugMessage;
  }

  private void printUsage() {
    new HelpFormatter().printHelp(serverName, options);
  }

}
