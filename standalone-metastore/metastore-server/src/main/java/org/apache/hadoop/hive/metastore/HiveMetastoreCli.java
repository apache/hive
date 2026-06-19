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

package org.apache.hadoop.hive.metastore;

import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.CommonCliOptions;

/**
 * HiveMetaStore specific CLI
 *
 */
class HiveMetastoreCli extends CommonCliOptions {
  private int port;

  @SuppressWarnings("static-access")
  HiveMetastoreCli(Configuration configuration) {
    super("hivemetastore", true);
    this.port = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.SERVER_PORT);

    // -p port
    OPTIONS.addOption(OptionBuilder
        .hasArg()
        .withArgName("port")
        .withDescription("Hive Metastore port number, default:"
            + this.port)
        .create('p'));

  }
  @Override
  public void parse(String[] args) {
    super.parse(args);

    // support the old syntax "hivemetastore [port]" but complain
    args = commandLine.getArgs();
    if (args.length > 0) {
      // complain about the deprecated syntax -- but still run
      System.err.println(
          "This usage has been deprecated, consider using the new command "
              + "line syntax (run with -h to see usage information)");

      this.port = Integer.parseInt(args[0]);
    }

    // notice that command line options take precedence over the
    // deprecated (old style) naked args...

    if (commandLine.hasOption('p')) {
      this.port = Integer.parseInt(commandLine.getOptionValue('p'));
    } else {
      // legacy handling
      String metastorePort = System.getenv("METASTORE_PORT");
      if (metastorePort != null) {
        this.port = Integer.parseInt(metastorePort);
      }
    }
  }

  public int getPort() {
    return this.port;
  }
}
