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

import jline.internal.Log;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParserFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.hms.EmbeddedTaskProvider;
import org.apache.hadoop.hive.metastore.tools.schematool.liquibase.LiquibaseTaskProvider;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTaskFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.logging.LogManager;

public class MetastoreSchemaTool {

  private static final Logger LOG = LoggerFactory.getLogger(MetastoreSchemaTool.class);

  private final SchemaToolTaskFactory taskFactory;
  private SchemaToolCommandLine cmdLine;

  public MetastoreSchemaTool(SchemaToolTaskFactory taskFactory) {
    this.taskFactory = taskFactory;
  }

  public static void main(String[] args) {
    MetastoreSchemaTool tool = new MetastoreSchemaTool(
        new SchemaToolTaskFactory(
            new NestedScriptParserFactory(), new LiquibaseTaskProvider(new EmbeddedTaskProvider())
        )
    );
    System.exit(tool.runcommandLine(args, null));
  }

  protected int runcommandLine(String[] args, OptionGroup additionalOptions) {
    try {
      LOG.info("Running schematool with the following arguments: " + StringUtils.join(args, ','));
      run(findHomeDir(), args, additionalOptions, MetastoreConf.newMetastoreConf());
      return 0;
    } catch (HiveMetaException e) {
      Log.error(e.getMessage());
      if (e.getCause() != null) {
        Throwable t = e.getCause();
        LOG.error("Underlying cause: " + t.getClass().getName() + " : " + t.getMessage());
        if (t instanceof SQLException) {
          LOG.error("SQL Error code: " + ((SQLException) t).getErrorCode());
        }
      }
      if (cmdLine != null) {
        if (cmdLine.hasOption("verbose")) {
          e.printStackTrace();
        } else {
          LOG.info("Use --verbose for detailed stacktrace.");
        }
      }
      LOG.error("*** schemaTool failed ***");
      return 1;
    }
  }

  private void run(String metastoreHome, String[] args, OptionGroup additionalOptions, Configuration conf) throws HiveMetaException {
    if (StringUtils.isBlank(metastoreHome)) {
      throw new HiveMetaException("No Metastore home directory provided");
    }
    try {
      cmdLine = new SchemaToolCommandLine(args, additionalOptions);
    } catch (ParseException e) {
      System.err.println("Failed to parse command line. ");
      throw new HiveMetaException(e);
    }
    TaskContext context = new TaskContext(metastoreHome, conf, cmdLine);
    taskFactory.getTask(cmdLine).executeChain(context);
  }

  private String findHomeDir() {
    // If METASTORE_HOME is set, use it, else use HIVE_HOME for backwards compatibility.
    String homeDir = System.getenv("METASTORE_HOME");
    return StringUtils.isBlank(homeDir) ? System.getenv("HIVE_HOME") : homeDir;
  }

}
