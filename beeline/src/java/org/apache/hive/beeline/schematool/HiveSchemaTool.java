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
package org.apache.hive.beeline.schematool;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParserFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.hms.EmbeddedTaskProvider;
import org.apache.hadoop.hive.metastore.tools.schematool.liquibase.LiquibaseTaskProvider;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTaskFactory;
import org.apache.hive.beeline.schematool.tasks.HiveTaskProvider;

public class HiveSchemaTool extends MetastoreSchemaTool {

  public static final String VERSION_SCRIPT = "CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '%s' " +
      "AS `SCHEMA_VERSION`, 'Hive release version %s' AS `VERSION_COMMENT`";
  
  public HiveSchemaTool(SchemaToolTaskFactory taskFactory) {
    super(taskFactory);
  }

  public static void main(String[] args) {
    EmbeddedTaskProvider etp = new EmbeddedTaskProvider();
    HiveSchemaTool tool = new HiveSchemaTool(new SchemaToolTaskFactory(
        new NestedScriptParserFactory(), new HiveTaskProvider(etp), new LiquibaseTaskProvider(etp)));
    OptionGroup additionalGroup = new OptionGroup();
    Option metaDbTypeOpt = Option.builder("metaDbType")
        .argName("metaDatabaseType")
        .hasArgs()
        .desc("Used only if upgrading the system catalog for hive")
        .build();
    additionalGroup.addOption(metaDbTypeOpt);
    System.setProperty(MetastoreConf.ConfVars.SCHEMA_VERIFICATION.getVarname(), "true");
    System.exit(tool.runcommandLine(args, additionalGroup));
  }

}
