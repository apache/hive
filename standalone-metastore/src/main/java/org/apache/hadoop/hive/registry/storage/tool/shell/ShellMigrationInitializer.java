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

package org.apache.hadoop.hive.registry.storage.tool.shell;

import org.apache.hadoop.hive.registry.storage.tool.sql.StorageProviderConfiguration;
import org.apache.hadoop.hive.registry.storage.tool.sql.StorageProviderConfigurationReader;
import org.apache.hadoop.hive.registry.storage.tool.sql.Utils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Map;

public class ShellMigrationInitializer {

    private static final String OPTION_SCRIPT_ROOT_PATH = "script-root";
    private static final String OPTION_CONFIG_FILE_PATH = "config";


    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(
                Option.builder("s")
                        .numberOfArgs(1)
                        .longOpt(OPTION_SCRIPT_ROOT_PATH)
                        .desc("Root directory of script path")
                        .build()
        );

        options.addOption(
                Option.builder("c")
                        .numberOfArgs(1)
                        .longOpt(OPTION_CONFIG_FILE_PATH)
                        .desc("Config file path")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .hasArg(false)
                        .longOpt(ShellMigrationOption.MIGRATE.toString())
                        .desc("Execute schema migration from last check point")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .hasArg(false)
                        .longOpt(ShellMigrationOption.INFO.toString())
                        .desc("Show the status of the schema migration compared to the target database")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .hasArg(false)
                        .longOpt(ShellMigrationOption.VALIDATE.toString())
                        .desc("Validate the target database changes with the migration scripts")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .hasArg(false)
                        .longOpt(ShellMigrationOption.REPAIR.toString())
                        .desc("Repairs the SCRIPT_CHANGE_LOG by removing failed migrations and correcting checksum of existing migration script")
                        .build()
        );

        CommandLineParser parser = new BasicParser();
        CommandLine commandLine = parser.parse(options, args);

        if (!commandLine.hasOption(OPTION_SCRIPT_ROOT_PATH)) {
            usage(options);
            System.exit(1);
        }

        boolean isShellMigrationOptionSpecified = false;
        ShellMigrationOption shellMigrationOptionSpecified = null;
        for (ShellMigrationOption shellMigrationOption : ShellMigrationOption.values()) {
            if (commandLine.hasOption(shellMigrationOption.toString())) {
                if (isShellMigrationOptionSpecified) {
                    System.out.println("Only one operation can be execute at once, please select one of ',migrate', 'validate', 'info', 'repair'.");
                    System.exit(1);
                }
                isShellMigrationOptionSpecified = true;
                shellMigrationOptionSpecified = shellMigrationOption;
            }
        }

        if (!isShellMigrationOptionSpecified) {
            System.out.println("One of the option 'migrate', 'validate', 'info', 'repair' must be specified to execute.");
            System.exit(1);
        }

        String scriptRootPath = commandLine.getOptionValue(OPTION_SCRIPT_ROOT_PATH);
        String confFilePath = commandLine.getOptionValue(OPTION_CONFIG_FILE_PATH);

        StorageProviderConfiguration storageProperties;
        try {
            Map<String, Object> conf = Utils.readConfig(confFilePath);

            StorageProviderConfigurationReader confReader = new StorageProviderConfigurationReader();
            storageProperties = confReader.readStorageConfig(conf);
        } catch (IOException e) {
            System.err.println("Error occurred while reading config file: " + confFilePath);
            System.exit(1);
            throw new IllegalStateException("Shouldn't reach here");
        }


        ShellMigrationHelper schemaMigrationHelper = new ShellMigrationHelper(ShellFlywayFactory.get(storageProperties, scriptRootPath));
        try {
            schemaMigrationHelper.execute(shellMigrationOptionSpecified);
            System.out.println(String.format("\"%s\" option successful", shellMigrationOptionSpecified.toString()));
        } catch (Exception e) {
            System.err.println(String.format("\"%s\" option failed : %s", shellMigrationOptionSpecified.toString(), e.getMessage()));
            System.exit(1);
        }

    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ShellMigrationInitializer [options]", options);
    }
}
