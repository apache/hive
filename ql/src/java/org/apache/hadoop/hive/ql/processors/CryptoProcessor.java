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

package org.apache.hadoop.hive.ql.processors;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class processes HADOOP commands used for HDFS encryption. It is meant to be run
 * only by Hive unit & queries tests.
 */
public class CryptoProcessor implements CommandProcessor {
  public static final Logger LOG = LoggerFactory.getLogger(CryptoProcessor.class.getName());

  private HadoopShims.HdfsEncryptionShim encryptionShim;

  private Options CREATE_KEY_OPTIONS;
  private Options DELETE_KEY_OPTIONS;
  private Options CREATE_ZONE_OPTIONS;

  private int DEFAULT_BIT_LENGTH = 128;

  private HiveConf conf;

  public CryptoProcessor(HadoopShims.HdfsEncryptionShim encryptionShim, HiveConf conf) {
    this.encryptionShim = encryptionShim;
    this.conf = conf;

    CREATE_KEY_OPTIONS = new Options();
    CREATE_KEY_OPTIONS.addOption(OptionBuilder.hasArg().withLongOpt("keyName").isRequired().create());
    CREATE_KEY_OPTIONS.addOption(OptionBuilder.hasArg().withLongOpt("bitLength").create());   // optional

    DELETE_KEY_OPTIONS = new Options();
    DELETE_KEY_OPTIONS.addOption(OptionBuilder.hasArg().withLongOpt("keyName").isRequired().create());

    CREATE_ZONE_OPTIONS = new Options();
    CREATE_ZONE_OPTIONS.addOption(OptionBuilder.hasArg().withLongOpt("keyName").isRequired().create());
    CREATE_ZONE_OPTIONS.addOption(OptionBuilder.hasArg().withLongOpt("path").isRequired().create());
  }

  private CommandLine parseCommandArgs(final Options opts, String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    return parser.parse(opts, args);
  }

  private CommandProcessorResponse returnErrorResponse(final String errmsg) {
    return new CommandProcessorResponse(1, "Encryption Processor Helper Failed:" + errmsg, null);
  }

  private void writeTestOutput(final String msg) {
    SessionState.get().out.println(msg);
  }

  @Override
  public CommandProcessorResponse run(String command) {
    String[] args = command.split("\\s+");

    if (args.length < 1) {
      return returnErrorResponse("Command arguments are empty.");
    }

    if (encryptionShim == null) {
      return returnErrorResponse("Hadoop encryption shim is not initialized.");
    }

    String action = args[0];
    String params[] = Arrays.copyOfRange(args, 1, args.length);

    try {
      if (action.equalsIgnoreCase("create_key")) {
        createEncryptionKey(params);
      } else if (action.equalsIgnoreCase("create_zone")) {
        createEncryptionZone(params);
      } else if (action.equalsIgnoreCase("delete_key")) {
        deleteEncryptionKey(params);
      } else {
        return returnErrorResponse("Unknown command action: " + action);
      }
    } catch (Exception e) {
      return returnErrorResponse(e.getMessage());
    }

    return new CommandProcessorResponse(0);
  }

  /**
   * Creates an encryption key using the parameters passed through the 'create_key' action.
   *
   * @param params Parameters passed to the 'create_key' command action.
   * @throws Exception If key creation failed.
   */
  private void createEncryptionKey(String[] params) throws Exception {
    CommandLine args = parseCommandArgs(CREATE_KEY_OPTIONS, params);

    String keyName = args.getOptionValue("keyName");
    String bitLength = args.getOptionValue("bitLength", Integer.toString(DEFAULT_BIT_LENGTH));

    try {
      encryptionShim.createKey(keyName, Integer.parseInt(bitLength));
    } catch (Exception e) {
      throw new Exception("Cannot create encryption key: " + e.getMessage());
    }

    writeTestOutput("Encryption key created: '" + keyName + "'");
  }

  /**
   * Creates an encryption zone using the parameters passed through the 'create_zone' action.
   *
   * @param params Parameters passed to the 'create_zone' command action.
   * @throws Exception If zone creation failed.
   */
  private void createEncryptionZone(String[] params) throws Exception {
    CommandLine args = parseCommandArgs(CREATE_ZONE_OPTIONS, params);

    String keyName = args.getOptionValue("keyName");
    String cryptoZoneStr = args.getOptionValue("path");
    if (cryptoZoneStr == null) {
      throw new Exception("Cannot create encryption zone: Invalid path 'null'");
    }
    Path cryptoZone = new Path(cryptoZoneStr);

    try {
      encryptionShim.createEncryptionZone(cryptoZone, keyName);
    } catch (IOException e) {
      throw new Exception("Cannot create encryption zone: " + e.getMessage());
    }

    writeTestOutput("Encryption zone created: '" + cryptoZone + "' using key: '" + keyName + "'");
  }

  /**
   * Deletes an encryption key using the parameters passed through the 'delete_key' action.
   *
   * @param params Parameters passed to the 'delete_key' command action.
   * @throws Exception If key deletion failed.
   */
  private void deleteEncryptionKey(String[] params) throws Exception {
    CommandLine args = parseCommandArgs(DELETE_KEY_OPTIONS, params);

    String keyName = args.getOptionValue("keyName");
    try {
      encryptionShim.deleteKey(keyName);
    } catch (IOException e) {
      throw new Exception("Cannot delete encryption key: " + e.getMessage());
    }

    writeTestOutput("Encryption key deleted: '" + keyName + "'");
  }

  @Override
  public void close() throws Exception {
  }
}
