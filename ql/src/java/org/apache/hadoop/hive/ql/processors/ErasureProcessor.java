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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileErasureCodingPolicy;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class processes hadoop commands used for Erasure Coding.
 * It is meant to be run only by Hive unit tests.
 *
 * The ‘Erasure’ commands implemented by this class allow test writers to use Erasure Coding in Hive.
 * Hdfs determines whether to use Erasure Coding for a file based on the presence of an Erasure
 * Coding Policy on the directory which contains the file.
 * These ‘Erasure’ commands can be used to manipulate Erasure Coding Policies.
 * These commands are similar to the user level commands provided by the ‘hdfs ec’ command as
 * documented at:
 * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSErasureCoding.html
 *
 * <ul>
 * <li>getPolicy:     Get an erasure coding policy for a Path.
 * <li>enablePolicy:  Enable an erasure coding policy.
 * <li>removePolicy:  Remove an erasure coding policy.
 * <li>disablePolicy: Disable an erasure coding policy.
 * <li>setPolicy:     Sets an erasure coding policy on a directory at the specified path
 * <li>unsetPolicy:   Unsets an erasure coding policy on a directory at the specified path
 * <li>echo:          Echo the parameters given to the command (not an ec command)
 * </ul>
 */
public class ErasureProcessor implements CommandProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ErasureProcessor.class.getName());

  private HadoopShims.HdfsErasureCodingShim erasureCodingShim;

  ErasureProcessor(HiveConf config) throws IOException {
    this.erasureCodingShim  = getErasureShim(config);
  }

  /**
   * Get an instance of HdfsErasureCodingShim from a config.
   */
  public static HadoopShims.HdfsErasureCodingShim getErasureShim(Configuration config) throws IOException {
    HadoopShims hadoopShims = ShimLoader.getHadoopShims();
    FileSystem fileSystem = FileSystem.get(config);
    return hadoopShims.createHdfsErasureCodingShim(fileSystem, config);
  }

  private CommandLine parseCommandArgs(final Options opts, String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    return parser.parse(opts, args);
  }

  private void writeTestOutput(final String msg) {
    SessionState.get().out.println(msg);
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    String[] args = command.split("\\s+");

    if (args.length < 1) {
      throw new CommandProcessorException("Erasure Processor Helper Failed: Command arguments are empty.");
    }

    if (erasureCodingShim == null) {
      throw new CommandProcessorException("Erasure Processor Helper Failed: Hadoop erasure shim is not initialized.");
    }

    String action = args[0].toLowerCase();
    String[] params = Arrays.copyOfRange(args, 1, args.length);

    try {
      switch (action) {
      // note we switch on the lowercase command name
      case "disablepolicy":
        disablePolicy(params);
        break;
      case "echo":
        echo(params);
        break;
      case "enablepolicy":
        enablePolicy(params);
        break;
      case "getpolicy":
        getPolicy(params);
        break;
      case "listpolicies":
        listPolicies();
        break;
      case "setpolicy":
        setPolicy(params);
        break;
      case "removepolicy":
        removePolicy(params);
        break;
      case "unsetpolicy":
        unsetPolicy(params);
        break;
      default:
        throw new CommandProcessorException(
            "Erasure Processor Helper Failed: Unknown erasure command action: " + action);
      }
    } catch (Exception e) {
      throw new CommandProcessorException("Erasure Processor Helper Failed: " + e.getMessage());
    }

    return new CommandProcessorResponse();
  }

  /**
   * Get an erasure coding policy for a Path.
   * @param params Parameters passed to the command.
   * @throws Exception if command failed.
   */
  private void getPolicy(String[] params) throws Exception {
    String command = "getPolicy";
    try {
      // getPolicy -path <path>
      Options getPolicyOptions = new Options();

      String pathOptionName = "path";
      Option policyOption = OptionBuilder.hasArg()
          .isRequired()
          .withLongOpt(pathOptionName)
          .withDescription("Path for which Policy should be fetched")
          .create();
      getPolicyOptions.addOption(policyOption);

      CommandLine args = parseCommandArgs(getPolicyOptions, params);
      String path = args.getOptionValue(pathOptionName);

      HdfsFileErasureCodingPolicy policy = erasureCodingShim.getErasureCodingPolicy(new Path(path));
      writeTestOutput("EC policy is '" + (policy != null ? policy.getName() : "REPLICATED") + "'");

    } catch (ParseException pe) {
      writeTestOutput("Error parsing options for " + command + " " + pe.getMessage());
    } catch (Exception e) {
      writeTestOutput("Caught exception running " + command + ": " + e.getMessage());
      throw new Exception("Cannot run " + command + ": " + e.getMessage(), e);
    }
  }

  /**
   * Echo the parameters given to the command.
   * @param params parameters which will be echoed
   */
  private void echo(String[] params) throws Exception {
    String command = "echo";
    try {
      writeTestOutput("ECHO " + Joiner.on(" ").join(params));
    } catch (Exception e) {
      writeTestOutput("Caught exception running " + command + ": " + e.getMessage());
      throw new Exception("Cannot run " + command + ": " + e.getMessage());
    }
  }

  /**
   * Enable an erasure coding policy.
   * @param params Parameters passed to the command.
   * @throws Exception If command failed.
   */
  private void enablePolicy(String[] params) throws Exception {
    String command = "enablePolicy";
    try {
      // enablePolicy -policy <policyName>
      Options enablePolicyOptions = new Options();

      String policyOptionName = "policy";
      Option policyOption = OptionBuilder.hasArg()
          .isRequired()
          .withLongOpt(policyOptionName)
          .withDescription("Policy to enable")
          .hasArg()
          .create();
      enablePolicyOptions.addOption(policyOption);

      CommandLine args = parseCommandArgs(enablePolicyOptions, params);
      String policyName = args.getOptionValue(policyOptionName);

      erasureCodingShim.enableErasureCodingPolicy(policyName);
      writeTestOutput("Enabled EC policy '" + policyName + "'");
    } catch (ParseException pe) {
      writeTestOutput("Error parsing options for " + command + " " + pe.getMessage());
    } catch (Exception e) {
      writeTestOutput("Caught exception running " + command + ": " + e.getMessage());
      throw new Exception("Cannot run " + command + ": " + e.getMessage());
    }
  }

  /**
   * Remove an erasure coding policy.
   * @param params Parameters passed to the command.
   * @throws Exception if command failed.
   */
  private void removePolicy(String[] params) throws Exception {
    String command = "removePolicy";
    try {
      // removePolicy -policy <policyName>
      Options removePolicyOptions = new Options();

      String policyOptionName = "policy";
      Option policyOption = OptionBuilder.hasArg()
          .isRequired()
          .withLongOpt(policyOptionName)
          .withDescription("Policy to remove")
          .create();
      removePolicyOptions.addOption(policyOption);

      CommandLine args = parseCommandArgs(removePolicyOptions, params);
      String policyName = args.getOptionValue(policyOptionName);

      erasureCodingShim.removeErasureCodingPolicy(policyName);
      writeTestOutput("Removed EC policy '" + policyName + "'");
    } catch (ParseException pe) {
      writeTestOutput("Error parsing options for " + command + " " + pe.getMessage());
    } catch (Exception e) {
      writeTestOutput("Caught exception running " + command + ": " + e.getMessage());
      throw new Exception("Cannot run " + command + ": " + e.getMessage());
    }
  }

  /**
   * Disable an erasure coding policy.
   * @param params Parameters passed to the command.
   * @throws Exception If command failed.
   */
  private void disablePolicy(String[] params) throws Exception {
    String command = "disablePolicy";
    try {
      // disablePolicy -policy <policyName>
      Options disablePolicyOptions = new Options();

      String policyOptionName = "policy";
      Option policyOption = OptionBuilder.hasArg()
          .isRequired()
          .withLongOpt(policyOptionName)
          .withDescription("Policy to disable")
          .create();
      disablePolicyOptions.addOption(policyOption);

      CommandLine args = parseCommandArgs(disablePolicyOptions, params);
      String policyName = args.getOptionValue(policyOptionName);

      erasureCodingShim.disableErasureCodingPolicy(policyName);
      writeTestOutput("Disabled EC policy '" + policyName + "'");
    } catch (ParseException pe) {
      writeTestOutput("Error parsing options for " + command + " " + pe.getMessage());
    } catch (Exception e) {
      writeTestOutput("Caught exception running " + command + ": " + e.getMessage());
      throw new Exception("Cannot run " + command + ": " + e.getMessage());
    }
  }

  /**
   * Sets an erasure coding policy on a directory at the specified path.
   * @param params Parameters passed to the command.
   * @throws Exception If command failed.
   */
  private void setPolicy(String[] params) throws Exception {
    String command = "setPolicy";
    try {
      // setPolicy -path <path> [-policy <policyName>]
      Options setPolicyOptions = new Options();

      String pathOptionName = "path";
      Option pathOption = OptionBuilder.hasArg()
          .isRequired()
          .withLongOpt(pathOptionName)
          .withDescription("Path to set policy on")
          .create();
      setPolicyOptions.addOption(pathOption);

      String policyOptionName = "policy";
      Option policyOption = OptionBuilder.hasArg()
          .withLongOpt(policyOptionName)
          .withDescription("Policy to set")
          .create();
      setPolicyOptions.addOption(policyOption);

      CommandLine args = parseCommandArgs(setPolicyOptions, params);
      String path = args.getOptionValue(pathOptionName);
      String policy = args.getOptionValue(policyOptionName);

      erasureCodingShim.setErasureCodingPolicy(new Path(path), policy);
      writeTestOutput("Set EC policy' " + policy);
    } catch (ParseException pe) {
      writeTestOutput("Error parsing options for " + command + " " + pe.getMessage());
    } catch (Exception e) {
      writeTestOutput("Caught exception running " + command + ": " + e.getMessage());
      throw new Exception("Cannot run " + command + ": " + e.getMessage());
    }
  }

  /**
   * Unsets an erasure coding policy on a directory at the specified path.
   * @param params Parameters passed to the command.
   * @throws Exception if command failed.
   */
  private void unsetPolicy(String[] params) throws Exception {
    String command = "unsetPolicy";
    try {
      // unsetPolicy -path <path>
      Options unsetPolicyOptions = new Options();

      String pathOptionName = "path";
      Option pathOption = OptionBuilder.hasArg()
          .isRequired()
          .withLongOpt(pathOptionName)
          .withDescription("Path to unset policy on")
          .create();
      unsetPolicyOptions.addOption(pathOption);

      CommandLine args = parseCommandArgs(unsetPolicyOptions, params);
      String path = args.getOptionValue(pathOptionName);

      erasureCodingShim.unsetErasureCodingPolicy(new Path(path));
      writeTestOutput("Unset EC policy");
    } catch (ParseException pe) {
      writeTestOutput("Error parsing options for " + command + " " + pe.getMessage());
    } catch (Exception e) {
      writeTestOutput("Caught exception running " + command + ": " + e.getMessage());
      throw new Exception("Cannot run " + command + ": " + e.getMessage());
    }
  }

  /**
   * Comparator the compares HdfsFileErasureCodingPolicy by name.
   */
  private Comparator<HdfsFileErasureCodingPolicy> nameComparator =
      Comparator.comparing(HdfsFileErasureCodingPolicy::getName);

  private void listPolicies() throws Exception {
    try {
      List<HdfsFileErasureCodingPolicy> erasureCodingPolicies =
          erasureCodingShim.getAllErasureCodingPolicies();
      erasureCodingPolicies.sort(nameComparator);
      if (erasureCodingPolicies.isEmpty()) {
        writeTestOutput("No EC Policies present");
      }
      for (HdfsFileErasureCodingPolicy policy : erasureCodingPolicies) {
        writeTestOutput("Policy: " + policy.getName() + " " + policy.getStatus());
      }
    } catch (Exception e) {
      throw new Exception("Cannot do language command: " + e.getMessage());
    }
  }

  @Override
  public void close() throws Exception {
  }
}
