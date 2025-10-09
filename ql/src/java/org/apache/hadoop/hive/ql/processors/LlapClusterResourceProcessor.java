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

import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.defaultNullString;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class LlapClusterResourceProcessor implements CommandProcessor {
  public static final Logger LOG = LoggerFactory.getLogger(LlapClusterResourceProcessor.class);
  private Options CLUSTER_OPTIONS = new Options();
  private HelpFormatter helpFormatter = new HelpFormatter();

  LlapClusterResourceProcessor() {
    CLUSTER_OPTIONS.addOption("info", "info", false, "Information about LLAP cluster");
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    SessionState ss = SessionState.get();
    command = new VariableSubstitution(() -> SessionState.get().getHiveVariables()).substitute(ss.getConf(), command);
    String[] tokens = command.split("\\s+");
    if (tokens.length < 1) {
      throw new CommandProcessorException("LLAP Cluster Processor Helper Failed: Command arguments are empty.");
    }

    String params[] = Arrays.copyOfRange(tokens, 1, tokens.length);
    try {
      return llapClusterCommandHandler(ss, params);
    } catch (Exception e) {
      throw new CommandProcessorException("LLAP Cluster Processor Helper Failed: " + e.getMessage());
    }
  }

  private CommandProcessorResponse llapClusterCommandHandler(SessionState ss, String[] params)
      throws ParseException, CommandProcessorException {
    CommandLine args = parseCommandArgs(CLUSTER_OPTIONS, params);
    String hs2Host = null;
    if (ss.isHiveServerQuery()) {
      hs2Host = ss.getHiveServer2Host();
    }

    boolean hasInfo = args.hasOption("info");
    if (hasInfo) {
      List<String> fullCommand = Lists.newArrayList("llap", "cluster");
      fullCommand.addAll(Arrays.asList(params));
      CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommandAndServiceObject(ss, HiveOperationType.LLAP_CLUSTER_INFO, fullCommand, hs2Host);
      if (authErrResp != null) {
        // there was an authorization issue
        return authErrResp;
      }
      try {
        LlapRegistryService llapRegistryService = LlapRegistryService.getClient(ss.getConf());
        String appId = llapRegistryService.getApplicationId() == null ? "null" :
          llapRegistryService.getApplicationId().toString();
        for (LlapServiceInstance instance : llapRegistryService.getInstances().getAll()) {
          ss.out.println(Joiner.on("\t").join(appId, instance.getWorkerIdentity(), instance.getHost(),
            instance.getRpcPort(), instance.getResource().getMemory() * 1024L * 1024L,
            instance.getResource().getVirtualCores()));
        }
        return new CommandProcessorResponse(getSchema(), null);
      } catch (Exception e) {
        LOG.error("Unable to list LLAP instances. err: ", e);
        throw new CommandProcessorException(
            "LLAP Cluster Processor Helper Failed: Unable to list LLAP instances. err: " + e.getMessage());
      }
    } else {
      String usage = getUsageAsString();
      throw new CommandProcessorException(
          "LLAP Cluster Processor Helper Failed: Unsupported sub-command option. " + usage);
    }
  }

  private Schema getSchema() {
    Schema sch = new Schema();
    sch.addToFieldSchemas(new FieldSchema("applicationId", serdeConstants.STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("workerIdentity", serdeConstants.STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("hostname", serdeConstants.STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("rpcPort", serdeConstants.STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("memory", serdeConstants.STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("vcores", serdeConstants.STRING_TYPE_NAME, ""));
    sch.putToProperties(SERIALIZATION_NULL_FORMAT, defaultNullString);
    return sch;
  }

  private String getUsageAsString() {
    StringWriter out = new StringWriter();
    PrintWriter pw = new PrintWriter(out);
    helpFormatter.printUsage(pw, helpFormatter.getWidth(), "llap cluster", CLUSTER_OPTIONS);
    pw.flush();
    return out.toString();
  }

  private CommandLine parseCommandArgs(final Options opts, String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    return parser.parse(opts, args);
  }

  @Override
  public void close() {
  }
}
