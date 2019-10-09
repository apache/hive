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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class LlapCacheResourceProcessor implements CommandProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(LlapCacheResourceProcessor.class);
  private Options CACHE_OPTIONS = new Options();
  private HelpFormatter helpFormatter = new HelpFormatter();

  LlapCacheResourceProcessor() {
    CACHE_OPTIONS.addOption("purge", "purge", false, "Purge LLAP IO cache");
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    SessionState ss = SessionState.get();
    command = new VariableSubstitution(() -> SessionState.get().getHiveVariables()).substitute(ss.getConf(), command);
    String[] tokens = command.split("\\s+");
    if (tokens.length < 1) {
      throw new CommandProcessorException("LLAP Cache Processor Helper Failed: Command arguments are empty.");
    }
    String params[] = Arrays.copyOfRange(tokens, 1, tokens.length);
    try {
      return llapCacheCommandHandler(ss, params);
    } catch (CommandProcessorException e) {
      throw e;
    } catch (Exception e) {
      throw new CommandProcessorException("LLAP Cache Processor Helper Failed: " + e.getMessage());
    }
  }

  private CommandProcessorResponse llapCacheCommandHandler(SessionState ss, String[] params)
      throws ParseException, CommandProcessorException {
    CommandLine args = parseCommandArgs(CACHE_OPTIONS, params);
    boolean purge = args.hasOption("purge");
    String hs2Host = null;
    if (ss.isHiveServerQuery()) {
      hs2Host = ss.getHiveServer2Host();
    }
    if (purge) {
      List<String> fullCommand = Lists.newArrayList("llap", "cache");
      fullCommand.addAll(Arrays.asList(params));
      CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommandAndServiceObject(ss, HiveOperationType.LLAP_CACHE_PURGE, fullCommand, hs2Host);
      if (authErrResp != null) {
        // there was an authorization issue
        return authErrResp;
      }
      try {
        LlapRegistryService llapRegistryService = LlapRegistryService.getClient(ss.getConf());
        llapCachePurge(ss, llapRegistryService);
        return new CommandProcessorResponse(getSchema(), null);
      } catch (Exception e) {
        LOG.error("Error while purging LLAP IO Cache. err: ", e);
        throw new CommandProcessorException(
            "LLAP Cache Processor Helper Failed: Error while purging LLAP IO Cache. err: " + e.getMessage());
      }
    } else {
      String usage = getUsageAsString();
      throw new CommandProcessorException(
          "LLAP Cache Processor Helper Failed: Unsupported sub-command option. " + usage);
    }
  }

  private Schema getSchema() {
    Schema sch = new Schema();
    sch.addToFieldSchemas(new FieldSchema("hostName", "string", ""));
    sch.addToFieldSchemas(new FieldSchema("purgedMemoryBytes", "string", ""));
    sch.putToProperties(SERIALIZATION_NULL_FORMAT, defaultNullString);
    return sch;
  }

  private void llapCachePurge(final SessionState ss, final LlapRegistryService llapRegistryService) throws Exception {
    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Future<Long>> futures = new ArrayList<>();
    Collection<LlapServiceInstance> instances = llapRegistryService.getInstances().getAll();
    for (LlapServiceInstance instance : instances) {
      futures.add(executorService.submit(new PurgeCallable(ss.getConf(), instance)));
    }

    int i = 0;
    for (LlapServiceInstance instance : instances) {
      Future<Long> future = futures.get(i);
      ss.out.println(Joiner.on("\t").join(instance.getHost(), future.get()));
      i++;
    }
  }

  private static class PurgeCallable implements Callable<Long> {
    public static final Logger LOG = LoggerFactory.getLogger(PurgeCallable.class);
    private Configuration conf;
    private LlapServiceInstance instance;
    private SocketFactory socketFactory;
    private RetryPolicy retryPolicy;

    PurgeCallable(Configuration conf, LlapServiceInstance llapServiceInstance) {
      this.conf = conf;
      this.instance = llapServiceInstance;
      this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
      //not making this configurable, best effort
      this.retryPolicy = RetryPolicies.retryUpToMaximumTimeWithFixedSleep(
        10000, 2000L, TimeUnit.MILLISECONDS);
    }

    @Override
    public Long call() {
      try {
        LlapManagementProtocolClientImpl client = new LlapManagementProtocolClientImpl(conf, instance.getHost(),
          instance.getManagementPort(), retryPolicy, socketFactory);
        LlapDaemonProtocolProtos.PurgeCacheResponseProto resp = client.purgeCache(null, LlapDaemonProtocolProtos
          .PurgeCacheRequestProto.newBuilder().build());
        return resp.getPurgedMemoryBytes();
      } catch (Exception e) {
        LOG.warn("Exception while purging cache.", e);
        return 0L;
      }
    }
  }

  private String getUsageAsString() {
    StringWriter out = new StringWriter();
    PrintWriter pw = new PrintWriter(out);
    helpFormatter.printUsage(pw, helpFormatter.getWidth(), "llap cache", CACHE_OPTIONS);
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
