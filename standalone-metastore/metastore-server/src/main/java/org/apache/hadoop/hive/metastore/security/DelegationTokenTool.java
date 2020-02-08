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

package org.apache.hadoop.hive.metastore.security;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Tool to twiddle MetaStore delegation tokens.
 */
public class DelegationTokenTool extends Configured implements Tool {
  private static Logger LOG = LoggerFactory.getLogger(DelegationTokenTool.class);

  private DelegationTokenStore delegationTokenStore;
  private static String confLocation;

  private enum OpType { DELETE, LIST }
  private OpType opType = OpType.LIST;

  private boolean isDryRun = false;

  private long timeLimitMillis;
  private Predicate<DelegationTokenIdentifier> selectForDeletion = Predicates.alwaysTrue();

  private static final int BATCH_SIZE_DEFAULT = 100;
  private int batchSize = BATCH_SIZE_DEFAULT; // Number of tokens to drop, between sleep intervals;
  private static final long SLEEP_TIME_MILLIS_DEFAULT = 10 * 1000;
  private long sleepTimeMillis = SLEEP_TIME_MILLIS_DEFAULT; // Sleep-time in milliseconds, between batches of delegation tokens dropped.

  private HadoopThriftAuthBridge.Server.ServerMode serverMode = HadoopThriftAuthBridge.Server.ServerMode.METASTORE;

  private DelegationTokenTool() {}

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DelegationTokenTool(), args));
  }

  private void readArgs(String[] args) throws Exception {

    args = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    Options options = new Options();
    options.addOption(new Option("confLocation", true, "Location of HCat/Hive Server's hive-site."));
    options.addOption(new Option("delete", false, "Delete delegation token."));
    options.addOption(new Option("list", false, "List delegation tokens."));
    options.addOption(new Option("olderThan", true, "Filter for token's issue-date. (e.g. 3d, 1h or 4m)."));
    options.addOption(new Option("expired", false, "Select expired delegation tokens for listing/deletion."));
    options.addOption(new Option("dryRun", false, "Don't actually delete delegation tokens."));
    options.addOption(new Option("batchSize", true, "Number of tokens to drop between sleep intervals."));
    options.addOption(new Option("sleepTime", true, "Sleep-time in seconds, between batches of dropped delegation tokens."));
    options.addOption(new Option("serverMode", true, "The service from which to read delegation tokens. Should be either of [METASTORE, HIVESERVER2]."));

    CommandLine commandLine = new GnuParser().parse(options,
        args,
        false // Stop on non-existent option.
    );

    if (commandLine.hasOption("confLocation")) {
      confLocation = commandLine.getOptionValue("confLocation");
    }

    if (commandLine.hasOption("list")) {
      opType = OpType.LIST;
    }
    else
    if (commandLine.hasOption("delete")) {
      opType = OpType.DELETE;
    }
    else {
      throw new IllegalArgumentException("Operation must be delete, list or get!");
    }

    isDryRun = (commandLine.hasOption("dryRun"));

    if (commandLine.hasOption("expired")) {
      LOG.info("Working on expired delegation tokens!");
      timeLimitMillis = System.currentTimeMillis();
      selectForDeletion = new Predicate<DelegationTokenIdentifier>() {
        public boolean apply(DelegationTokenIdentifier input) {
          return timeLimitMillis > input.getMaxDate();
        }
      };
    }
    else
    if (commandLine.hasOption("olderThan")) {

      String olderThanLimitString = commandLine.getOptionValue("olderThan");
      switch (olderThanLimitString.charAt(olderThanLimitString.length()-1)) {
        case 'd':
        case 'D':
          timeLimitMillis = System.currentTimeMillis() - 24*60*60*1000*Integer.parseInt(olderThanLimitString.substring(0, olderThanLimitString.length()-1));
          break;
        case 'h':
        case 'H':
          timeLimitMillis = System.currentTimeMillis() - 60*60*1000*Integer.parseInt(olderThanLimitString.substring(0, olderThanLimitString.length()-1));
          break;
        case 'm':
        case 'M':
          timeLimitMillis = System.currentTimeMillis() - 60*1000*Integer.parseInt(olderThanLimitString.substring(0, olderThanLimitString.length()-1));
          break;
        default:
          throw new IllegalArgumentException("Unsupported time-limit: " + olderThanLimitString);
      }

      LOG.info("Working on delegation tokens older than current-time (" + timeLimitMillis + ").");
      selectForDeletion = new Predicate<DelegationTokenIdentifier>() {
        public boolean apply(DelegationTokenIdentifier input) {
          return timeLimitMillis > input.getIssueDate();
        }
      };

    }
    else {
      // Neither "expired" nor "olderThan" criteria selected. This better not be an attempt to delete tokens.
      if (opType == OpType.DELETE) {
        throw new IllegalArgumentException("Attempting to delete tokens. " +
            "Specify deletion criteria (either expired or time-range).");
      }
    }

    if (commandLine.hasOption("batchSize")) {
      String batchSizeString = commandLine.getOptionValue("batchSize");
      batchSize = Integer.parseInt(batchSizeString);
      if (batchSize < 1) {
        LOG.warn("Invalid batch-size! (" + batchSize + ") Resetting to defaults.");
        batchSize = BATCH_SIZE_DEFAULT;
      }
      LOG.info("Batch-size for drop == " + batchSize);
    }

    if (commandLine.hasOption("sleepTime")) {
      String sleepTimeString = commandLine.getOptionValue("sleepTime");
      sleepTimeMillis = 1000 * Integer.parseInt(sleepTimeString);
      if (sleepTimeMillis <= 0) {
        LOG.warn("Invalid sleep-time! (" + sleepTimeMillis + ") Resetting to defaults.");
        sleepTimeMillis = SLEEP_TIME_MILLIS_DEFAULT;
      }
      LOG.info("Sleep between drop-batches: " + sleepTimeMillis + " milliseconds.");
    }

    if (commandLine.hasOption("serverMode")) {
      String serverModeString = commandLine.getOptionValue("serverMode").toLowerCase();
      switch(serverModeString) {
        case "metastore":
          serverMode = HadoopThriftAuthBridge.Server.ServerMode.METASTORE; break;
        case "hiveserver2":
          serverMode = HadoopThriftAuthBridge.Server.ServerMode.HIVESERVER2; break;
        default:
          throw new IllegalArgumentException("Invalid value for for serverMode (" + serverModeString + ")" +
              "Should be either \"METASTORE\", or \"HIVESERVER2\"");
      }
    }
    LOG.info("Running with serverMode == " + serverMode);

  }

  private void init() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path(confLocation));

    String tokenStoreClassName =
        MetastoreConf.getVar(conf,MetastoreConf.ConfVars.DELEGATION_TOKEN_STORE_CLS, "");
    if (StringUtils.isBlank(tokenStoreClassName)) {
      throw new Exception("Could not find Delegation TokenStore implementation.");
    }

    Class<? extends DelegationTokenStore> clazz = Class.forName(tokenStoreClassName).asSubclass(DelegationTokenStore.class);
    delegationTokenStore = ReflectionUtils.newInstance(clazz, conf);
    delegationTokenStore.init(null, serverMode);
  }

  private List<DelegationTokenIdentifier> getAllDelegationTokenIDs() throws Exception {
    return delegationTokenStore.getAllDelegationTokenIdentifiers();
  }

  private void doList() throws Exception {
    for (DelegationTokenIdentifier tokenId : Iterables.filter(getAllDelegationTokenIDs(), selectForDeletion)) {
      System.out.println(tokenId.toString());
    }
  }

  private void doDelete() throws Exception {
    int nDeletedTokens = 0;
    List<DelegationTokenIdentifier> allDelegationTokenIDs = getAllDelegationTokenIDs();
    for (DelegationTokenIdentifier tokenId : Iterables.filter(allDelegationTokenIDs, selectForDeletion)) {
      if ((++nDeletedTokens % batchSize) == 0) {
        LOG.info("Deleted " + nDeletedTokens + "/" + allDelegationTokenIDs.size() +
                 " (" + (((long)(100*nDeletedTokens))/allDelegationTokenIDs.size()) + "%). " +
                 "Sleeping for " + sleepTimeMillis + "ms...");
        try {Thread.sleep(sleepTimeMillis); } catch (InterruptedException ignore) {}
      }
      LOG.info("Deleting token: " + tokenId.toString());
      if (!isDryRun) {
        delegationTokenStore.removeToken(tokenId);
      }
    }
  }

  public int run(String[] args) throws Exception {
    try {
      readArgs(args);
      init();

      switch (opType) {
        case LIST:
          doList(); break;
        case DELETE:
          doDelete(); break;
      }

      return 0;
    }
    catch (Exception exception) {
      LOG.error("Unexpected exception: ", exception);
      return -1;
    }
  }
}
